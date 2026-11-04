param(
    [string]$Task = "help"
)

$ErrorActionPreference = "Stop"
$Rest = @($args)
$VenvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$VenvDbt = Join-Path $PSScriptRoot ".venv\Scripts\dbt.exe"
$AirflowCompose = @("compose", "-f", "airflow/docker-compose.yml")

function Invoke-Spark {
    param([string[]]$Command)
    & docker compose run --rm spark @Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit code $LASTEXITCODE): $($Command -join ' ')"
    }
}

function Invoke-Host {
    param([string]$Exe, [string[]]$Arguments)
    if (-not (Test-Path $Exe)) {
        throw "Missing $Exe. Create the host venv first (see README)."
    }
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit code $LASTEXITCODE): $Exe $($Arguments -join ' ')"
    }
}

function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed (exit code $LASTEXITCODE): docker $($Arguments -join ' ')"
    }
}

switch ($Task) {
    # ---- Docker / Spark ----
    "build" { Invoke-Docker @("compose", "build") }
    "shell"  { Invoke-Spark @("bash") }
    "test"   { Invoke-Spark (@("pytest", "-q") + $Rest) }
    "lint"   { Invoke-Spark @("ruff", "check", ".") }
    "ingest" { Invoke-Spark (@("python", "-m", "repopulse.ingest") + $Rest) }
    "bronze" { Invoke-Spark (@("python", "-m", "repopulse.jobs.bronze_job") + $Rest) }
    "silver" { Invoke-Spark (@("python", "-m", "repopulse.jobs.silver_job") + $Rest) }
    "counts" { Invoke-Spark @("python", "-m", "repopulse.counts") }
    "run" {
        Invoke-Spark (@("python", "-m", "repopulse.ingest") + $Rest)
        Invoke-Spark (@("python", "-m", "repopulse.jobs.bronze_job") + $Rest)
        Invoke-Spark (@("python", "-m", "repopulse.jobs.silver_job") + $Rest)
    }
    "ml-train" { Invoke-Spark (@("python", "-m", "repopulse.ml.train_sparkml") + $Rest) }
    "reset" {
        # Deletes derived layers only. Raw downloads are kept.
        foreach ($d in @("data/bronze", "data/quarantine", "data/silver", "data/metrics")) {
            if (Test-Path $d) { Remove-Item -Recurse -Force $d }
        }
        Write-Host "Removed bronze, quarantine, silver, metrics. Raw files kept."
    }
    # ---- host (no Docker): DuckDB warehouse, dbt, DQ gate, ML ----
    "host-test" {
        Invoke-Host $VenvPython (@("-m", "pytest", "-q",
            "tests/test_warehouse.py", "tests/test_load_warehouse.py", "tests/test_dq.py",
            "tests/test_ml_spec.py", "tests/test_ml_metrics.py", "tests/test_sql_end_to_end.py") + $Rest)
    }
    "wh-check"    { Invoke-Host $VenvPython @("-m", "repopulse.warehouse") }
    "sql"         { Invoke-Host $VenvPython (@("-m", "repopulse.warehouse", "--sql") + $Rest) }
    "load"        { Invoke-Host $VenvPython (@("-m", "repopulse.load_warehouse") + $Rest) }
    "dq"          { Invoke-Host $VenvPython (@("-m", "repopulse.dq") + $Rest) }
    "dbt"         { Invoke-Host $VenvDbt ($Rest + @("--project-dir", "dbt", "--profiles-dir", "dbt")) }
    "ml-features" { Invoke-Host $VenvPython (@("-m", "repopulse.ml.build_features") + $Rest) }
    "ml-eval"     { Invoke-Host $VenvPython (@("-m", "repopulse.ml.evaluate") + $Rest) }
    # ---- Airflow (Docker) ----
    "airflow-build" { Invoke-Docker ($AirflowCompose + @("build")) }
    "airflow-up"    { Invoke-Docker ($AirflowCompose + @("up", "-d", "--build")) }
    "airflow-down"  { Invoke-Docker ($AirflowCompose + @("down")) }
    "airflow"       { Invoke-Docker ($AirflowCompose + @("exec", "-T", "airflow-scheduler", "airflow") + $Rest) }
    default {
        Write-Host "Usage: ./tasks.ps1 <task> [args]"
        Write-Host "Spark (Docker):"
        Write-Host "  build | shell | lint | test | counts | reset"
        Write-Host "  ingest|bronze|silver|run --start D [--end D] [--start-hour H] [--end-hour H]"
        Write-Host "  ml-train                   SparkML models (needs data/ml/features.parquet)"
        Write-Host "Host (no Docker; needs .venv):"
        Write-Host "  host-test | wh-check | sql `"SELECT ...`" | load <range> | dq <range> | dbt <args>"
        Write-Host "  ml-features [--feature-hours H --horizon-hours H --threshold T --plan-only]"
        Write-Host "  ml-eval"
        Write-Host "Airflow (Docker):"
        Write-Host "  airflow-up | airflow-down | airflow-build | airflow <airflow CLI args>"
    }
}