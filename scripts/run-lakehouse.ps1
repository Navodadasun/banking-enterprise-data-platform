param([string]$RunId)

$ErrorActionPreference = 'Stop'
$composeFile = 'infrastructure/docker/compose.lakehouse.yaml'
docker compose --project-directory . -f $composeFile exec -T spark `
  bash -lc 'test -f /opt/project/data-processing/lakehouse/pipeline.py'
if ($LASTEXITCODE -ne 0) {
  throw 'Spark cannot see the project at /opt/project. Recreate only the Spark container from the project root (never remove volumes): docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml up -d --no-deps --force-recreate spark'
}
$packages = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.92.1,org.apache.hadoop:hadoop-aws:3.3.4'
$pipelineArgs = @()
if ($RunId) { $pipelineArgs += @('--run-id', $RunId) }

docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml exec -T spark `
  bash -lc 'mkdir -p /var/lib/bedp-lakehouse && touch /var/lib/bedp-lakehouse/lakehouse_runs.jsonl'
if ($LASTEXITCODE -ne 0) { throw 'Could not prepare the structured lakehouse run log.' }

docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml exec -T spark `
  /opt/spark/bin/spark-submit `
  --master 'local[2]' `
  --driver-memory 3g `
  --packages $packages `
  /opt/project/data-processing/lakehouse/pipeline.py @pipelineArgs
$pipelineExitCode = $LASTEXITCODE
$runtimePath = Join-Path (Get-Location) 'runtime'
New-Item -ItemType Directory -Force -Path $runtimePath | Out-Null
docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml cp `
  spark:/var/lib/bedp-lakehouse/lakehouse_runs.jsonl `
  (Join-Path $runtimePath 'lakehouse_runs.jsonl')
if ($LASTEXITCODE -ne 0) { throw 'Could not copy the structured lakehouse run log to runtime\lakehouse_runs.jsonl.' }
if ($pipelineExitCode -ne 0) { throw 'Lakehouse pipeline failed. Review runtime\lakehouse_runs.jsonl and the Spark command output.' }
