$ErrorActionPreference = 'Stop'
$packages = 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.92.1,org.apache.hadoop:hadoop-aws:3.3.4'

docker compose --project-directory . -f infrastructure/docker/compose.lakehouse.yaml exec -T spark `
  /opt/spark/bin/spark-submit `
  --master 'local[2]' `
  --driver-memory 3g `
  --packages $packages `
  /opt/project/data-processing/lakehouse/run_sql.py
if ($LASTEXITCODE -ne 0) { throw 'Lakehouse SQL verification failed.' }
