$ErrorActionPreference = 'Stop'
$compose = 'infrastructure/docker/compose.yaml'
$dbs = @{
  'core_banking' = 'accounts'; 'payment_processing' = 'transactions';
  'card_management' = 'cards'; 'loan_management' = 'loans';
  'customer_management' = 'customers'; 'branch_management' = 'branches'
}
foreach ($db in $dbs.Keys) {
  Write-Host "Checking $db ($($dbs[$db]))"
  docker compose --project-directory . -f $compose exec -T postgres psql -U bedp_admin -d $db -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM $($dbs[$db]);"
  if ($LASTEXITCODE -ne 0) { throw "Database verification failed: $db" }
}
docker compose --project-directory . -f $compose exec -T postgres psql -U bedp_admin -d postgres -c 'SHOW wal_level;'
if ($LASTEXITCODE -ne 0) { throw 'WAL verification failed' }
