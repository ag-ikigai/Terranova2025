param(
  [string]$OutDir       = "C:\TerraNova\outputs",
  [string]$BackupOutDir = "C:\TerraNova_backups\TerraNova_20250914_1710\outputs",
  [switch]$WhatIf
)
$ErrorActionPreference="Stop"
$targets = @(
  "m0_inputs\FX_Path.parquet",
  "m0_calendar.parquet",
  "m0_opening_bs.parquet"
)
foreach($rel in $targets){
  $src = Join-Path $BackupOutDir $rel
  $dst = Join-Path $OutDir       $rel
  if(Test-Path $src){
    Write-Host "RESTORE $rel  <=  $src" -ForegroundColor Cyan
    if(-not $WhatIf){ Copy-Item $src $dst -Force }
  } else {
    Write-Host "SKIP (missing in backup): $src" -ForegroundColor Yellow
  }
}
Write-Host "Restore complete."
