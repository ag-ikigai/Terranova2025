param(
  [string]$OutDir       = "C:\TerraNova\outputs",
  [string]$BackupOutDir = "C:\TerraNova_backups\TerraNova_20250914_1710\outputs"
)
$ErrorActionPreference="Stop"
$targets = @(
  "m0_inputs\FX_Path.parquet",
  "m0_calendar.parquet",
  "m0_opening_bs.parquet"
)

# Show size + hash for each side
foreach($rel in $targets){
  $a = Join-Path $OutDir       $rel
  $b = Join-Path $BackupOutDir $rel
  if(-not (Test-Path $a)){ Write-Host "[MISS NOW]" $a -ForegroundColor Yellow }
  if(-not (Test-Path $b)){ Write-Host "[MISS BAK]" $b -ForegroundColor Yellow }
  if((Test-Path $a) -and (Test-Path $b)){
    $ha = Get-FileHash $a
    $hb = Get-FileHash $b
    $sa = (Get-Item $a).Length
    $sb = (Get-Item $b).Length
    "{0}`n  NOW:  {1}  size={2}`n  BAK:  {3}  size={4}`n" -f $rel,$ha.Hash,$sa,$hb.Hash,$sb | Write-Host
  }
}
