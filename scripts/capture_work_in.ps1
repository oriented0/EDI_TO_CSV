param(
    [string]$WatchPath = "C:\hira\DDMD\work\in",
    [string]$Destination = "D:\Github\EDI_TO_CSV\work_in_capture",
    [string]$Filter = "cmd_*"
)

New-Item -ItemType Directory -Path $Destination -Force | Out-Null

$fsw = New-Object System.IO.FileSystemWatcher
$fsw.Path = $WatchPath
$fsw.Filter = $Filter
$fsw.IncludeSubdirectories = $false
$fsw.EnableRaisingEvents = $true

$sourceId = "WorkInCapture"
Register-ObjectEvent -InputObject $fsw -EventName Created -SourceIdentifier $sourceId -Action {
    Start-Sleep -Milliseconds 100
    $source = $Event.SourceEventArgs.FullPath
    $name = Split-Path $source -Leaf
    $timestamp = Get-Date -Format "yyyyMMdd_HHmmss_fff"
    $destName = "${timestamp}_$name"
    $dest = Join-Path $using:Destination $destName
    try {
        Copy-Item -LiteralPath $source -Destination $dest -ErrorAction Stop
        Write-Host "Captured $name -> $dest"
    }
    catch {
        Write-Warning "Failed to copy ${name}: $_"
    }
} | Out-Null

Write-Host "Watching $WatchPath for $Filter (copies to $Destination). Press Ctrl+C to stop."

try {
    while ($true) {
        Start-Sleep -Seconds 1
    }
}
finally {
    Unregister-Event -SourceIdentifier $sourceId -ErrorAction SilentlyContinue
    $fsw.Dispose()
}
