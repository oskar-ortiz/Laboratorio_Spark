$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $projectRoot

$env:JAVA_HOME = (& cmd.exe /c ('for %I in ("' + (Join-Path $projectRoot '.tools\jdk-17') + '") do @echo %~sI')).Trim()
$env:HADOOP_HOME = (& cmd.exe /c ('for %I in ("' + (Join-Path $projectRoot '.hadoop') + '") do @echo %~sI')).Trim()
$env:PYSPARK_PYTHON = (& cmd.exe /c ('for %I in ("' + (Join-Path $projectRoot '.venv\Scripts\python.exe') + '") do @echo %~sI')).Trim()
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:Path = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$projectRoot\.venv\Scripts;$env:Path"

if (-not (Test-Path $env:PYSPARK_PYTHON)) {
    throw 'No se encontro .venv. Ejecuta python -m venv .venv y reinstala los requisitos.'
}

& (Join-Path $projectRoot '.venv\Scripts\Activate.ps1')
Write-Host "Entorno Spark activo en $projectRoot"
Write-Host "JAVA_HOME=$env:JAVA_HOME"
Write-Host "HADOOP_HOME=$env:HADOOP_HOME"
