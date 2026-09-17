@echo off
set "PROJECT_ROOT=%~sdp0.."
pushd "%PROJECT_ROOT%"
set "JAVA_HOME=%PROJECT_ROOT%\.tools\jdk-17"
set "HADOOP_HOME=%PROJECT_ROOT%\.hadoop"
set "PYSPARK_PYTHON=%PROJECT_ROOT%\.venv\Scripts\python.exe"
set "PYSPARK_DRIVER_PYTHON=%PYSPARK_PYTHON%"
set "PATH=%JAVA_HOME%\bin;%HADOOP_HOME%\bin;%PROJECT_ROOT%\.venv\Scripts;%PATH%"
call "%PROJECT_ROOT%\.venv\Scripts\activate.bat"
echo Entorno Spark activo en %CD%
echo JAVA_HOME=%JAVA_HOME%
echo HADOOP_HOME=%HADOOP_HOME%
