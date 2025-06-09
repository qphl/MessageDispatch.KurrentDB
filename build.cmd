@echo off

SET VERSION=0.0.0
IF NOT [%1]==[] (SET VERSION=%1)

SET TAG=0.0.0
IF NOT [%2]==[] (SET TAG=%2)
SET TAG=%TAG:tags/=%

dotnet restore .\src\MessageDispatch.KurrentDB.sln -PackagesDirectory .\src\packages -Verbosity detailed

dotnet format .\src\MessageDispatch.KurrentDB.sln --severity warn --verify-no-changes -v diag
IF %errorlevel% neq 0 EXIT /B %errorlevel%

dotnet pack .\src\MessageDispatch.KurrentDB\MessageDispatch.KurrentDB.csproj -o .\dist -p:Version="%VERSION%" -p:PackageVersion="%VERSION%" -p:Tag="%TAG%" -c Release