@echo off

FOR %%A IN (%*) DO (
     if "%%A"=="help" goto helpscreen
     if "%%A"=="-help" goto helpscreen
     if "%%A"=="--help" goto helpscreen
)

SET CLASSPATH=%CLASSPATH%;.

FOR %%i IN ("%SQLRUNNER_HOME%\lib\*jar", "%SQLRUNNER_HOME%\jdbc\*jar") DO SET CLASSPATH=!CLASSPATH!;%%i

set args=
FOR %%i IN ("%SQLRUNNER_HOME%\init\*properties", "%SQLRUNNER_HOME%\init\*sql") DO SET args=!args! "%%i"

FOR %%i IN ("%USERPROFILE%\.sqlrunner\*properties", "%USERPROFILE%\.sqlrunner\*sql", "%USERPROFILE%\_sqlrunner\*properties", "%USERPROFILE%\_sqlrunner\*sql") DO SET args=!args! "%%i"

rem echo CLASSPATH=%CLASSPATH%
rem echo ARGS=%args%

rem echo java db.SQLRunner %args% %*

java db.SQLRunner %args% %*
exit

:helpscreen
echo usage: sqlrunner db=dbname user=dbuser pass=dbpass filename.sql
