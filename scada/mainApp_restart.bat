@echo off

set _task=mainApp.exe
set _svr=D:\mainApp\mainApp.exe 
set _des=start.bat

:checkstart
SET status=1 
(TASKLIST|FIND /I "%_task%"||SET status=0) 2>nul 1>nul
ECHO %status%
IF %status% EQU 1 (goto checkag ) ELSE (goto startsvr)


:startsvr
echo %time% 
echo ********����ʼ����********
echo �������������� %time% ,����ϵͳ��־ >> restart_service.txt
echo start %_svr% > %_des%
echo exit >> %_des%
start %_des%
set/p=.<nul
for /L %%i in (1 1 10) do set /p a=.<nul&ping.exe /n 2 127.0.0.1>nul
echo .
echo Wscript.Sleep WScript.Arguments(0) >%tmp%/delay.vbs 
cscript //b //nologo %tmp%/delay.vbs 60000 
del %_des% /Q
echo ********�����������********
goto checkstart


:checkag
echo %time% ������������,60���������.. 
echo Wscript.Sleep WScript.Arguments(0) >%tmp%/delay.vbs 
cscript //b //nologo %tmp%/delay.vbs 60000 
goto checkstart