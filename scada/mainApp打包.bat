pyinstaller -y -w --add-data .\MDB\*.*;.\MDB mainApp.py mainApp.py
copy mainApp_restart.bat .\dist\mainApp\mainApp_restart.bat
pause