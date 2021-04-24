REM 1. Необходимо выполнить в коммандной строке:
REM Пароль будет запрошен, необходимо набрать пароль.
REM Путь до pg_restore.exe у меня занесен в окружение винды.


ECHO OFF
cls
pg_restore.exe --host localhost --port 5432 --username postgres --dbname netology --verbose "d:\YandexDisk\_Pentaho_IN\world-db.backup"

psql --host localhost --port 5432 --username postgres --dbname netology -c "select * from world.country;"

psql --host localhost --port 5432 --username postgres --dbname netology -c "select * from world.country limit 3;"