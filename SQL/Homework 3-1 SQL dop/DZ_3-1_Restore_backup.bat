REM 1. ���������� ��������� � ���������� ������:
REM ������ ����� ��������, ���������� ������� ������.
REM ���� �� pg_restore.exe � ���� ������� � ��������� �����.


ECHO OFF
cls
pg_restore.exe --host localhost --port 5432 --username postgres --dbname netology --verbose "d:\YandexDisk\_Pentaho_IN\world-db.backup"

psql --host localhost --port 5432 --username postgres --dbname netology -c "select * from world.country;"

psql --host localhost --port 5432 --username postgres --dbname netology -c "select * from world.country limit 3;"