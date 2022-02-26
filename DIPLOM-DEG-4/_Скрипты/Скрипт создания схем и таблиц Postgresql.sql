-- DROP DATABASE IF EXISTS Diplom_db;
-- CREATE DATABASE "Diplom_db" WITH OWNER "postgres" ENCODING 'UTF8';

-- Создаю СхемЫ:
-- drop SCHEMA IF EXISTS stage;
-- CREATE SCHEMA stage;
-- drop SCHEMA IF EXISTS ods;
-- CREATE SCHEMA ods;

-- drop SCHEMA IF EXISTS dds;
-- CREATE SCHEMA dds;
-- drop SCHEMA IF EXISTS rejected;
-- CREATE SCHEMA rejected;


set datestyle = euro;
-- show datestyle;

-- Таблица stage.bookings
DROP TABLE IF EXISTS stage.bookings;
CREATE TABLE stage.bookings (
	BookingNumber int,
	Room bpchar(10),
	typeroom bpchar(10),
	sex bpchar(1),
	numbervisits smallint,
	n_arrival smallint,
	datearrival date,
	datedeparture date,
	reservationstatus bpchar(3),
	daysinclinic smallint,
	rate_passed bpchar(10),
	birthday date,
	AgeOnArrival smallint,
	city bpchar(50),
	geo bpchar(50),
	amount_booking numeric(10,2),
	amount_additionally numeric(10,2)
);
copy stage.bookings from '/input_data/Bookings.csv' WITH delimiter ';' csv HEADER NULL 'NULL'; 



-- Таблица rejected.rejected_table
-- data quality (ошибки)
DROP TABLE IF EXISTS rejected.rejected_table;
CREATE TABLE rejected.rejected_table (
	ActualDate timestamp,
	error text,
	BookingNumber int,
	Room bpchar(10),
	typeroom bpchar(10),
	sex bpchar(1),
	numbervisits smallint,
	n_arrival smallint,
	datearrival date,
	datedeparture date,
	reservationstatus bpchar(3),
	daysinclinic smallint,
	rate_passed bpchar(10),
	birthday date,
	AgeOnArrival smallint,
	city bpchar(50),
	geo bpchar(50),
	amount_booking numeric(10,2),
	amount_additionally numeric(10,2)
);

-- Таблица ods.Bookings_ods
DROP TABLE IF EXISTS ods.Bookings_ods;
CREATE TABLE ods.Bookings_ods (
	ActualDate timestamp,
	BookingNumber int,
	Room bpchar(10),
	typeroom bpchar(10),
	sex bpchar(1),
	numbervisits smallint,
	n_arrival smallint,
	datearrival date,
	datedeparture date,
	reservationstatus bpchar(3),
	daysinclinic smallint,
	rate_passed bpchar(10),
	birthday date,
	AgeOnArrival smallint,
	city bpchar(50),
	geo bpchar(50),
	amount_booking numeric(10,2),
	amount_additionally numeric(10,2)
);






-- /////////////////////////////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////////////////////////////
-- DDS
-- /////////////////////////////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////////////////////////////
 
-- /////////////////////////////////////////////////////////////////////////////////////
-- Удаляю таблицу фактов: dds.Fact_Bookings
DROP TABLE IF EXISTS dds.Fact_Bookings;
-- /////////////////////////////////////////////////////////////////////////////////////


-- Таблица dds.Dim_Rooms
DROP TABLE IF EXISTS dds.Dim_Rooms;
CREATE TABLE dds.Dim_Rooms (
	id serial not null primary key,
	Room bpchar(10),
	typeRoom bpchar(10) NOT NULL	
);


-- Таблица dds.Dim_Sex
DROP TABLE IF EXISTS dds.Dim_Sex;
CREATE TABLE dds.Dim_Sex (
	id serial not null primary key,
	sex bpchar(3) NOT NULL	
);


-- Таблица dds.Dim_Calendar
DROP TABLE IF EXISTS dds.Dim_Calendar;
CREATE TABLE dds.Dim_Calendar
AS
WITH dates AS (
    SELECT dd::date AS dt
    FROM generate_series
            ('1945-01-01'::date
            , '2030-01-01'::date
            , '1 day'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDD')::bigint AS id,
    Date(dt)::date as date_,
    to_char(dt, 'YYYY-MM-DD') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS work_day
FROM dates
ORDER BY dt;
ALTER TABLE dds.Dim_Calendar ADD PRIMARY KEY (id);


-- Таблица dds.Dim_Rate
DROP TABLE IF EXISTS dds.Dim_Rate;
CREATE TABLE dds.Dim_Rate (
	id serial not null primary key,
	rate bpchar(10) NOT NULL	
);


-- Таблица dds.Dim_Geo
DROP TABLE IF EXISTS dds.Dim_Geo;
CREATE TABLE dds.Dim_Geo (
	id serial not null primary key,
	geo bpchar(50) NOT NULL	
);


-- -------------------------------------
-- Таблица dds.Dim_Nummber
DROP TABLE IF EXISTS dds.Dim_Number;
CREATE TABLE dds.Dim_Number
AS
WITH num_ AS (
    SELECT nn AS nn
    FROM generate_series(1, 30000, 1) nn
)
SELECT
    -- to_char(nn) AS id,
	nn as id,
    nn as nn
FROM num_
ORDER BY nn;
ALTER TABLE dds.Dim_Number ADD PRIMARY KEY (id);





-- /////////////////////////////////////////////////////////////////////////////////////
-- Создаем таблицу фактов: dds.Fact_Bookings
-- /////////////////////////////////////////////////////////////////////////////////////
-- DROP TABLE IF EXISTS dds.Fact_Bookings;
CREATE table dds.Fact_Bookings(
	BookingNumber_key int not null references dds.Dim_Number(id),
	room_key int not null references dds.Dim_Rooms(id),
	sex_key int not null references dds.Dim_Sex(id),
	datearrival_key bigint not null references dds.Dim_Calendar(id),
	datedeparture_key bigint not null references dds.Dim_Calendar(id),
--	departure_delay int not null,
--	arrival_delay int not null,
	rate_key int not null references dds.Dim_Rate(id),
	geo_key int not null references dds.Dim_Geo(id),
	amount_booking float8,
	amount_add_booking float8
);
-- /////////////////////////////////////////////////////////////////////////////////////
-- /////////////////////////////////////////////////////////////////////////////////////

