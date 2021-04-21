-- скрипты создания таблиц в докере

-- Создаю Схемы Fact и Dim :
-- drop SCHEMA IF EXISTS dim;
-- drop SCHEMA IF EXISTS fact;
-- drop SCHEMA IF EXISTS Rejected;
-- CREATE SCHEMA dim;
-- CREATE SCHEMA fact;
-- CREATE SCHEMA Rejected;


-- /////////////////////////////////////////////////////////////////////////////////////
-- Удаляем таблицы.
-- Удаляем таблицу фактов: fact.Fact_Flights
DROP TABLE IF EXISTS fact.Fact_Flights;
-- /////////////////////////////////////////////////////////////////////////////////////


-- Таблица dim.Dim_Aircrafts
DROP TABLE IF EXISTS dim.Dim_Aircrafts;
CREATE TABLE dim.Dim_Aircrafts (
	id serial not null primary key,
	aircraft_code bpchar(3) NOT NULL,
	model text  NOT NULL, 	-- jsonb,
	"range" int4
	CONSTRAINT aircrafts_range_check CHECK ((range > 0))
);

-- Таблица dim.Dim_Airports
DROP TABLE IF EXISTS dim.Dim_Airports;
CREATE TABLE dim.Dim_Airports (
	id serial not null primary key,
	airport_code bpchar(3) NOT NULL,
	airport_name text NOT NULL,
	city text NOT NULL,
	coordinates text NOT NULL,
	timezone text NOT NULL
);

-- Таблица dim.Dim_Tariffs
DROP TABLE IF EXISTS dim.Dim_Tariffs;
CREATE TABLE dim.Dim_Tariffs (
	id serial not null primary key,
	tariff text NOT NULL
);

-- Таблица dim.Dim_Passengers
DROP TABLE IF EXISTS dim.Dim_Passengers;
CREATE TABLE dim.Dim_Passengers (
	id serial not null primary key,
	passenger_id varchar(20) NOT NULL,
	passenger_name text NOT NULL,
	phone varchar(50),
	email varchar(100)
);

-- Таблица dim.Dim_Calendar
DROP TABLE IF EXISTS dim.Dim_Calendar;
CREATE TABLE dim.Dim_Calendar
AS
WITH dates AS (
    SELECT dd::timestamp AS dt
    FROM generate_series
            ('2017-05-01 0000'::timestamp
            , '2017-09-30 2400'::timestamp
            , '1 minute'::interval) dd
)
SELECT
    to_char(dt, 'YYYYMMDDHH24MISS')::bigint AS id,
    dt as date_,
    to_char(dt, 'YYYY-MM-DD HH24:MI:SS') AS ansi_date,
    date_part('isodow', dt)::int AS day,
    date_part('week', dt)::int AS week_number,
    date_part('month', dt)::int AS month,
    date_part('isoyear', dt)::int AS year,
    (date_part('isodow', dt)::smallint BETWEEN 1 AND 5)::int AS week_day
FROM dates
ORDER BY dt;
ALTER TABLE dim.Dim_Calendar ADD PRIMARY KEY (id);



-- /////////////////////////////////////////////////////////////////////////////////////
-- Создаем таблицу фактов: fact.Fact_Flights
-- /////////////////////////////////////////////////////////////////////////////////////
DROP TABLE IF EXISTS fact.Fact_Flights;
CREATE table fact.Fact_Flights(
	passenger_key int not null references dim.Dim_Passengers(id),
	actual_departure_key bigint not null references dim.Dim_Calendar(id),
	actual_arrival_key bigint not null references dim.Dim_Calendar(id),
	departure_delay int not null,
	arrival_delay int not null,
	aircraft_key int not null references dim.Dim_Aircrafts(id),
	departure_airport_key int not null references dim.Dim_Airports(id),
	arrival_airport_key int not null references dim.Dim_Airports(id),
	fare_condition_key int not null references dim.Dim_Tariffs(id),
	amount float8
);




-- ***************************************************************************************
-- Создаем таблицы для некачественных строк (rejected)

-- Таблица rejected.rejected_Aircrafts
DROP TABLE IF EXISTS rejected.rejected_Aircrafts;
CREATE TABLE rejected.rejected_Aircrafts (
	aircraft_code bpchar(3),
	model text,
	"range" int4,
	error text
);

-- Таблица rejected.rejected_airports
DROP TABLE IF EXISTS rejected.rejected_airports;
CREATE TABLE rejected.rejected_airports (
	airport_code bpchar(3),
	airport_name text,
	city text,
	coordinates text,
	timezone text,
	error text
);

-- Таблица rejected.rejected_Tariffs
DROP TABLE IF EXISTS rejected.rejected_Tariffs;
CREATE TABLE rejected.rejected_Tariffs (
	tariff text,
	error text
);

-- Таблица rejected.Dim_Passengers
DROP TABLE IF EXISTS rejected.rejected_Passengers;
CREATE TABLE rejected.rejected_Passengers (
	passenger_id varchar(20),
	passenger_name text,
	phone varchar(50),
	email varchar(100),
	error text
);

-- Таблица rejected.fact.Fact_Flights
DROP TABLE IF EXISTS rejected.rejected_Flights;
CREATE table rejected.rejected_Flights(
	ticket_no bpchar(13),
	flight_id int,
	fare_conditions varchar(10),
	amount float8,
	scheduled_departure timestamp,
	scheduled_arrival timestamp,
	departure_airport bpchar(3),
	arrival_airport bpchar(3),
	status varchar(20),
	aircraft_code bpchar(3),
	actual_departure timestamp,
	actual_arrival timestamp,
	error text
);