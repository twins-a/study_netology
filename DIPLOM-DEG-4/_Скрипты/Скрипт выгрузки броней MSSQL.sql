declare @dateStart				DATETIME		= NULL	-- с
declare @dateEnd				DATETIME		= NULL	-- по

SET @dateStart = DATEFROMPARTS(2019,1,1)
SET @dateEnd = DATEFROMPARTS(2019,6,30);


-- расчет сумм по броням
-- ЗАМЕНИЛ НА RAND()
-- *************************************************************************************************************
-- drop table #summi
/*
SELECT
	GenericNO,
	sum(
	CASE  
		WHEN (LocalCurrencyAmount > 0) OR (LocalCurrencyAmount < 0 AND IsRevenue=1) THEN LocalCurrencyAmount
		ELSE 0 
	END 
		) Summa_Broni
	INTO #summi
	FROM
		[Logus.HMS].[dbo].[VTransactionFull]
	WHERE
		CorrectionKind = 0			-- 
		AND TransactionStatus='P'		-- только проведенные транзакции
		AND IsRevenue=1					-- только доходные
		AND NOT GenericNo like '5%'		-- отсекаем суммы по профилю
		AND ReservationStatus IN ('IN','OUT')

		GROUP BY
			GenericNo
*/
-- *************************************************************************************************************



-- определим программу, которую проходил гость:
-- Это программа с большим количеством дней (если кол-во дней одинаковое, то берем первую программу, то есть проданную)
-- *************************************************************************************************************
SELECT
	ReservationNo,
	RateCode
INTO #program
FROM
	(SELECT
		ReservationNo,
		RateCode,
		DayDateMin,
		ROW_NUMBER() OVER(PARTITION BY tlm.ReservationNo ORDER BY tlm."RowCount" desc, tlm.DayDateMin) AS 'RN'
	FROM
		(SELECT
			right(ReservationNo, 6) as ReservationNo
			, RateCode
			, MIN(DayDate) AS 'DayDateMin'
			, COUNT (*) AS 'RowCount'
		FROM
			[Logus.HMS].[lr].[VOccupation]
		GROUP BY 
			right(ReservationNo, 6), RateCode
		) tlm
	) tlf
WHERE tlf.RN = 1
-- *************************************************************************************************************



SELECT
	*
FROM
	(
	SELECT
		row_number() over(ORDER BY res.ReservationGenericNo)+1500 AS 'BookingNumber',

		r.Code AS 'Room',
		rt.Code AS 'TypeRoom',
	
		guestInfo.Sex AS 'Sex',

		COUNT(res.ReservationGenericNo) OVER(PARTITION BY vgp.GuestProfileGenericNo) AS 'NumberVisits',
	
		ROW_NUMBER() OVER (PARTITION BY vgp.GuestProfileGenericNo ORDER BY res.ArrivalDate) AS 'N_Arrival',

		FORMAT(res.ArrivalDate, 'dd.MM.yyyy') AS 'DateArrival',
		FORMAT(res.DepartureDate, 'dd.MM.yyyy') AS 'DateDeparture'

		, ReservationStatus AS 'ReservationStatus'
		, DATEDIFF (day, res.ArrivalDate, res.DepartureDate) + 1 AS 'DaysInClinic'		-- Провел дней в клинике

		,prog.RateCode AS 'Rate_Passed'	-- Тариф пройден

		,CASE
			WHEN guestinfo.BirthDate IS NOT NULL THEN FORMAT(guestinfo.BirthDate, 'dd.MM.yyyy')
			WHEN vgp.BirthDate IS NOT NULL THEN FORMAT(vgp.BirthDate, 'dd.MM.yyyy')
			ELSE NULL
		END AS 'BirthDay'

		, CASE
			WHEN guestinfo.BirthDate is not NULL THEN DATEDIFF (day, guestinfo.BirthDate, res.ArrivalDate) / 365
			WHEN vgp.BirthDate is not NULL THEN DATEDIFF (day, vgp.BirthDate, res.ArrivalDate) / 365
			ELSE
				200
		END AS AgeOnArrival		 			-- Возраст на момент приезда

		, guestInfo.City AS 'City'
		, dgc.Name Geo,

		round(RAND(CHECKSUM(NewId()))*10000+1, 0) AS Amount_Bookings,
		round(RAND(CHECKSUM(NewId()))*1000+1, 0) AS Amount_Additionally
	
	from
		VReservationEx res
		inner join ReservationGuest rg on rg.ReservationId = res.ReservationId
		inner join GuestInfoBase guestInfo on guestInfo.Id = rg.Id
		left join VKivachGuestProfile as vgp on vgp.GuestProfileId = res.GuestProfileId
		left join dict.Room r on r.Id = res.RoomId
		left join dict.Location l on r.LocationId = l.Id
		inner join dict.RoomType rt on rt.Id = res.RoomTypeId
		inner join dict.Rate rate on rate.Id = res.RateId
		left join dict.GuaranteeKind kind on kind.Id = res.GuaranteeKindId
		inner join Folio f on f.Id = res.FolioId
		LEFT JOIN [dict].[GeoCode] dgc ON dgc.id=res.GeoCodeId
		LEFT JOIN [dict].[TrackCode] dtc ON dtc.id=res.TrackCodeId
		LEFT JOIN [dict].[OpenCode] doc ON doc.id=res.OpenCodeId
		LEFT JOIN [dict].[MarketSegment] dms ON dms.id=res.MarketSegmentId
		LEFT JOIN (SELECT GuestInfoBaseId GuestInfoBaseId
				, MAX(PhoneNumber) PhoneNumber
			FROM dbo.GuestPhone
			GROUP BY GuestInfoBaseId) gp ON gp.GuestInfoBaseId=vgp.GuestProfileId
		LEFT JOIN VCustomFieldValue VCustomFieldValue on substring(GuestProfileGenericNo,2,6)=VCustomFieldValue.AccountId AND CustomFieldCode='CF_ARRC'
		LEFT JOIN #program prog on prog.ReservationNo = res.ReservationGenericNo

	WHERE
		ReservationStatus IN ('IN','OUT')
		AND res.ArrivalDate >= @dateStart AND res.ArrivalDate < @dateEnd

		) A

	GO

	drop table if exists #program