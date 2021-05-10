-- drop materialized VIEW dz_3_4;

-- создание материализованного представления
-- explain analyze
CREATE MATERIALIZED VIEW dz_3_4 AS
select 
	p.payment_id, p.customer_id, p.staff_id, p.rental_id, p.amount, payment_date,
	cu.last_name || ' ' || cu.first_name as customer,
	a.postal_code || ' ' || a.district || ' ' || a.city_id || ' ' || a.address as customer_postal_address,
	f.title as title_film,
	to_char(payment_date, 'Month YYYY') as month_payment,
	-- sum(amount) over (partition by p.customer_id, cast(date_trunc('month', payment_date) as date)) as sum_for_month,
	sum(amount) over (partition by p.customer_id, to_char(payment_date, 'Month YYYY')) as sum_for_month,
	to_char(payment_date, 'WW') as week_payment,
	-- to_char(payment_date, 'IW') as week_payment,
	-- sum(amount) over (partition by p.customer_id, cast(date_trunc('week', payment_date) as date)) as sum_for_week,
	sum(amount) over (partition by p.customer_id, to_char(payment_date, 'WW')) as sum_for_week,
	sum(amount) over (partition by to_char(payment_date, 'Month YYYY')) as sum_for_month_all,
	sum(amount) over (partition by to_char(payment_date, 'WW')) as sum_for_week_all,
	a2.postal_code || ' ' || a2.district || ' ' || a2.city_id || ' ' || a2.address || ' ' || a2.phone as store_address,
	s.last_name || ' ' || s.first_name as fio_staff
	-- ,p.customer_id
	-- ,p.payment_date 
	-- ,cast(date_trunc('week', p.payment_date) as date)	
	-- , s2.address_id 
from
	payment p
join customer cu on cu.customer_id = p.customer_id 
join address a on a.address_id = cu.address_id 
join rental r on r.rental_id = p.rental_id 
join inventory i on i.inventory_id = r.inventory_id 
join film f on f.film_id = i.film_id 
join staff s on p.staff_id=s.staff_id 
join store s2 on s2.store_id = s.store_id 
join address a2 on a2.address_id = S2.address_id
with data;


-- обновление материализованного представления
-- REFRESH MATERIALIZED VIEW dz_3-4;


-- explain analyze
select 
	*
from 
	dz_3_4;