-- функция, в виде аргументов принимает две даты и в качестве 
-- результата возвращает сумму продаж между этими датами, включая эти даты.

-- ВАРИАНТ 1
create or replace function sales_amount_period(d1 date, d2 date, out sum_sales numeric(9,2))
as $$
begin 
	d2 = d2 + 1;
	select 
		sum(f.rental_rate)
	from 
		rental r 
	into sum_sales
	left join inventory i on i.inventory_id = r.inventory_id
	left join film f on f.film_id = i.film_id 
	where 
		Rental_date between d1 and d2;
	return;
end $$
language plpgsql;

select sales_amount_period('2005/05/24', '2005/05/24');	-- 19.92
select sales_amount_period('2005/05/24', '2005/05/25');	-- 443,55


-- ВАРИАНТ 2
create or replace function sales_amount_period_2(d1 date, d2 date) returns numeric(9,2)
as $$
declare 
	sum_sales numeric(9,2);
begin 
	d2 = d2 + 1;
	select 
		sum(f.rental_rate)
	from 
		rental r 
	into sum_sales
	left join inventory i on i.inventory_id = r.inventory_id
	left join film f on f.film_id = i.film_id 
	where 
		Rental_date between d1 and d2;
	return sum_sales;
end $$
language plpgsql;

select sales_amount_period_2('2005/05/24', '2005/05/24');	-- 19.92
select sales_amount_period_2('2005/05/24', '2005/05/25');	-- 443,55