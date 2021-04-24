-- Создание таблицы not_active_customer
-- DROP TABLE public.not_active_customer;

CREATE TABLE public.not_active_customer (
	id serial NOT null primary key,
	-- customer_id int2 NOT NULL,
	customer_id int2 NOT NULL references public.customer(customer_id),	
	not_active_date date not NULL DEFAULT now()::date
);



-- Создание триггеной функции
create or replace function f_not_active_customer() returns trigger as $$
	begin 
		if old.active=1 and new.active=0 and TG_OP = 'UPDATE' then
			-- insert into public.not_active_customer select new.customer_id;
			INSERT INTO public.not_active_customer(customer_id) VALUES(NEW.customer_id);
		end if;
		return null;
	end;
$$ language plpgsql;



-- создание триггера
drop trigger if exists audit_active_customer on public.customer;
create trigger audit_active_customer
after update on public.customer 
	for each row execute function f_not_active_customer(customer_id);
	


-- проверка:
update public.customer SET active = 0 WHERE customer_id < 21;
