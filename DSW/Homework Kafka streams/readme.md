# Study_netology

## Homework Kafka streams


Основное решение (модуль QuantityAlertsAppHomeWork.java):
	Поступают два потока (purchases и products). Объединяю их, агрегирую, фильтрую. 
	И записываю в топик product_quantity_alerts_dsl_homework.



-----------------------------------------------------------------------
Дополнительное решение (папка Additionally)
модули: ProductJoinerApp.java и QuantityAlertsApp.java)

а) ProductJoinerApp.java
	Принимаю два потока: (purchases и products).
	Объединяю их и записываю в новый поток: purchase_with_joined_product-dsl
	в виде:
	{"purchase_id": 120034, "purchase_quantity": 3, "productid": 75, "product_name": "117854", "product_price": 59.79334530432371, "purchase_summa": 179.38003591297112}
	где уже присутствует расчитанная сумма покупки.

б) QuantityAlertsApp.java
	Принимаю поток purchase_with_joined_product-dsl
	Арегирую, фильтрую и кидаю в поток: product_quantity_alerts-dsl
	
Основные скрины предоставил.