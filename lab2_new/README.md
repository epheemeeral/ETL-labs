# Лабораторная работа №2 — ETL-процесс с Pentaho Data Integration

## Вариант 17

| Параметр | Значение |
|---|---|
| **Основной фильтр** | Менеджер: Person = `Kelly Williams` |
| **Доп. задание 1** | Статистика доставки |
| **Доп. задание 2** | Анализ прибыли |

---

### 1. Подготовка базы данных

<img width="778" height="158" alt="image" src="https://github.com/user-attachments/assets/6a3f30f2-e5fd-4e7d-85eb-4455723f47ee" />

<img width="779" height="156" alt="image" src="https://github.com/user-attachments/assets/e62c469d-e22b-4d9a-80b2-2650b05e0522" />

<img width="782" height="147" alt="image" src="https://github.com/user-attachments/assets/6488ec92-9f97-4380-ad14-163babc3bbdf" />
---


### 2. Настройка Job (Главного задания)

Создаем Job (CSV_to_MySQL.kjb), который управляет всем процессом
<img width="1319" height="629" alt="image" src="https://github.com/user-attachments/assets/07720850-2129-498f-a17e-790bac76039f" />

2.1 Set Variables: Создаем переменную пути к файлу.
<img width="755" height="384" alt="image" src="https://github.com/user-attachments/assets/3cf3665a-6324-406d-87a2-2c91020e6d89" />

2.2 Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.
<img width="336" height="151" alt="image" src="https://github.com/user-attachments/assets/a948c3fd-2b50-4fa4-a5a3-ef8b594733b7" />

2.3 HTTP (Download): Загрузка файла, если его нет.
<img width="1115" height="802" alt="image" src="https://github.com/user-attachments/assets/af27fe85-c9f3-43e4-8afd-b83047b27af2" />

### Шаги Job

| Шаг | Тип | Описание |
|---|---|---|
| **START** | Special | Точка входа |
| **Set Variables** | Set Variables | Устанавливает переменную `CSV_FILE_PATH = /home/.../samplestore-general.csv` |
| **Check File Exists** | File Exists | Проверяет наличие CSV-файла по пути `${CSV_FILE_PATH}` |
| **HTTP Download** | HTTP | Скачивает CSV из GitHub если файл отсутствует |
| **Truncate Tables** | SQL | Очищает таблицы `orders`, `customers`, `products` перед загрузкой |
| **Load Orders** | Transformation | Запускает `Load_Orders.ktr` |
| **Load Customers** | Transformation | Запускает `Load_Customers.ktr` |
| **Load Products** | Transformation | Запускает `Load_Products.ktr` |
| **Analytics Delivery** | Transformation | Запускает `Analytics_Delivery.ktr` |
| **Analytics Profit** | Transformation | Запускает `Analytics_Profit.ktr` |
| **Success** | Success | Завершение |


## Трансформация 1: Load Orders

### Шаги

**CSV file input**
- Файл: `${CSV_FILE_PATH}`
- Разделитель: `;`
- Кодировка: UTF-8
- Заголовок: первая строка
- Поля `Order Date`, `Ship Date` читаются как `Date` с форматом `dd/MM/yyyy`

**Select Values**

| Поле CSV | Новое имя | Тип |
|---|---|---|
| Row ID | row_id | Integer |
| Order Date | order_date | Date (dd/MM/yyyy) |
| Ship Date | ship_date | Date (dd/MM/yyyy) |
| Ship Mode | ship_mode | String |
| Sales | sales | Number |
| Quantity | quantity | Integer |
| Discount | discount | Number |
| Profit | profit | Number |
| Returned | returned | String |
| Person | person | String |

**Value Mapper** — преобразование поля `returned`:

| Входное значение | Выходное значение |
|---|---|
| `Yes` | `1` |
| `No` | `0` |
| *(пусто)* | `0` |

**Memory Group By** — дедупликация:
- Группировка по: `row_id`
- Для всех остальных полей: агрегация `FIRST`

> Скриншот Memory Group By:

![Memory Group By](screenshots/memory_group_by_orders.png)

**Filter Rows** — фильтр (вариант 17) + валидация:
```
order_date IS NOT NULL
AND ship_date IS NOT NULL
AND person = 'Kelly Williams'
```
- `TRUE` → Table Output → таблица `orders`
- `FALSE` → Write to Log → запись отклонённых строк в лог

> Скриншот Filter Rows:

![Filter Rows](screenshots/filter_rows_orders.png)

---

## Трансформация 2: Load Customers

**Файл**: `Load_Customers.ktr`

**Пайплайн**:
```
CSV file input
  → Select Values (поля клиента + person)
  → Filter Rows (person = 'Kelly Williams')
  → Memory Group By (дедупликация по customer_id)
  → Table Output (таблица customers)
```

**Select Values** — оставляет поля: `customer_id`, `customer_name`, `segment`, `country`, `city`, `state`, `postal_code`, `region`, `person`

**Filter Rows**: `person = 'Kelly Williams'`

**Memory Group By**: группировка по `customer_id`, остальные поля — `FIRST`

> Скриншот Memory Group By:

![Memory Group By Customers](screenshots/memory_group_by_customers.png)

---

## Трансформация 3: Load Products

**Файл**: `Load_Products.ktr`

**Пайплайн**:
```
CSV file input
  → Select Values (поля продукта + person)
  → Filter Rows (person = 'Kelly Williams')
  → Memory Group By (дедупликация по product_id)
  → Table Output (таблица products)
```

**Select Values** — оставляет поля: `product_id`, `category`, `sub_category`, `product_name`, `person`

**Filter Rows**: `person = 'Kelly Williams'`

**Memory Group By**: группировка по `product_id`, остальные поля — `FIRST`

---

## Доп. задание 1: Статистика доставки (Analytics_Delivery.ktr)

**Пайплайн**:
```
CSV file input
  → Select Values (ship_mode, order_date, ship_date, sales, quantity)
  → Calculator (delivery_days = ship_date - order_date)
  → Memory Group By (группировка по ship_mode)
  → Write to Log
```

**Результат** — статистика по способам доставки:

| ship_mode | order_count | total_sales | avg_delivery_days | total_quantity |
|---|---|---|---|---|
| Standard Class | 1439 | ... | 5.0 | ... |
| Second Class | 465 | ... | 3.0 | ... |
| First Class | 299 | ... | 2.0 | ... |
| Same Day | 120 | ... | 0.0 | ... |

---

## Доп. задание 2: Анализ прибыли (Analytics_Profit.ktr)

**Пайплайн**:
```
CSV file input
  → Select Values (category, region, sales, profit, discount)
  → Memory Group By (группировка по category + region)
  → Write to Log
```

**Результат** — анализ прибыли по категориям и регионам:

| category | region | order_count | total_profit | avg_profit | avg_discount |
|---|---|---|---|---|---|
| Furniture | West | 707 | 11504.95 | 16.27 | 0.13 |
| Office Supplies | South | 995 | 19986.39 | 20.09 | 0.17 |
| Technology | East | ... | ... | ... | ... |
| ... | ... | ... | ... | ... | ... |

---

## SQL-запросы проверки данных

### Количество строк в таблицах

```sql
SELECT 'orders'    AS tbl, COUNT(*) AS cnt FROM orders
UNION ALL
SELECT 'customers' AS tbl, COUNT(*) AS cnt FROM customers
UNION ALL
SELECT 'products'  AS tbl, COUNT(*) AS cnt FROM products;
```

**Результат:**

| tbl | cnt |
|---|---|
| orders | 2323 |
| customers | 629 |
| products | 1310 |

### Проверка фильтрации по варианту (только Kelly Williams)

```sql
SELECT DISTINCT person FROM orders;
SELECT DISTINCT person FROM customers;
SELECT DISTINCT person FROM products;
-- Результат во всех трёх: Kelly Williams
```

### Примеры данных из таблицы orders

```sql
SELECT * FROM orders LIMIT 5;
```

| row_id | order_date | ship_date | ship_mode | sales | quantity | discount | profit | returned | person |
|---|---|---|---|---|---|---|---|---|---|
| 15 | 2017-11-22 | 2017-11-26 | Standard Class | 68.81 | 5 | 0.80 | -123.86 | 0 | Kelly Williams |
| 16 | 2017-11-22 | 2017-11-26 | Standard Class | 2.54 | 3 | 0.80 | -3.82 | 0 | Kelly Williams |
| 17 | 2016-11-11 | 2016-11-18 | Standard Class | 665.88 | 6 | 0.00 | 13.32 | 0 | Kelly Williams |

### Проверка дедупликации

```sql
-- Дубликатов нет: row_id уникален
SELECT row_id, COUNT(*) AS cnt FROM orders GROUP BY row_id HAVING cnt > 1;
-- Результат: 0 строк

-- Дубликатов нет: customer_id уникален
SELECT customer_id, COUNT(*) AS cnt FROM customers GROUP BY customer_id HAVING cnt > 1;
-- Результат: 0 строк
```

### Проверка маппинга returned

```sql
SELECT returned, COUNT(*) AS cnt FROM orders GROUP BY returned;
```

| returned | cnt |
|---|---|
| 0 | 2231 |
| 1 | 92 |

### Проверка корректности дат

```sql
SELECT MIN(order_date), MAX(order_date) FROM orders;
-- Результат: 2014-01-06 / 2017-12-30
SELECT COUNT(*) FROM orders WHERE order_date IS NULL OR ship_date IS NULL;
-- Результат: 0
```

### Статистика по способам доставки

```sql
SELECT ship_mode, COUNT(*) AS cnt, ROUND(AVG(profit), 2) AS avg_profit
FROM orders
GROUP BY ship_mode
ORDER BY cnt DESC;
```

| ship_mode | cnt | avg_profit |
|---|---|---|
| Standard Class | 1439 | 17.62 |
| Second Class | 465 | 19.60 |
| First Class | 299 | 12.40 |
| Same Day | 120 | 12.77 |

---

## Итоги

| Критерий | Выполнено |
|---|---|
| Динамическое скачивание файла (HTTP) | ✅ |
| Переменная среды (Set Variables) | ✅ |
| Нормализация на 3 таблицы | ✅ |
| Дедупликация (Memory Group By) | ✅ |
| Обработка ошибок (Filter Rows + Write to Log) | ✅ |
| Фильтр по варианту (Person = Kelly Williams) | ✅ |
| Маппинг Returned → 0/1 (Value Mapper) | ✅ |
| Доп. задание 1: Статистика доставки | ✅ |
| Доп. задание 2: Анализ прибыли | ✅ |
