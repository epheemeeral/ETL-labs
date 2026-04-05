# Лабораторная работа №2 — ETL-процесс с Pentaho Data Integration

## Вариант 17

| Параметр | Значение |
|---|---|
| **Основной фильтр** | Менеджер: Person = `Kelly Williams` |
| **Доп. задание 1** | Статистика доставки |
| **Доп. задание 2** | Анализ прибыли |

---

## Структура репозитория

```
lab2/
├── CSV_to_MySQL.kjb          # Главный Job
├── Load_Orders.ktr           # Трансформация: загрузка заказов
├── Load_Customers.ktr        # Трансформация: загрузка клиентов
├── Load_Products.ktr         # Трансформация: загрузка продуктов
├── Analytics_Delivery.ktr    # Доп. задание 1: статистика доставки
├── Analytics_Profit.ktr      # Доп. задание 2: анализ прибыли
└── README.md
```

---

## Техническое окружение

- **ETL**: Pentaho Data Integration 9.4
- **СУБД**: MySQL 8.0 (удалённый сервер `95.131.149.21:3306`)
- **БД**: `mgpu_ico_etl_17`
- **Исходные данные**: `samplestore-general.csv` (~10 000 строк, разделитель `;`)
- **ОС**: Ubuntu 22.04, Java OpenJDK 11

---

## Структура базы данных

```sql
CREATE TABLE orders (
    row_id      INT PRIMARY KEY,
    order_date  DATE,
    ship_date   DATE,
    ship_mode   VARCHAR(50),
    sales       DECIMAL(10,2),
    quantity    INT,
    discount    DECIMAL(4,2),
    profit      DECIMAL(10,2),
    returned    TINYINT(1) DEFAULT 0,
    person      VARCHAR(100)
);

CREATE TABLE customers (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    customer_id   VARCHAR(20) NOT NULL,
    customer_name VARCHAR(100),
    segment       VARCHAR(50),
    country       VARCHAR(100),
    city          VARCHAR(100),
    state         VARCHAR(100),
    postal_code   VARCHAR(20),
    region        VARCHAR(50),
    person        VARCHAR(100)
);

CREATE TABLE products (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    product_id    VARCHAR(20) NOT NULL,
    category      VARCHAR(50),
    sub_category  VARCHAR(50),
    product_name  VARCHAR(255),
    person        VARCHAR(100)
);
```

---

## Job: CSV_to_MySQL.kjb

Порядок выполнения:

```
START → Set Variables → Check File Exists
    ├─ [файл есть]  → Truncate Tables
    └─ [файла нет]  → HTTP Download → Truncate Tables
→ Load Orders → Load Customers → Load Products
→ Analytics Delivery → Analytics Profit → Success
```

| Шаг | Тип | Описание |
|---|---|---|
| **START** | Special | Точка входа |
| **Set Variables** | Set Variables | Переменная `CSV_FILE_PATH = /home/god/Downloads/datain/samplestore-general.csv`, scope: JVM |
| **Check File Exists** | File Exists | Проверяет наличие `${CSV_FILE_PATH}` |
| **HTTP Download** | HTTP | Скачивает CSV с GitHub если файл не найден |
| **Truncate Tables** | SQL | Очищает таблицы `orders`, `customers`, `products` |
| **Load Orders** | Transformation | Запускает `Load_Orders.ktr` |
| **Load Customers** | Transformation | Запускает `Load_Customers.ktr` |
| **Load Products** | Transformation | Запускает `Load_Products.ktr` |
| **Analytics Delivery** | Transformation | Запускает `Analytics_Delivery.ktr` |
| **Analytics Profit** | Transformation | Запускает `Analytics_Profit.ktr` |
| **Success** | Success | Завершение |

---

## Трансформация 1: Load Orders

**Файл**: `Load_Orders.ktr`

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`, разделитель `;`, кодировка UTF-8. Поля `Order Date` и `Ship Date` — тип Date, формат `dd/MM/yyyy`. |
| **Select Values** | Переименование полей в snake_case, конвертация типов: `row_id` → Integer, даты → Date, `sales`/`discount`/`profit` → Number. |
| **Value Mapper** | Преобразование поля `returned`: `Yes` → `1`, `No` → `0`, пустое → `0`. |
| **Memory Group By** | Дедупликация по `row_id`, для остальных полей — агрегация `FIRST`. |
| **Filter Rows** | Условие: `order_date IS NOT NULL AND ship_date IS NOT NULL AND person = 'Kelly Williams'`. TRUE → Table Output, FALSE → Write to Log. |
| **Table Output** | Запись в таблицу `orders`. |

---

## Трансформация 2: Load Customers

**Файл**: `Load_Customers.ktr`

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`, разделитель `;`, кодировка UTF-8. |
| **Select Values** | Выбор полей: `customer_id`, `customer_name`, `segment`, `country`, `city`, `state`, `postal_code`, `region`, `person`. |
| **Filter Rows** | Условие: `person = 'Kelly Williams'`. |
| **Memory Group By** | Дедупликация по `customer_id`, остальные поля — `FIRST`. |
| **Table Output** | Запись в таблицу `customers`. |

---

## Трансформация 3: Load Products

**Файл**: `Load_Products.ktr`

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`, разделитель `;`, кодировка UTF-8. |
| **Select Values** | Выбор полей: `product_id`, `category`, `sub_category`, `product_name`, `person`. |
| **Filter Rows** | Условие: `person = 'Kelly Williams'`. |
| **Memory Group By** | Дедупликация по `product_id`, остальные поля — `FIRST`. |
| **Table Output** | Запись в таблицу `products`. |

---

## Доп. задание 1: Статистика доставки

**Файл**: `Analytics_Delivery.ktr`

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`. |
| **Select Values** | Выбор полей: `ship_mode`, `order_date`, `ship_date`, `sales`, `quantity`. |
| **Calculator** | Вычисление `delivery_days = ship_date - order_date` (разница в днях). |
| **Memory Group By** | Группировка по `ship_mode`: COUNT заказов, SUM продаж, AVG дней доставки, SUM количества. |
| **Write to Log** | Вывод результата в лог. |

Результат:

| ship_mode | order_count | total_sales | avg_delivery_days | total_quantity |
|---|---|---|---|---|
| Standard Class | 1439 | ... | 5.0 | ... |
| Second Class | 465 | ... | 3.0 | ... |
| First Class | 299 | ... | 2.0 | ... |
| Same Day | 120 | ... | 0.0 | ... |

---

## Доп. задание 2: Анализ прибыли

**Файл**: `Analytics_Profit.ktr`

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`. |
| **Select Values** | Выбор полей: `category`, `region`, `sales`, `profit`, `discount`. |
| **Memory Group By** | Группировка по `category` + `region`: COUNT, SUM прибыли, AVG прибыли, AVG скидки. |
| **Write to Log** | Вывод результата в лог. |

Результат:

| category | region | order_count | total_profit | avg_profit | avg_discount |
|---|---|---|---|---|---|
| Furniture | West | 707 | 11504.95 | 16.27 | 0.13 |
| Office Supplies | South | 995 | 19986.39 | 20.09 | 0.17 |
| Technology | East | ... | ... | ... | ... |

---

## Проверка данных

### Количество строк

```sql
SELECT 'orders'    AS tbl, COUNT(*) AS cnt FROM orders
UNION ALL
SELECT 'customers' AS tbl, COUNT(*) AS cnt FROM customers
UNION ALL
SELECT 'products'  AS tbl, COUNT(*) AS cnt FROM products;
```

| tbl | cnt |
|---|---|
| orders | 2323 |
| customers | 629 |
| products | 1310 |

### Фильтрация по варианту

```sql
SELECT DISTINCT person FROM orders;
SELECT DISTINCT person FROM customers;
SELECT DISTINCT person FROM products;
-- Во всех трёх таблицах: Kelly Williams
```

### Дедупликация

```sql
SELECT row_id, COUNT(*) AS cnt FROM orders GROUP BY row_id HAVING cnt > 1;
-- 0 строк

SELECT customer_id, COUNT(*) AS cnt FROM customers GROUP BY customer_id HAVING cnt > 1;
-- 0 строк
```

### Маппинг returned

```sql
SELECT returned, COUNT(*) AS cnt FROM orders GROUP BY returned;
```

| returned | cnt |
|---|---|
| 0 | 2231 |
| 1 | 92 |

### Корректность дат

```sql
SELECT MIN(order_date), MAX(order_date) FROM orders;
-- 2014-01-06 / 2017-12-30

SELECT COUNT(*) FROM orders WHERE order_date IS NULL OR ship_date IS NULL;
-- 0
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
| Динамическое скачивание файла (HTTP) | Да |
| Переменная среды (Set Variables) | Да |
| Нормализация на 3 таблицы | Да |
| Дедупликация (Memory Group By) | Да |
| Обработка ошибок (Filter Rows + Write to Log) | Да |
| Фильтр по варианту (Person = Kelly Williams) | Да |
| Маппинг Returned → 0/1 (Value Mapper) | Да |
| Доп. задание 1: Статистика доставки | Да |
| Доп. задание 2: Анализ прибыли | Да |
