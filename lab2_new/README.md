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
-- Таблица заказов (факты)
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

-- Таблица клиентов (измерение)
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

-- Таблица продуктов (измерение)
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

## Описание Job (CSV_to_MySQL.kjb)

Job управляет всем ETL-процессом и выполняет шаги в следующем порядке:

```
START
  → Set Variables
  → Check File Exists
      ├─ [файл существует] → Truncate Tables
      └─ [файла нет]       → HTTP Download → Truncate Tables
  → Load Orders
  → Load Customers
  → Load Products
  → Analytics Delivery
  → Analytics Profit
  → Success
```

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

### Настройка шага Set Variables

- **Variable**: `CSV_FILE_PATH`
- **Value**: `/home/god/Downloads/datain/samplestore-general.csv`
- **Scope**: JVM (доступна всем трансформациям)

### Настройка шага HTTP Download

- **URL**: `https://raw.githubusercontent.com/BosenkoTM/workshop-on-ETL/main/data_for_lessons/samplestore-general.csv`
- **Target file**: `${CSV_FILE_PATH}`

> Скриншот шага HTTP:

![HTTP Step](screenshots/http_step.png)

### Настройка шага Check File Exists

- **Filename**: `${CSV_FILE_PATH}`
- При успехе (файл есть) → переход к `Truncate Tables`
- При ошибке (файла нет) → переход к `HTTP Download`

> Скриншот шага Check File Exists:

![Check File Exists](screenshots/check_file_exists.png)

---

## Трансформация 1: Load Orders

**Файл**: `Load_Orders.ktr`

**Пайплайн**:
```
CSV file input
  → Select Values (переименование и конвертация типов)
  → Value Mapper (Returned: Yes→1, No→0, empty→0)
  → Memory Group By (дедупликация по row_id)
  → Filter Rows (фильтр по варианту + валидация)
      ├─ TRUE  → Table Output (таблица orders)
      └─ FALSE → Write to Log (лог ошибок)
```

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
