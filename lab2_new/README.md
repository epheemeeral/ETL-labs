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

**Файл**: `CSV_to_MySQL.kjb`

Создаем Job (CSV_to_MySQL.kjb), который управляет всем процессом
<img width="1319" height="629" alt="image" src="https://github.com/user-attachments/assets/07720850-2129-498f-a17e-790bac76039f" />

2.1 Set Variables: Создаем переменную пути к файлу.
<img width="755" height="384" alt="image" src="https://github.com/user-attachments/assets/3cf3665a-6324-406d-87a2-2c91020e6d89" />

2.2 Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.
<img width="336" height="151" alt="image" src="https://github.com/user-attachments/assets/a948c3fd-2b50-4fa4-a5a3-ef8b594733b7" />

2.3 HTTP (Download): Загрузка файла, если его нет.
<img width="1115" height="802" alt="image" src="https://github.com/user-attachments/assets/af27fe85-c9f3-43e4-8afd-b83047b27af2" />

2.4 Transformation. Последовательный вызов трех трансформаций для загрузки данных.
<img width="744" height="711" alt="image" src="https://github.com/user-attachments/assets/95581a0f-8a31-4f65-93ed-467775b2faef" />
<img width="744" height="718" alt="image" src="https://github.com/user-attachments/assets/7d2c8399-4dfc-462e-9eda-4efeecf8a617" />
<img width="745" height="711" alt="image" src="https://github.com/user-attachments/assets/85b87a2e-f127-468f-9a05-534bbefaf8b8" />

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

<img width="998" height="771" alt="image" src="https://github.com/user-attachments/assets/d111f117-3a33-4db8-823f-2c680ac4cbbe" />

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

<img width="971" height="564" alt="image" src="https://github.com/user-attachments/assets/3801d6db-f387-4c11-a3e6-bff07cd8010b" />

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

<img width="969" height="563" alt="image" src="https://github.com/user-attachments/assets/ff1f2a7e-b922-4615-910a-6dc7403ad6d5" />

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

<img width="946" height="566" alt="image" src="https://github.com/user-attachments/assets/7aa089de-55da-475f-a105-4ffa41acfb7f" />

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`. |
| **Select Values** | Выбор полей: `ship_mode`, `order_date`, `ship_date`, `sales`, `quantity`. |
| **Calculator** | Вычисление `delivery_days = ship_date - order_date` (разница в днях). |
| **Memory Group By** | Группировка по `ship_mode`: COUNT заказов, SUM продаж, AVG дней доставки, SUM количества. |
| **Write to Log** | Вывод результата в лог. |


---

## Доп. задание 2: Анализ прибыли

**Файл**: `Analytics_Profit.ktr`

<img width="977" height="553" alt="image" src="https://github.com/user-attachments/assets/455cc967-4982-433c-8ae4-db18534e41a1" />

| Шаг | Описание |
|---|---|
| **CSV file input** | Чтение `${CSV_FILE_PATH}`. |
| **Select Values** | Выбор полей: `category`, `region`, `sales`, `profit`, `discount`. |
| **Memory Group By** | Группировка по `category` + `region`: COUNT, SUM прибыли, AVG прибыли, AVG скидки. |
| **Write to Log** | Вывод результата в лог. |

---

## Проверка данных

### Количество строк

<img width="1377" height="332" alt="image" src="https://github.com/user-attachments/assets/dc04833f-9fc6-4073-892b-34dee278d708" />

### Фильтрация по варианту

<img width="603" height="895" alt="image" src="https://github.com/user-attachments/assets/256f6524-029d-4ea3-98ec-bad60e849cb9" />

<img width="606" height="277" alt="image" src="https://github.com/user-attachments/assets/28383e7f-5819-4998-8bc5-a2e126a3c3e9" />


### Дедупликация

<img width="621" height="160" alt="image" src="https://github.com/user-attachments/assets/f0b53852-4984-4e34-8892-5ce62785a7c2" />

<img width="724" height="157" alt="image" src="https://github.com/user-attachments/assets/1f585e67-b095-44d1-9540-ff3a5e9975d9" />


### Маппинг returned

<img width="525" height="298" alt="image" src="https://github.com/user-attachments/assets/e9e18a97-c134-4984-9800-0a986cb45fd8" />


### Корректность дат

<img width="496" height="279" alt="image" src="https://github.com/user-attachments/assets/8e0a3c80-d732-49b6-b9c0-237b14664e4d" />

<img width="617" height="212" alt="image" src="https://github.com/user-attachments/assets/47a895a0-1620-4b85-9486-8a698ee9d3d8" />


### Статистика по способам доставки

<img width="975" height="345" alt="image" src="https://github.com/user-attachments/assets/337b1293-dd2c-4799-b940-9d642ce58b99" />

---

