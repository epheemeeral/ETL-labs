# Лабораторная работа 2.1 Динамические соединения с базами данных

## Цель работы
Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).

## Вариант 17	
Необходимо модифицировать основной фильтр (в шаге Filter Rows) и выполнить два дополнительных задания (создать новые трансформации с расчетами/агрегацией и выводом в лог или временную таблицу).

Основной фильтр для загрузки в БД	Доп: Менеджер: конкретное Person	
задание 1 (Аналитика): Статистика доставки	
Доп. задание 2 (Аналитика): Анализ прибыли

### 1. Подготовка базы данных

<img width="778" height="158" alt="image" src="https://github.com/user-attachments/assets/6a3f30f2-e5fd-4e7d-85eb-4455723f47ee" />

<img width="779" height="156" alt="image" src="https://github.com/user-attachments/assets/e62c469d-e22b-4d9a-80b2-2650b05e0522" />

<img width="782" height="147" alt="image" src="https://github.com/user-attachments/assets/6488ec92-9f97-4380-ad14-163babc3bbdf" />

### 2. Настройка Job (Главного задания)

Создаем Job (CSV_to_MySQL.kjb), который управляет всем процессом
<img width="984" height="668" alt="image" src="https://github.com/user-attachments/assets/79f60831-d521-4728-8ea1-51a41fcd055a" />

2.1 Set Variables: Создаем переменную пути к файлу.
<img width="1199" height="549" alt="image" src="https://github.com/user-attachments/assets/f052961b-3ec6-4141-8bb9-186eefad119f" />

2.2 Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.
<img width="412" height="179" alt="image" src="https://github.com/user-attachments/assets/edd8ba54-3f18-4de3-98de-3daea0213d55" />

2.3 HTTP (Download): Загрузка файла, если его нет.
<img width="1207" height="772" alt="image" src="https://github.com/user-attachments/assets/d58fb3d9-6497-462c-8f47-1cbe87b74847" />


2.4 Transformation. Последовательный вызов трех трансформаций для загрузки данных.
<img width="744" height="711" alt="image" src="https://github.com/user-attachments/assets/95581a0f-8a31-4f65-93ed-467775b2faef" />
<img width="744" height="718" alt="image" src="https://github.com/user-attachments/assets/7d2c8399-4dfc-462e-9eda-4efeecf8a617" />
<img width="745" height="711" alt="image" src="https://github.com/user-attachments/assets/85b87a2e-f127-468f-9a05-534bbefaf8b8" />

### Реализация Трансформаций (Transformations)

#### Трансформация 1: lab_02_1_csv_orders.ktr
Загрузка данных заказов в таблицу orders.

<img width="872" height="390" alt="image" src="https://github.com/user-attachments/assets/523cd931-c59e-4c8f-9faa-6c0f0b54569e" />


| Шаг | Описание |
|-----|----------|
| **CSV file input** | Чтение файла `${CSV_FILE_PATH}`, разделитель `;`, кодировка UTF-8, 23 поля. |
| **Select values** | Переименование полей в snake_case (`Row ID` → `row_id`, `Order Date` → `order_date` и т.д.). Конвертация типов: `row_id` → Integer, `order_date` и `ship_date` → Date (dd.MM.yyyy). |
| **Memory group by** | Группировка по полям: `row_id`, `order_date`, `ship_date`, `ship_mode`, `sales`, `quantity`, `discount`, `profit`, `returned`. Устраняет дублирующиеся записи. |
| **Filter rows** | Условие: `order_date IS NOT NULL AND ship_date IS NOT NULL`. **TRUE** → Value mapper (далее в БД). **FALSE** → Write to log (логирование ошибочных записей). |
| **Value mapper** | Преобразование поля `returned`: `Yes` → `1`, `No` → `0`, пустое → `0`. |
| **Table output** | Загрузка в таблицу `orders`. Truncate before insert. Batch по 1000 записей. |

Проверка: <img width="461" height="83" alt="image" src="https://github.com/user-attachments/assets/bc53a063-eca9-4a03-a5a0-4dbb854dc19f" />

---

#### Трансформация 2: lab_02_2_csv_to_Сustomers.ktr
Загрузка уникальных клиентов в таблицу customers.

<img width="978" height="648" alt="image" src="https://github.com/user-attachments/assets/ec0a0e0b-085b-4d84-a41c-0fe1bd02dc33" />


| Шаг | Описание |
|-----|----------|
| **Select values** | Выбор 8 полей клиента: `customer_id`, `customer_name`, `segment`, `country`, `city`, `state`, `postal_code`, `region`. |
| **Memory group by** | Дедупликация по всем полям клиента. Устраняет повторяющихся клиентов. |
| **Table output** | Загрузка уникальных клиентов в таблицу `customers`. |

Проверка: <img width="445" height="83" alt="image" src="https://github.com/user-attachments/assets/8b65e978-10f3-41d8-b13a-dbe7c5334b4c" />

---

#### Трансформация 3: lab_02_3_csv_to_products.ktr
Загрузка уникальных продуктов в таблицу products.

<img width="977" height="664" alt="image" src="https://github.com/user-attachments/assets/9104163f-b6c0-446c-9ed4-789b5283e858" />

| Шаг | Описание |
|-----|----------|
| **Select values** | Выбор 5 полей продукта: `product_id`, `category`, `sub_category`, `product_name`, `person`. |
| **Memory group by** | Дедупликация по `product_id` и остальным полям. |
| **Table output** | Загрузка уникальных продуктов в таблицу `products`. |

Проверка: <img width="461" height="88" alt="image" src="https://github.com/user-attachments/assets/0fedc49c-a92b-4e58-8c78-3d37b2a8c5a8" />


## Индивидуальное задание 

### Статистика доставки (задание 1)
Трансформация lab_02_4_delivery_stats.ktr анализирует время доставки.
<img width="944" height="381" alt="image" src="https://github.com/user-attachments/assets/0bacb27b-3a4f-4e37-b684-0cffd910be7d" />

| Шаг | Описание |
|-----|----------|
| **Calculator** | Вычисление `delivery_days = ship_date - order_date` (разница в днях). |
| **Memory group by** | Группировка по `ship_mode` с агрегатами: <br> • Среднее время доставки (AVG) <br> • Минимальное время (MIN) <br> • Максимальное время (MAX) <br> • Количество заказов (COUNT) |

---

### Анализ прибыли (задание 2)
Трансформация lab_02_5_profit_analysis.ktr анализирует прибыль менеджера Chuck Magee по категориям и подкатегориям.

<img width="961" height="412" alt="image" src="https://github.com/user-attachments/assets/067e2b73-8e12-4ff0-8591-ba6d56bac2de" />

| Шаг | Описание |
|-----|----------|
| **Filter rows** | Фильтрация: `person = 'Chuck Magee'`. |
| **Memory group by** | Группировка по `category`, `sub_category`: <br> • Общая сумма продаж (SUM sales) <br> • Общая прибыль (SUM profit) <br> • Средняя скидка (AVG discount) <br> • Количество заказов (COUNT) |
| **Sort rows** | Сортировка по прибыли (убывание) для выявления самых прибыльных подкатегорий. |

