Цель работы. Получить практические навыки создания сложного ETL-процесса, включающего динамическую загрузку файлов по HTTP, нормализацию базы данных, обработку дубликатов и настройку обработки ошибок с использованием Pentaho Data Integration (PDI).
1. Техническое обеспечение и окружение

Для выполнения работы используется виртуальная машина ETL_devops_26 (образ Ubuntu 22.04).
Необходимое ПО:

    ETL: Pentaho Data Integration 9.4
    СУБД: MySQL (удаленный сервер)
    Исходные данные: CSV-файл, загружаемый динамически.

Реквизиты подключения к СУБД (Target DB):

    Хост (Host): 95.131.149.21
    Порт (Port): 3306
    Веб-интерфейс (phpMyAdmin): Ссылка
    База данных: mgpu_ico_etl_XX (где XX — номер вашей базы, выданный преподавателем).
    Логин/Пароль. Запросить у ведущего преподавателя.

2. Подготовка базы данных

Перед запуском ETL-процесса необходимо создать структуру таблиц в вашей базе данных (mgpu_ico_etl_XX). Выполните следующий SQL-скрипт через phpMyAdmin или DBeaver:

-- 1. Таблица заказов (фактов)
DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
 row_id INT PRIMARY KEY,
 order_date DATE,
 ship_date DATE,
 ship_mode VARCHAR(50),
 sales DECIMAL(10,2),
 quantity INT,
 discount DECIMAL(4,2),
 profit DECIMAL(10,2),
 returned TINYINT(1) DEFAULT 0 -- 1 = Yes, 0 = No
);

-- 2. Таблица клиентов (измерение)
DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
 id INT AUTO_INCREMENT PRIMARY KEY,
 customer_id VARCHAR(20) NOT NULL,
 customer_name VARCHAR(100),
 segment VARCHAR(50),
 country VARCHAR(100),
 city VARCHAR(100),
 state VARCHAR(100),
 postal_code VARCHAR(20),
 region VARCHAR(50),
 INDEX idx_customer_id (customer_id),
 INDEX idx_region (region)
);

-- 3. Таблица продуктов (измерение)
DROP TABLE IF EXISTS products;
CREATE TABLE products (
 id INT AUTO_INCREMENT PRIMARY KEY,
 product_id VARCHAR(20) NOT NULL,
 category VARCHAR(50),
 sub_category VARCHAR(50),
 product_name VARCHAR(255),
 person VARCHAR(100),
 INDEX idx_product_id (product_id),
 INDEX idx_category (category),
 INDEX idx_subcategory (sub_category)
);

-- 4. Индексы и настройка кодировки
ALTER TABLE orders ADD INDEX idx_order_date (order_date);
ALTER TABLE orders ADD INDEX idx_ship_date (ship_date);

-- ЗАМЕНИТЕ mgpu_ico_etl_XX на имя вашей базы данных!
ALTER DATABASE mgpu_ico_etl_XX CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE customers CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
ALTER TABLE products CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

3. Ход работы
Шаг 1. Настройка Job (Главного задания)

Создайте Job (CSV_to_MySQL.kjb), который управляет всем процессом.

    Set Variables: Создайте переменную пути к файлу.
        Variable: CSV_FILE_PATH
        Value: /home/dba/Downloads/datain/samplestore-general.csv (или путь внутри вашей ВМ).
    Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.
    HTTP (Download): Загрузка файла, если его нет.
        URL: https://raw.githubusercontent.com/BosenkoTM/workshop-on-ETL/main/data_for_lessons/samplestore-general.csv
        Target file: ${CSV_FILE_PATH}
    Transformation. Последовательный вызов трех трансформаций для загрузки данных.

Шаг 2. Реализация Трансформаций (Transformations)

В каждой трансформации в шаге CSV Input используйте переменную ${CSV_FILE_PATH} вместо жесткого пути.
Трансформация 1. Load Orders

    Select Values. Установите типы данных (Date format: dd.MM.yyyy для дат, Integer для ID).
    Memory Group By. Используется для дедупликации (группировка по row_id, взятие первых значений по остальным полям).
    Filter Rows (Валидация):
        Условие: order_date IS NOT NULL AND ship_date IS NOT NULL.
        TRUE -> Table Output (в таблицу orders).
        FALSE -> Write to Log (логирование ошибок).
    Value Mapper. Преобразование поля Returned: Yes -> 1, No -> 0, Empty -> 0.

Трансформация 2. Load Customers

    Select Values. Оставьте только поля, относящиеся к клиенту (customer_id, name, city и т.д.).
    Memory Group By. Группировка по customer_id (устранение дублей клиентов).
    Table Output. Загрузка в таблицу customers.

Трансформация 3. Load Products

    Select Values. Оставьте поля продукта (product_id, category, name и т.д.).
    Memory Group By. Группировка по product_id.
    Table Output. Загрузка в таблицу products.

4. Варианты индивидуальных заданий

Вам необходимо модифицировать основной фильтр (в шаге Filter Rows) и выполнить два дополнительных задания (создать новые трансформации с расчетами/агрегацией и выводом в лог или временную таблицу).
№ 	Основной фильтр для загрузки в БД 	Доп. задание 1 (Аналитика) 	Доп. задание 2 (Аналитика)
1 	Заказы за 2016 год 	Анализ прибыльности по категориям 	Отчет по менеджерам
2 	Регион: только Central 	Статистика по способам доставки 	Анализ возвратов
3 	Сегмент: только Consumer 	Отчет по скидкам 	Анализ по штатам
4 	Категория: только Office Supplies 	Анализ продаж по месяцам 	Отчет по клиентам
5 	Размер заказа: Quantity > 3 	Статистика по регионам 	Анализ прибыльности
6 	Прибыль: Profit > 0 	Отчет по категориям 	Анализ доставки
7 	Скидка: Discount > 0 	Анализ по сегментам 	Отчет по возвратам
8 	Доставка: Standard Class 	Статистика продаж 	Анализ по городам
9 	Только заказы без возвратов 	Отчет по менеджерам 	Анализ категорий
10 	Страна: United States 	Анализ скидок 	Отчет по регионам
11 	Сумма продажи: Sales > 100 	Статистика по категориям 	Анализ клиентов
12 	Срок доставки > 5 дней 	Отчет по прибыли 	Анализ продаж
13 	Штат: только Texas 	Анализ возвратов 	Отчет по доставке
14 	Только заказы с возвратами 	Статистика по менеджерам 	Анализ регионов
15 	Подкатегория: только Art 	Отчет по городам 	Анализ скидок
16 	Заказы со скидкой > 15% 	Анализ категорий 	Отчет по сегментам
17 	Менеджер: конкретное Person 	Статистика доставки 	Анализ прибыли
18 	Заказы 1-го квартала 2016 	Отчет по регионам 	Анализ возвратов
19 	Маржа (Profit/Sales) > 0.2 	Анализ по штатам 	Отчет по категориям
20 	Срочная доставка (Same Day) 	Статистика по клиентам 	Анализ продаж
5. Отчетность и требования к сдаче
Формат отчета

Отчет оформляется в репозитории на GitHub или GitVerse. В системе Moodle прикрепляется только ссылка.

Структура репозитория:

    README.md:
        Вариант задания.
        Описание логики работы Job и трансформаций.
        Скриншоты настроек шага HTTP, Check File Exists.
        Скриншоты реализации дедупликации (Memory Group By) и обработки ошибок.
        SQL-запросы проверки данных в БД (COUNT строк, примеры данных).
    Файлы: .kjb (Job) и .ktr (Transformations).

Критерии оценки (Максимум 10 баллов)
Критерий 	Баллы 	Описание
Настройка соединения и HTTP 	2 	Корректная настройка динамического скачивания файла и переменных среды (Set Variables).
Нормализация и загрузка 	3 	Данные успешно разделены на 3 таблицы (orders, customers, products), связи соблюдены.
Обработка данных 	3 	Реализована дедупликация (удаление дублей) и обработка ошибок (валидация NULL полей с записью в лог).
Индивидуальное задание 	2 	Реализован фильтр по варианту и выполнены аналитические задачи (SQL или трансформация).


