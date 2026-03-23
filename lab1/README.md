# Лабораторная работа 1.1 Установка и настройка ETL-инструмента. Создание конвейеров данных

## Цель работы: 
Изучение основных принципов работы с ETL-инструментами на примере Pentaho Data Integration (PDI), настройка среды, создание конвейера обработки данных (фильтрация, очистка, замена значений) и выгрузка результатов в базу данных MySQL.

## Вариант 17	
[Программы лояльности: обработка бонусных данных](https://www.kaggle.com/datasets/arjunbhasin2013/ccdata)

### Описание данных
Используется датасет «Credit Card Dataset for Clustering» с платформы Kaggle. Датасет содержит информацию о поведении 8950 клиентов кредитных карт за последние 6 месяцев.

| Поле | Описание |
|------|----------|
| CUST_ID | Уникальный идентификатор клиента |
| BALANCE | Остаток на счёте |
| BALANCE_FREQUENCY | Частота обновления баланса (0-1) |
| PURCHASES | Сумма покупок |
| ONEOFF_PURCHASES | Сумма разовых покупок |
| INSTALLMENTS_PURCHASES | Сумма покупок в рассрочку |
| CASH_ADVANCE | Сумма снятия наличных |
| PURCHASES_FREQUENCY | Частота покупок (0-1) |
| ONEOFF_PURCHASES_FREQ. | Частота разовых покупок |
| PURCH._INSTALLMENTS_FREQ. | Частота покупок в рассрочку |
| CASH_ADVANCE_FREQUENCY | Частота снятия наличных |
| CASH_ADVANCE_TRX | Количество транзакций снятия |
| PURCHASES_TRX | Количество транзакций покупок |
| CREDIT_LIMIT | Кредитный лимит |
| PAYMENTS | Сумма платежей |
| MINIMUM_PAYMENTS | Минимальные платежи |
| PRC_FULL_PAYMENT | Доля полных платежей (0-1) |
| TENURE | Срок обслуживания (месяцы) |

### Создание ETL-конвейера

В Pentaho Spoon создана трансформация lab_01_loyalty_program.ktr, реализующая полный ETL-конвейер из 6 шагов:
1) CSV File Input
Шаг загружает данные из файла CC GENERAL.csv. Настроены типы данных для всех 18 столбцов: CUST_ID как String, числовые поля как Number (с точностью до 6 знаков), целочисленные поля (CASH_ADVANCE_TRX, PURCHASES_TRX, TENURE) как Integer.
<img width="974" height="622" alt="image" src="https://github.com/user-attachments/assets/7f71dec5-8d0e-482f-b33a-a9bb3c097669" />

2) String Operations
Применяется операция Trim (both) к полю CUST_ID для удаления начальных и конечных пробелов. Это гарантирует корректность идентификаторов клиентов при загрузке в базу данных.
<img width="974" height="158" alt="image" src="https://github.com/user-attachments/assets/6be74344-c517-4af9-9802-c99e01900401" />

3) Filter Rows
Фильтрация строк с пустым CUST_ID (условие: CUST_ID IS NOT NULL). Записи без идентификатора клиента отбрасываются как некорректные.
<img width="740" height="381" alt="image" src="https://github.com/user-attachments/assets/14d6a4e0-b1f4-417f-8412-e53b13af79bc" />

4) If Null (MINIMUM_PAYMENTS)
Замена пустых (NULL) значений в столбце MINIMUM_PAYMENTS на 0. В исходном датасете 313 записей (3.5%) с отсутствующими значениями.
<img width="787" height="601" alt="image" src="https://github.com/user-attachments/assets/9de91136-2042-4761-8d47-a02f491c84ce" />

5) If Null (CREDIT_LIMIT)
Замена пустых (NULL) значений в столбце CREDIT_LIMIT на 0. Обнаружена 1 запись без указания кредитного лимита.
<img width="786" height="600" alt="image" src="https://github.com/user-attachments/assets/9fdabfb5-b7df-4936-8e4d-70d961a1f515" />

6) Table Output
Очищенные данные загружаются в таблицу cc_loyalty базы данных mgpu_ico_etl_17 на сервере MySQL (95.131.149.21:3306). Перед вставкой таблица очищается (Truncate). Пакетная вставка по 1000 записей.
<img width="932" height="760" alt="image" src="https://github.com/user-attachments/assets/13bdface-ee4c-4b4a-9320-20b8f14c03c4" />

<img width="1201" height="684" alt="image" src="https://github.com/user-attachments/assets/08483b9a-693e-4bc0-9cf8-e19966a14558" />

Схема и запуск конвейера:
CSV File Input -> String Operations -> Filter empty CUST_ID -> Replace MINIMUM_PAYMENTS -> Replace CREDIT_LIMIT -> Table Output (MySQL)
<img width="1204" height="775" alt="image" src="https://github.com/user-attachments/assets/50cff0fd-77e2-4764-a3b0-c0903f5629eb" />


### Проверка результатов SQL-запросами

