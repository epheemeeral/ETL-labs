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
Set Variables: Создайте переменную пути к файлу.
Variable: CSV_FILE_PATH
Value: /home/dba/Downloads/datain/samplestore-general.csv (или путь внутри вашей ВМ).
Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.
HTTP (Download): Загрузка файла, если его нет.
URL: https://raw.githubusercontent.com/BosenkoTM/workshop-on-ETL/main/data_for_lessons/samplestore-general.csv
Target file: ${CSV_FILE_PATH}
Transformation. Последовательный вызов трех трансформаций для загрузки данных.
