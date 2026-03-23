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
<img width="980" height="663" alt="image" src="https://github.com/user-attachments/assets/c27efa6e-15ee-4b4d-a503-506693eab628" />

2.1 Set Variables: Создаем переменную пути к файлу.
Variable: CSV_FILE_PATH
Value: /home/dba/Downloads/datain/samplestore-general.csv 
<img width="1194" height="357" alt="image" src="https://github.com/user-attachments/assets/ba88b1f1-4ce2-4b44-8d6e-59932f7cd21a" />

2.2 Check File Exists: Проверка наличия файла ${CSV_FILE_PATH}.
<img width="407" height="170" alt="image" src="https://github.com/user-attachments/assets/d0ddcfcb-aedb-4ae6-8bce-f79bb2e61217" />

2.3 HTTP (Download): Загрузка файла, если его нет.
<img width="1209" height="771" alt="image" src="https://github.com/user-attachments/assets/7b17d075-8603-490d-a180-69ccbc81515c" />

2.4 Transformation. Последовательный вызов трех трансформаций для загрузки данных.
<img width="744" height="711" alt="image" src="https://github.com/user-attachments/assets/95581a0f-8a31-4f65-93ed-467775b2faef" />
<img width="744" height="718" alt="image" src="https://github.com/user-attachments/assets/7d2c8399-4dfc-462e-9eda-4efeecf8a617" />
<img width="745" height="711" alt="image" src="https://github.com/user-attachments/assets/85b87a2e-f127-468f-9a05-534bbefaf8b8" />


