# Лабораторна робота №1 

Запуск програми

Потрібно виконати такі дії в даній послідовності
```shell
  https://github.com/VadimPusiak788/DB_Lab.git
  docker-compose up -d
  python -m venv env
  source env/bin/activate
  source .env
  python3 -m pip install -r requirements.txt
```
В папку `data_files` завантажити файли вигляду `OdataXXXXFile.csv`, де `XXXX` це рік проходження ЗНО.
І після цього виконати 
`python3 database.py`
Після виконання роботи програми видалити контейнери командою
`docker-compose down`

# Падіння бази даних

У випадку падіння бази даних, програма записує в іншу таблицю номер рядка на якого останнього закомітила. Після відновлення роботи бази даних програма починає записувати дані з того рядка на якому зупинилася.

