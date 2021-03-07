from conf import DB
import psycopg2
import csv
from psycopg2 import sql
import itertools
from datetime import datetime
import logging


logs = logging.getLogger(__name__)
logging.basicConfig(
    filename="logs.log",
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s  %(message)s",
    datefmt="%m-%d-%Y %H:%M:%S",
)

def determinate_types(file):
    data_types = {'OUTID': 'uuid'}

    with open('data_files/Odata2019File.csv', encoding="cp1251") as create_file:
        spam = csv.DictReader(create_file, delimiter=';')

        flag = 0
        for data in spam:
            for key, value in data.items():
                if value != 'null' and key not in data_types:
                    try:
                        float(value.replace(',', '.'))
                        data_types[key] = 'numeric'
                    except ValueError:
                        data_types[key] = 'text'
        data_types['year'] = 'numeric'



    return data_types


def create_table():
    logs.info("Start create table")

    command_create = 'CREATE TABLE zno_data('
    table_columns_types = determinate_types('data_files/Odata2019File.csv')
    command_create += ", ".join(f'{column} {value}' for column,
                              value in table_columns_types.items())
    command_create += ');'
    command_alter_pr = 'alter table zno_data add primary key (OUTID);'
    command_create_counter = 'CREATE TABLE counter_tbl(year INTEGER, counter INTEGER DEFAULT 0);'

    try:
        conn = psycopg2.connect(**DB)
        cur = conn.cursor()
        cur.execute('DROP TABLE IF EXISTS zno_data;')
        cur.execute(command_create)
        cur.execute(command_create_counter)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as e:
        logs.info(e)
    finally:
        if conn is not None:
            conn.close()
    cur.close()
    logs.info("Finish create table")


def select_counter(year):
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()

    cur.execute(
        'select counter from counter_tbl where year = %s;', (year,))

    counter = cur.fetchone()
    cur.close()
    conn.close()
    return counter


def insert_data(file):
    year = int(file[-12:-8])
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    counter = select_counter(year)
    if counter is None:
        cur.execute('insert into counter_tbl(year) values(%s)', (year,))
        counter = 0
    else:
        counter = counter[0]
    with open(file, encoding="cp1251") as create_file:
        spam = itertools.islice(csv.DictReader(
            create_file, delimiter=';'), int(counter), None)
        logs.info("Start insert data %s", year)

        for data in spam:
            
            column = [key.lower() for key, value in data.items() if value != 'null']
            values = [el.replace(',', '.') for el in data.values() if el != 'null']
            
            column.append('year')
            values.append(year)
            
            try:
                cur.execute(sql.SQL("insert into zno_data ({}) values ({})").format(
                    sql.SQL(', ').join(map(sql.Identifier, column)),
                    sql.SQL(', ').join(sql.Placeholder() * len(column))), values)
                counter += 1
            except Exception as e:
                logs.warning(e)
                conn.rollback()

            if counter % 40 == 0:
                try:
                    cur.execute('update counter_tbl set counter=%s where year=%s', (counter, year))
                    conn.commit()
                except psycopg2.OperationalError as e:
                    logs.warning(e)


        try:
            cur.execute('update counter_tbl set counter=%s where year=%s', (counter, year))

            conn.commit()
        except Exception as e:
            logs.warning(e)
            conn.rollback()

    logs.info("Finish insert data %s", year)

    cur.close()
    conn.close()



def write_data():
    conn = psycopg2.connect(**DB)
    cur = conn.cursor()
    logs.info("Start select data")

    cur.execute("select regname, min(engball100), year from zno_data where engteststatus='Зараховано' group by regname, year;")
    rows = cur.fetchall()
    with open('data_files/result.csv', mode='w') as file:
        fieldnames = ['Регіон', 'Мінімальний бал', 'Рік']

        result_writer = csv.writer(file, delimiter=',')
        result_writer.writerow(fieldnames)

        for row in rows:
            result_writer.writerow(row)

    logs.info("Data write the result.csv")


if __name__ == "__main__":
    create_table()
    insert_data('data_files/Odata2019File.csv')
    insert_data('data_files/Odata2020File.csv')
    write_data()
