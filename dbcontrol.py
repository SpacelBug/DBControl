import pymysql
import psycopg2

import sys
import time

from DBControl.table import dataTable

'''
Переводит время в формате "%Y-%m-%d %H:%M" в "Timestamp" 
'''
def toTimestamp(table, pos):
    for row in table:
        try:
            row[pos]=time.mktime(time.strptime(row[pos], "%Y-%m-%d %H:%M"))
        except Exception:
            row[pos]=0.0
    return table

'''
Главный класс
'''
class dataBase:
    '''
    Инициализатор/Конструктор класса

    Параменты на вход:
    -Имя СУБД
    -Логин к БД
    -Пароль
    -Адрес
    -Имя БД
    -Порт
    -схема (для postgres)
    '''
    def __init__(self, nameOfDBMS, login, password, adress, dbName, port, schemasName=''):
        try:
            self.DBMSname=nameOfDBMS.lower()
            self.login=login
            self.password=password
            self.adress=adress
            self.dbName=dbName
            self.port=port
            self.schemasName=schemasName
        except Exception:
            print("Ошибка инициализации параметров базы данных")
    '''
    Подключение к БД

    (только для MySQL и PostgreSQL)
    '''
    def connect(self):
        if(self.DBMSname=="mysql"):
            con = pymysql.connect(
                      host=self.adress,
                      user=self.login,
                      password=self.password,
                      db=self.dbName)
        elif(self.DBMSname=="postgresql"):
            con = psycopg2.connect(
                      database=self.dbName,
                      user=self.login,
                      password=self.password,
                      host=self.adress,
                      port=self.port
                      )
        else:
            print('Неподдерживаемая СУБД')
        return(con)
    '''
    Создает шаблоны таблиц БД
    -------------------------

    Словарь вида:
    Ключ (название таблицы): [название колонки, ...]
    '''
    def createTemplatesForTables(self):

        self.dictOfTablesTemplates={}

        with self.connect() as connection:
            mainCur = connection.cursor()
            if(self.DBMSname=="postgresql"):
                mainCur.execute("SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'")
            else:
                mainCur.execute('SHOW TABLES')
            for name in mainCur:
                subCur = connection.cursor()
                self.dictOfTablesTemplates[name[0]]=[]
                if(self.DBMSname=="postgresql"):
                    subCur.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{name[0]}'")
                else:
                    subCur.execute(f"SHOW COLUMNS FROM {name[0]}")
                for columnName in subCur:
                    self.dictOfTablesTemplates[name[0]].append(columnName[0])

    '''
    Просмотр созданных шаблонов

    (Если не указано имя таблицы,
    выведет все шаблоны)
    '''
    def showTablesTemplates(self, table=''):
        if (table==''):
            for template in self.dictOfTablesTemplates:
                print(f'{template}{self.dictOfTablesTemplates[template]}')
        else:
            try:
                print(self.dictOfTablesTemplates[table])
            except:
                print('Ошибка запроса')
    '''
    Запрос на выборку
    condition - для добавления условия
    (Придется писать ручками)
    (Если не указанны имена полей,
    выбирает все)
    '''
    def select(self, table, columns='*', condition=''):
        listOfValues = []
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                if (self.DBMSname == 'postgresql'):
                    query = f"SELECT {','.join(columns)} FROM {self.schemasName}.\"{table}\" {condition}"
                else:
                    query = f"SELECT {columns} FROM {table} {condition}"
                cursor.execute(query)
                for row in cursor:
                    listOfValues.append(list(row))
            except Exception:
                print(f'Ошибка запроса: SELECT \n {query}')
            return (dataTable(table, listOfValues))
    '''
    Запрос на изменение

    в таблице tableName, меняет target
    на elem где where = marker

    Только MySQL
    '''
    def update(self, tableName, elem, target, marker):
        with self.connect() as connection:
            cursor=connection.cursor()
            try:
                cursor.execute(f"UPDATE {tableName} SET {target} = '{elem}' WHERE {where} = '{marker}';")
                print(f"{tableName} обновлена")
            except Exception:
                print(f"Ошибка запроса\nUPDATE {tableName} SET {target} = '{elem}' WHERE {where} = '{marker}';")
    '''
    Запрос на вставку

    (Пока выводит только строки 
    sql запросов на вставку)

    Только MySQL
    '''
    def insert(self, table, columns, list_of_values):
        query = ''
        with self.connect() as connection:
            cursor = connection.cursor()
            try:
                if self.DBMSname == 'postgresql':
                    query = f"INSERT INTO {self.schemasName}.\"{table}\" ({','.join(columns)}) VALUES ({','.join(list_of_values)})"
                else:
                    query = f"INSERT INTO {self.schemasName}.\"{table}\" ({','.join(columns)}) VALUES ({','.join(list_of_values)})"
                print(query)
                # cursor.execute(query)
            except Exception:
                print(f'Ошибка запроса\n{query}')
    '''
    (Не проверен!)
    Запрос на удаление строк, выбираемых
    согласно couple

    couple = {column:value}
    column - должна быть строкой

    Пока выводит только sql запросы

    Только для MySQL
    '''
    def delete(self, table, couple):
        if (len(couple)==1):
            with self.connect() as connection:
                cursor=connection.cursor()
                try:
                    for name in couple:
                        if(str(type(name))=="<class 'str'>"):
                            print(f"DELETE FROM {table} WHERE {name}={couple[name]} LIMIT 1")
                            #cursor.execute(f"DELETE FROM {table} WHERE {name}={couple[name]} LIMIT 1")
                        else:
                            print('Имя столбца не явл строкой')
                except Exception:
                    print("Ошибка запроса")
        else:
            print("<couple>key не является парой")
    '''
    Просто джоин

    Вроде как работал но нужно проверить

    Только MySQL
    '''
    def join(self, mainTable, secondaryTables, column):
        listOfValues=[]
        with self.connect() as connection:
            cursor=connection.cursor()
            query=f"SELECT * FROM {mainTable} "
            for table in secondaryTables:
                query+=f"LEFT JOIN {table} ON {mainTable}.{column} = {table}.{column} "
            query+=f"ORDER BY {mainTable}.{column}"
            try:
                cursor.execute(query)
                for row in cursor:
                    listOfValues.append(row)
            except Exception:
                print(f"Ошибка запроса\n{query}")
            return(listOfValues)
    '''
    Экспортируем данные в таблицу(цы)

    Пока выводит просто SQL запросы
    '''
    def importData(self, tabelName, colNames, listOfValues,):
        query = ''
        for i in range(len(listOfValues)):
            for j in range(len(listOfValues[i])):
                if listOfValues[i][j] == '':
                    listOfValues[i][j] = 'Null'
        with self.connect() as connection:
            cursor = connection.cursor()
            for row in listOfValues:
                if self.DBMSname == 'postgresql':
                    query = f"INSERT INTO {self.schemasName}.\"{tabelName}\" ({ ','.join(colNames) }) values ({','.join(row)})"
                else:
                    query = f"INSERT INTO {tabelName} ({','.join(colNames)}) values ({','.join(row)})"
                cursor.execute(query)
        print('Import complete')
