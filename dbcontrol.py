import pymysql
import psycopg2

import datetime
import time

def toTimestamp(table, pos):
    for row in table:
        try:
            row[pos]=time.mktime(time.strptime(row[pos], "%Y-%m-%d %H:%M"))
        except Exception:
            row[pos]=0.0
    return table

class dataTable:
    def __init__(self, name='', data=[]):
        self.tableName = name
        self.tableData = data
        self.tableSize = {"rows": len(self.tableData), "cols": len(self.tableData)}
    def showData (self, count=0):
        if count == 0:
            count = len(self.tableData)
        for indx in range(count):
            print(self.tableData[indx])
    def getRow (self, rowIndx=''):
        try:
            return(self.tableData[rowIndx])
        except:
            print(f"ERROR: check parameters of fuction")
    def showRow (self, rowIndx=''):
        try:
            print(self.tableData[rowIndx])
        except:
            print(f"ERROR: check parameters of fuction")
    def conColumns(self, firsPosIndex, secoundPosIndex):
        for row in self.tableData:
            secoundElem=row.pop(secoundPosIndex)
            row[firsPosIndex]=f'{row[firsPosIndex]} {str(secoundElem)}'

class dataBase:
	'''
	Инициализатор/Конструктор класса

	Параменты на вход:
	-Имя СУБД
	-Логин к БД
	-Пароль
	-Адрес
	-Имя БД
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

	def rowToSqlString(self, row):
		counter=0
		sqlString=""
		for elem in row:
			if counter!=len(row):
				if str(type(elem))=="<class 'str'>":
					sqlString+=f"'{elem}',"
				else:
					sqlString+=f"{elem},"
			else:
				if str(type(elem))=="<class 'str'>":
					sqlString+=f"'{elem}'"
				else:
					sqlString+=f"{elem}"
		return(sqlString)
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
	Просто собирает удобные таблицы
	'''
	def pythonSQLTable(self, tableName):

		table=[]

		with self.connect() as connection:
			mainCur = connection.cursor()
			if(self.DBMSname=="postgresql"):
				mainCur.execute(f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = N'{tableName}'")
			else:
				mainCur.execute(f"SHOW COLUMNS FROM {tableName}")
			for colName in mainCur:
				table.append({colName[0]:[]})
			for col in table:
				for colName in col:
					mainCur = connection.cursor()
					mainCur.execute(f"SELECT {colName} FROM {tableName}")
					for item in mainCur:
						col[colName].append(item)

		return(table)

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
				return(self.dictOfTablesTemplates[table])
			except:
				print('Ошибка запроса')
	'''
	Запрос на вставку

	(пока не использовать!)
	'''
	def insert(self, table, columns, listOfValues):
		print()
		with self.connect() as connection:
			cursor=connection.cursor()
			try:
				#cursor.execute(f"INSERT INTO {table} ({columns}) VALUES ({listOfValues})")
				print(f"INSERT INTO {table} ({(columns)}) VALUES ({(listOfValues)})")
			except Exception:
				print(f"Ошибка запроса\nINSERT INTO {table} ({(columns)}) VALUES ({(listOfValues)})")
	'''
	Запрос на выборку

	condition - для добавления условия
	(Придется писать ручками)

	(Если не указанны имена полей,
	выбирает все)
	'''
	def select(self, table, columns='*', condition=''):
		listOfValues=[]
		with self.connect() as connection:
			cursor=connection.cursor()
			try:
				if (self.DBMSname=='postgresql'):
					cursor.execute(f"SELECT {','.join(columns)} FROM {self.schemasName}\"{table}\" {condition}")
				else:
					cursor.execute(f"SELECT {','.join(columns)} FROM {table} {condition}")
				for row in cursor:
					if(columns=='*'):
						listOfValues.append(row)
					else:
						listOfValues.append(row[0])	
			except Exception:
				print(f'Ошибка запроса: SELECT ')
			return(listOfValues)
	'''
	Просто джоин
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
	Запрос на изменение

	в таблице tableName, меняет target
	на elem где where = marker
	'''
	def update(self, tableName, elem, target, marker):
		with self.connect() as connection:
			cursor=connection.cursor()
			try:
				cursor.execute(f"UPDATE {tableName} SET {target} = '{elem}' WHERE {where} = '{marker}';")
				print(f"{tableName} обновлена")
			except Exception:
				print(f"Ошибка запроса\nUPDATE {tableName} SET {target} = '{elem}' WHERE {where} = '{marker}';")
				
	def changeColValues(table, tableName, where, target):
		for row in table:
			counter=0
			for elem in row:
				with self.connect() as connection:
					cursor=connection.cursor()
					try:
						cursor.execute(f"SELECT {target} FROM {tableName} WHERE {where}={elem}")
						for row in cursor:
							print(row)
					except Exception:
						print(f"Ошибка запроса\nSELECT {target} FROM {tableName} WHERE {where}={elem}")
	'''
	(Не проверен!)
	Запрос на удаление строк, выбираемых
	согласно couple

	couple = {column:value}
	column - должна быть строкой
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
	Вытаскиваем на импорт все данные,
	или данные по одной таблице

	(пока по одной только)
	'''
	def exportData(self, fromTable=''):
		listOfValues=[]
		mainCur=self.select(fromTable, self.showTablesTemplates(fromTable))
		for row in mainCur:
			listOfValues.append(list(row))
		return(listOfValues)
	'''
	Экспортируем данные в таблицу(цы)
	'''
	def importData(self, listOfValues, tabelName, colNames=''):
		for row in listOfValues:
			print(f"INSERT INTO {tabelName} ({ ','.join(colNames) }) values ({self.rowToSqlString(row)})")
	'''
	Придется для этого метода переделать метод с шаблонами,
	дабы туда еще и типы переменных записывать
	'''
	def createTableFromTemplate(self, template):
		print(template)
		pass
