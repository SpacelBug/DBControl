# DBControl
It`s my own python module with class for different DB (MySQL, PostgreSQL)
## Eng.
### Requirements.
- PyMySQL
- Psycopg2
### Methods and funtions:

List of methods from dataBase class:

- createTemplatesForTables
- showTablesTemplates
- select
- update
- insert
- delete
- join
- importData

List of methods from dataTable class:

- showData
- getRow
- getData
- showRow
- conColumns
- colToStr

List of functions:

- toTimestamp (table, pos)
  Change all values (///) in targeted column  
   
## Rus.
### Требования.
- PyMySQL
- Psycopg2
### Методы и функции:

Список методов класса dataBase:

- createTemplatesForTables
- showTablesTemplates
- select
- update
- insert
- delete
- join
- importData

Список методов класса dataTable:

- showData
- getRow
- getData
- showRow
- conColumns
- colToStr

Список функций:

- toTimestamp (table, pos)
  Меняет все значения (///) в указанной колонке  
