class dataTable:
    def __init__(self, name='', data=[]):
        self.tableName = name
        self.tableData = self.__data_to_str__(data)
        self.tableSize = {"rows": len(self.tableData), "cols": len(self.tableData)}
    def __data_to_str__(self, data):
        for i in range(len(data)):
            for j in range(len(data[i])):
                data[i][j]=str(data[i][j])
        return(data)
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
    def getData (self):
        try:
            return(self.tableData)
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
    def colToStr(self, pos):
        for i in range(len(self.tableData)):
            self.tableData[i][pos] = f"'{self.tableData[i][pos]}'"
    def removeCol(self, pos):
        for i in range(len(self.tableData)):
            del self.tableData[i][pos]