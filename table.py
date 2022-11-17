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