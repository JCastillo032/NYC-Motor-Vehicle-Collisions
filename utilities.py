import pyodbc as db
import sqlalchemy

# class utilities:
def getConnString(pSvr, pDB, pType):
    # sqlalchemy
    if pType =='write':
        cnStr = (r'mssql+pyodbc://@' + pSvr + '/' +pDB + '?'
            r'trusted_connection=yes&'
            r'driver=ODBC+Driver+13+for+SQL+Server')
        sQLAction = sqlalchemy.create_engine(cnStr)
    # pyodbc
    elif pType == 'read':
        cnStr = (r'Driver={SQL Server};'
            r'Server=' + pSvr + ';'
            r'Database=' + pDB + ';'
            r'Trusted_Connection=yes;')
        sQLAction = db.connect(cnStr,autocommit=True)
    return sQLAction