import pyodbc
import sqlalchemy
from urllib.parse import quote_plus
import pandas as pd

class TableWriter:

    def __init__(self, credentials: dict):

        connStr = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=tcp:bookr.database.windows.net,1433;"
            "DATABASE=bookrDev;"
            f"UID=bennett.lundberg;"
            f"PWD=Valleyforge0!;"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )

        self.conn = pyodbc.connect(connStr)
        self.crsr = self.conn.cursor()
        odbc_str = quote_plus(connStr)
        self.bulkConnection = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % odbc_str)
        self.engine = self.bulkConnection

    def run(self, qry: str):

        self.crsr.execute(qry)
        self.conn.commit()

    
    def write(self, pdTable: pd.DataFrame, sqlTable: str):
        pdTable.to_sql(sqlTable, con = self.engine, if_exists = "replace", index = False)

class TableReader:
        
    def __init__(self, credentials: dict):

        connStr = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            "SERVER=tcp:bookr.database.windows.net,1433;"
            "DATABASE=bookrDev;"
            f"UID=bennett.lundberg;"
            f"PWD=Valleyforge0!;"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
            "Connection Timeout=30;"
        )

        self.conn = pyodbc.connect(connStr)
        self.crsr = self.conn.cursor()

    def read(self, qry: str):

        dat = pd.read_sql(qry, con = self.conn)
        return dat