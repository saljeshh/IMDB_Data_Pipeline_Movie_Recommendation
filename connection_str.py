from sqlalchemy import create_engine

# Database connection
connection_string = 'mssql+pyodbc://sa:password@SALJESH/IMDB?driver=ODBC+Driver+17+for+SQL+Server'
engine = create_engine(connection_string)
