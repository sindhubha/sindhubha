from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from fastapi import FastAPI
import pyodbc
# DESKTOP-92NNMNK
#192.168.95.10 --tnpl
#10.0.15.10 --spb
# DESKTOP-S7A826U\SQLEXPRESS --roots
# LAPTOP-USGH7G0M\MSSQLSERVER2022 
driver = 'ODBC Driver 17 for SQL Server'
server_name = '10.0.15.10'
database_name = 'ems_v1_completed'
user_name = 'sa'     
password = 'admin@2023'

conn_str = (
    f'DRIVER={driver};'
    f'SERVER={server_name};'
    f'DATABASE={database_name};'
    f'UID={user_name};'
    f'PWD={password}'
)  

# SQLAlchemy engine pool_pre_ping=True,poolclass=QueuePool
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={conn_str}", pool_pre_ping=True)

# FastAPI instance
app = FastAPI()

# Session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Dependency to get a database session
def get_db():
    db = None
    try:
        db = SessionLocal()
        yield db
    finally:
        if db:
            db.close()


