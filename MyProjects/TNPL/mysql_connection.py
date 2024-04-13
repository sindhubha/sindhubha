from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, text
from sqlalchemy.pool import QueuePool


engine = create_engine("mysql+pymysql://root@localhost:3306/ems_v1",pool_pre_ping=True,poolclass=QueuePool)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
   
    try:
        db = SessionLocal()
        yield db
    finally:
        db.close()

        