from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from sqlalchemy.ext.asyncio import AsyncSession
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.order_wise_model import orderwise
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.order_wise_model import orderwise
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/order_wise/")
async def order_wise(table_name :str = Form(''),
                     obj :str = Form(''),
                     cnx: AsyncSession = Depends(get_db)):

    try: 
        if table_name == "" :
            return _getErrorResponseJson("table_name is required...")
        
        if obj == "" :
            return _getErrorResponseJson("obj is required...")
        
        result = await orderwise(cnx,table_name,obj)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    