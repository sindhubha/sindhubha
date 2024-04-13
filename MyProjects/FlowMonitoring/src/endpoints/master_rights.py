from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_rights_model import report_name_list,report_fields_list,update_reportfields
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.master_rights_model import report_name_list,report_fields_list,update_reportfields
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/get_report_name_list/", tags=["Master Rights"])
async def get_report_name_list(cnx: AsyncSession = Depends(get_db)):
    try: 
        
        result = await report_name_list(cnx)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_report_fields_list/", tags=["Master Rights"])
async def get_report_fields_list(report_id :int = Form(''),
                                 cnx: AsyncSession = Depends(get_db)):
    try: 
        if report_id == '':
             return _getErrorResponseJson("report_id is required") 
        
        
        result = await report_fields_list(cnx,report_id)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_report_fields/", tags=["Master Rights"])
async def update_report_fields(obj:str = Form(''),
                               cnx: AsyncSession = Depends(get_db)):
 
  
    try:
        if obj == "" :
            return _getErrorResponseJson("obj is required...")
        
        await update_reportfields(cnx,obj)
        return _getSuccessResponseJson("data save successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
