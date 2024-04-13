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
        from src.models.mysql.report_fields_model import power_report_name,power_report_fields,power_reportfield,update_power_report
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.report_fields_model import power_report_name,power_report_fields,power_reportfield,update_power_report
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/get_power_report_name/")
async def get_power_report_name(cnx: AsyncSession = Depends(get_db)):
    try: 
        
        result = await power_report_name(cnx)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_power_report_fields/")
async def get_power_report_fields(plant_id : int = Form(''),
                                  report_id:int=Form(''),
                                  cnx: AsyncSession = Depends(get_db)):
    try: 
        if report_id == '':
             return _getErrorResponseJson("report_id is required") 
        
        if plant_id == '':
             return _getErrorResponseJson("plant_id is required") 
        
        if report_id == 0:
            result = await power_report_fields(cnx,plant_id)
        else:
            result = await power_reportfield(cnx,plant_id,report_id)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_power_report_fields/")
async def update_power_report_fields(plant_id : int = Form(''),
                                     report_id:int=Form(''),
                                     obj: str = Form(''),
                                     cnx: AsyncSession = Depends(get_db)):
 
  
    try:
        if plant_id == "" :
            return _getErrorResponseJson("plant_id is required...")
        
        if report_id == "" :
            return _getErrorResponseJson("report_id is required...")
        
        if obj == "" :
            return _getErrorResponseJson("obj is required...")
        
        await update_power_report(cnx,plant_id,report_id,obj)
        return _getSuccessResponseJson("data save successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
