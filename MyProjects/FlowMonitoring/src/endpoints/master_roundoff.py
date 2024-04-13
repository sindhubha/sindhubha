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
        from src.models.mysql.master_parameter_roundoff_model import parameter_roundoff_list,update_parameterroundoff
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.master_machine_factor_model import machine_factor_list,update_machine_factor
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/get_parameter_roundoff/")
async def get_parameter_roundoff(plant_id :int = Form(""),
                                 cnx: AsyncSession = Depends(get_db)): 

    try: 

        result = await parameter_roundoff_list(cnx,plant_id)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_parameter_roundoff/")
async def update_parameter_roundoff(plant_id :str = Form(''),
                                    obj :str = Form(''),
                                    cnx: AsyncSession = Depends(get_db)):
    try:
        if plant_id == "" :
            return _getErrorResponseJson("plant_id is required...")
        
        if obj == "" :
            return _getErrorResponseJson("obj is required...")
        
        await update_parameterroundoff(cnx,plant_id,obj)
        return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
