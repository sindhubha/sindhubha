from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder
from log_file import createFolder
from typing import List, Dict
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.adminmodel import getGroupDetailsReport_code
    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
    
@router.post("/getGroupDetailsReport", tags=["Users"])
async def getReportFields_api(company_id:str=Form(""),bu_id:str=Form(""),plant_id:str=Form(""),department_id:str=Form(""),equipment_group_id:str=Form(""),equipment_class_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        result = await getGroupDetailsReport_code(cnx,company_id,bu_id,plant_id,department_id,equipment_group_id,equipment_class_id)
        response = { "iserror": False, "message": "Data Returned Successfully", "res":result}
        return response
    
    except Exception as e:
        return _getErrorResponseJson(str(e))
	