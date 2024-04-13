from fastapi import APIRouter
from fastapi import Form,Depends,File,UploadFile,Request
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
        from src.models.mysql.master_meter_factor_model import meter_factor_list,update_meter_factor
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/get_master_meter_factor/")
async def get_master_meter_factor(id :str = Form(''),
                                  plant_id :str = Form(''),
                                  meter_id :str = Form(''),
                                  cnx: AsyncSession = Depends(get_db)):

    try: 

        result = await meter_factor_list(cnx,id,plant_id,meter_id)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_master_meter_factor/")
async def update_master_meter_factor(request: Request,
                                    plant_id :str = Form(''),
                                     meter_id :str = Form(''),
                                     obj :str = Form(''),
                                     user_login_id :str = Form(''),
                                     cnx: AsyncSession = Depends(get_db)):
    try:
        if meter_id == "" :
            return _getErrorResponseJson("meter_id is required...")
        
        if plant_id == "" :
            return _getErrorResponseJson("plant_id is required...")
        
        if obj == "" :
            return _getErrorResponseJson("obj is required...")
        
        if user_login_id == "" :
            return _getErrorResponseJson("user_login_id is required...")
        
        await update_meter_factor(cnx,plant_id,meter_id,obj,user_login_id,request)
        return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
