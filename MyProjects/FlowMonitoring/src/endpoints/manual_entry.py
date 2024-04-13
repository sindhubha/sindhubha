from fastapi import APIRouter
from fastapi import Form,Depends,Request
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from fastapi.requests import Request 
from sqlalchemy.ext.asyncio import AsyncSession
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.manual_entry_model import savemanual_entry,manualdata_correction
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.manual_entry_model import savemanual_entry
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/manual_entry/")
async def save_manual_entry(request: Request,
                            obj: str = Form(''),                                                  
                            user_login_id :str =Form(''),                          
                            plant_id :str =Form(''),                          
                            cnx: AsyncSession = Depends(get_db)):

    try: 
        if obj == "" :
            return _getErrorResponseJson("obj is required...")
        
        if plant_id == "":
            return _getErrorResponseJson("plant_id is required...")
        
        await savemanual_entry(cnx,obj,user_login_id,plant_id,request)
        response = {
            "iserror": False,
            "message": "Data save Successfully.",
            "data": ''
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)

@router.post("/manual_data_correction/")
async def manual_data_correction(mill_date: str = Form(''),                                                  
                                 mill_shift :str =Form(''),                          
                                 plant_id :str =Form(''),                          
                                 cnx: AsyncSession = Depends(get_db)):

    try: 
        if mill_date == "" :
            return _getErrorResponseJson("Date is required...")
        
        if mill_shift == "" :
            return _getErrorResponseJson("Shift is required...")
        
        if plant_id == "" :
            return _getErrorResponseJson("Plant is required...")
    
        await manualdata_correction(cnx,mill_date,mill_shift,plant_id)
        response = {
            "iserror": False,
            "message": "Data Corrected Successfully.",
            "data": ''
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)