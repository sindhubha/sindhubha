from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from pathlib import Path
from fastapi.requests import Request 
from fastapi import FastAPI, Depends, Form, File, UploadFile,Body
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))


# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_holiday_type_model import holiday_type_list,save_holidaytype,update_holidaytype,update_holidaytype_status,getholidaytypedtl
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.master_holiday_type_model import holiday_type_list,save_holidaytype,update_holidaytype,update_holidaytype_status
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/get_holiday_type_list/{id}", tags=["Master Holiday Type"])   
@router.post("/get_holiday_type_list/", tags=["Master Holiday Type"])
async def get_holiday_type_list(id: int = '', 
                                cnx: AsyncSession = Depends(get_db)):
    try:

        result = await holiday_type_list(cnx,id)

        createFolder("Log/","Query executed successfully for plant meter_group list")
        
        response = {
            "iserror": False,
            "message": "data returned successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_holiday_type/", tags=["Master Holiday Type"])
async def save_holiday_type(id:int =Form(''),
                            holiday_type:str=Form(''),
                            user_login_id:str=Form(None),
                            cnx:AsyncSession=Depends(get_db)): 
  
    try:
        
        if holiday_type == "":
            return _getErrorResponseJson(" Holiday Type is Required")
                
        if user_login_id == "":
            return _getErrorResponseJson(" user_login_id is required")   
                
        if id == '':
            result = await getholidaytypedtl(cnx,holiday_type)   
            if len(result)>0:
               return _getErrorResponseJson("Entry Already Exists...") 
            
            await save_holidaytype(cnx, holiday_type,user_login_id)
            createFolder("Log/","Query executed successfully for save plant meter_group")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_holidaytype(cnx,id,holiday_type,user_login_id)
            createFolder("Log/","Query executed successfully for update plant meter_group")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_holiday_type/", tags=["Master Holiday Type"])
async def remove_holiday_type(id: int = Form(''), 
                              status : str = Form(''),
                              cnx: AsyncSession = Depends(get_db)): 
    if id == "":
        return _getErrorResponseJson("ID is Required")
    
    try:

        await update_holidaytype_status(cnx, id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
        
    except Exception as e:
        return get_exception_response(e)
    
