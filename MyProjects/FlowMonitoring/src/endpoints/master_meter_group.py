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
# createFolder("Log/","file_path"+str(file_path))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_meter_group_model import meter_group_Lists,save_meter_group,update_meter_group,update_meter_groupStatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.master_meter_group_model import meter_group_Lists,save_meter_group,update_meter_group,update_meter_groupStatus
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/get_meter_group_list/{meter_group_id}")
@router.post("/get_meter_group_list/")
async def meter_group_list(meter_group_id:int = '',                        
                           cnx:AsyncSession = Depends(get_db)):
    try:

        result = await meter_group_Lists(cnx,meter_group_id)

        createFolder("Log/","Query executed successfully for plant meter_group list")
        
        response = {
            "iserror": False,
            "message": "data returned successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_meter_group/")
async def save_metergroup(meter_group_id: int = Form(""),                                
                          meter_id : str = Form(""),
                          group_type:str= Form(""),
                          type_id:int= Form(""),
                          user_login_id : str = Form(""),                             
                          cnx: AsyncSession = Depends(get_db)):
  
    try:
        
        if meter_id == "":
            return _getErrorResponseJson(" meter_id is required")
                
        if group_type == "":
            return _getErrorResponseJson(" group_type is required")
                
        if type_id == "":
            return _getErrorResponseJson(" type_id is required")
                
        if meter_group_id == '':
            await save_meter_group(cnx, meter_id,group_type,type_id,user_login_id)
            return _getSuccessResponseJson("saved successfully...")
        else:
            await update_meter_group(cnx,meter_group_id,meter_id,group_type,type_id,user_login_id)
            createFolder("Log/","Query executed successfully for update plant meter_group")
            return _getSuccessResponseJson("updated successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_meter_group/")
async def remove_meter_group(meter_group_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if meter_group_id == "":
        return _getErrorResponseJson("meter_group id is required")
    
    try:
        await update_meter_groupStatus(cnx, meter_group_id, status)
        if status !='':
            return _getSuccessResponseJson("status updated successfully.")
        else:
            return _getSuccessResponseJson("deleted successfully.")
        
    except Exception as e:
        return get_exception_response(e)
    
