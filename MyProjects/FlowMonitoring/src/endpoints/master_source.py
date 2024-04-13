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
        from src.models.mysql.master_source_model import source_Lists,save_source,update_source,update_sourceStatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/get_source_list/{source_id}",tags=["Master Source"])
@router.post("/get_source_list/",tags=["Master Source"])
async def source_list(source_id:int = '',                        
                      cnx:AsyncSession = Depends(get_db)):
    try:

        result = await source_Lists(cnx,source_id)

        createFolder("Log/","Query executed successfully for plant meter_source list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_source/",tags=["Master Source"])
async def savesource(source_id: int = Form(""),                                
                     source_name:str= Form(""),
                     user_login_id : str = Form(""),                             
                     cnx: AsyncSession = Depends(get_db)):
  
    try:
                
        if source_name == "":
            return _getErrorResponseJson(" Source Name is required")
                
        if source_id == '':
            await save_source(cnx, source_name,user_login_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_source(cnx,source_id,source_name,user_login_id)
            createFolder("Log/","Query executed successfully for update plant meter_source")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_source/",tags=["Master Source"])
async def remove_source(source_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if source_id == "":
        return _getErrorResponseJson("Source Id is required")
    
    try:

        await update_sourceStatus(cnx, source_id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
        
    except Exception as e:
        return get_exception_response(e)
    
