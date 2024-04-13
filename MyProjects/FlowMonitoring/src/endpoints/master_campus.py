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
        from src.models.mysql.master_campus_model import campus_Lists,save_campus,update_campus,update_campusStatus,getcampusdtl
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/get_campus_list/",tags=["Master Campus"])
async def campus_list(campus_id:int = Form(''),                         
                      cnx:AsyncSession = Depends(get_db)):
    try:

        result = await campus_Lists(cnx,campus_id)

        createFolder("Log/","Query executed successfully for plant meter_campus list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_campus/",tags=["Master Campus"])
async def savecampus(campus_id: int = Form(""),                               
                     campus_code : str = Form(""),
                     campus_name:str= Form(""),
                     demand_meter_limit:str= Form(""),
                     user_login_id : str = Form(""),                             
                     cnx: AsyncSession = Depends(get_db)):
  
    try:
                
        if campus_code == "":
            return _getErrorResponseJson(" Campus Code is required")
                
        if campus_name == "":
            return _getErrorResponseJson(" Campus Name is required")
                
        if campus_id == '':
            result = await getcampusdtl(cnx, campus_code)
            if len(result)>0:
                return _getErrorResponseJson("Given Campus Code is Already Exist...")
            
            await save_campus(cnx, campus_code,campus_name,demand_meter_limit,user_login_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_campus(cnx,campus_id,campus_code,campus_name,demand_meter_limit,user_login_id)
            createFolder("Log/","Query executed successfully for update plant meter_campus")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_campus/",tags=["Master Campus"])
async def remove_campus(campus_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if campus_id == "":
        return _getErrorResponseJson("Campus Id is required")
    
    try:

        await update_campusStatus(cnx, campus_id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
        
    except Exception as e:
        return get_exception_response(e)
    
