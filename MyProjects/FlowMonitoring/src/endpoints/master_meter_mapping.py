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
        from src.models.mysql.master_meter_mapping_model import meter_mapping_list,save_metermapping,update_metermapping,update_metermappingStatus,check_metermappingdtl
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"
   
@router.post("/get_meter_mapping_list/", tags=["Master Meter Mapping"])
async def get_meter_mapping_list(id: int = Form(''),
                                 campus_id : str = Form(''),
                                 plant_id : str = Form(''),
                                 equipment_id : int = Form(''),
                                 cnx: AsyncSession = Depends(get_db)):
    try:

        result = await meter_mapping_list(cnx,id,campus_id,plant_id,equipment_id)

        createFolder("Log/","Query executed successfully for plant save_meter_mapping list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)

@router.post("/save_meter_mapping/", tags=["Master Meter Mapping"])
async def save_meter_mapping(id : int = Form(''),
                             meter : str = Form(''),
                             company_id : str = Form(''),
                             bu_id : str = Form(''),
                             plant_id : str = Form(''),
                             plant_department_id : str = Form(''),
                             parameter : str = Form(''),
                             user_login_id : int = Form(''),
                             cnx:AsyncSession=Depends(get_db)): 
  
    try:
        createFolder("Log/",str(meter))  
        if meter == "":
            return _getErrorResponseJson(" meter is required")   
        
        if company_id == "":
            return _getErrorResponseJson(" company_id is required")   
        if bu_id == "":
            return _getErrorResponseJson(" bu_id is required")   
        if plant_id == "":
            return _getErrorResponseJson(" plant_id is required")   
        if plant_department_id == "":
            return _getErrorResponseJson(" plant_department_id is required")   
        
        if user_login_id == "":
            return _getErrorResponseJson(" user_login_id is required")   

        if id == '':  
            results = await check_metermappingdtl(cnx,meter,id)
            createFolder("Log/",f"results{results}")
            if len(results)>0:
                return _getErrorResponseJson("Given Equipment Is Already Exists...")
            await save_metermapping(cnx,company_id,bu_id,plant_id,plant_department_id, parameter,meter,user_login_id)
            createFolder("Log/","Query executed successfully for save plant save_meter_mapping")
            return _getSuccessResponseJson("saved successfully...")
        else :
            await update_metermapping(cnx,id,company_id,bu_id,plant_id,plant_department_id,parameter,meter,user_login_id)
            createFolder("Log/","Query executed successfully for update plant save_meter_mapping")
            return _getSuccessResponseJson("updated successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_metermapping_status/", tags=["Master Meter Mapping"])
async def update_metermapping_status(id: str = Form(''),     
                                     status : str = Form(''),                        
                                     cnx: AsyncSession = Depends(get_db)):
    
    try:
        await update_metermappingStatus(cnx,id,status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
    except Exception as e:
        return get_exception_response(e)