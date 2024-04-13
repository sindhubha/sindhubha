from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from pathlib import Path
from fastapi.requests import Request 
from fastapi import FastAPI, Depends, Form, File, UploadFile,Body
import os
import json
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))


# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_polling_data_model import pollingtime_list,polling_timeentry
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/polling_time_list/",tags=["Master Polling Time"])
async def polling_time_list(meter_id: int=Form(''),   
                            campus_id: str=Form(''),                 
                            company_id: str=Form(''),                 
                            bu_id: str=Form(''),                 
                            plant_id: str=Form(''),                                
                            cnx: AsyncSession = Depends(get_db)):
    try:
        result = await pollingtime_list(cnx,meter_id,campus_id,company_id,bu_id,plant_id)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
  
@router.post("/polling_time_entry/",tags=["Master Polling Time"])
async def polling_timelist(meter_id: str=Form(''), 
                           meter_state_condition1 : int = Form(''),               
                           meter_state_condition2 : int = Form(''),               
                           meter_state_condition3 : int = Form(''),               
                           meter_state_condition4 : int = Form(''),               
                           meter_state_condition5 : int = Form(''),               
                           meter_state_condition6 : int = Form(''),               
                           cnx: AsyncSession = Depends(get_db)):
    try:
        print(meter_id)
        # if meter_id == '':
        #     return JSONResponse({"iserror":True,"message":"meter_id is required"}) 
        await polling_timeentry(cnx,meter_id,meter_state_condition1,meter_state_condition2,meter_state_condition3,meter_state_condition4,meter_state_condition5,meter_state_condition6)
        return _getSuccessResponseJson("Save Data Successfully")
    
    except Exception as e:
        return get_exception_response(e)