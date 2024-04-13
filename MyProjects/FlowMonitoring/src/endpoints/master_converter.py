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
        from src.models.mysql.master_converter_model import converter_Lists,getconverterdtl,save_converter,update_converter,update_converterStatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.master_converter_model import converter_Lists,getconverterdtl,save_converter,update_converter,update_converterStatus
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/get_converter_list/", tags=["Master Converter"])
async def converter_list(campus_id:str = Form(''),
                         converter_make_id:int = Form(''), 
                         converter_model_id:int = Form(''), 
                         converter_id:int = Form(''), 
                         is_limit_check : str =Form(''),                                           
                         cnx:AsyncSession = Depends(get_db)):
    try:
        print(converter_id)

        result = await converter_Lists(cnx,campus_id,converter_make_id,converter_model_id,converter_id,is_limit_check)

        createFolder("Log/","Query executed successfully for plant converter list")
    
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_converter_details/", tags=["Master Converter"])
async def save_converter_details(campus_id : str = Form(""),
                                 company_id:str = Form(''),                                 
                                 bu_id:str = Form(''),                                 
                                 plant_id:str = Form(''),                                 
                                 converter_id:int = Form(''),                                 
                                 converter_model_id:int = Form(''),                                 
                                 converter_name : str = Form(""),
                                 ip_address:str= Form(""),
                                 port_no:int= Form(""),
                                 meter_limit:int= Form(""),
                                 user_login_id : str = Form(""),                             
                                 mac : str = Form(""), 
                                 baud_rate : str =Form(''),                       
                                 parity : str =Form(''),                               
                                 cnx: AsyncSession = Depends(get_db)):
  
    try:
        
        if campus_id == "":
            return _getErrorResponseJson(" Campus ID is Required")
        
        if converter_name == "":
            return _getErrorResponseJson(" Converter Name is Required")
        
        if converter_model_id == "":
            return _getErrorResponseJson(" Converter Model is Required")
                
        if ip_address == "":
            return _getErrorResponseJson(" IP Address is Required")
                
        if port_no == "":
            return _getErrorResponseJson(" Port is Required")
                
        if meter_limit == "":
            return _getErrorResponseJson(" Meter Limit is Required")
                
        if converter_id == '':
            result = await getconverterdtl(cnx, converter_name)
            if len(result)>0:
                return _getErrorResponseJson("Given Converter Name Is Already Exists...")
                
            await save_converter(cnx,campus_id,converter_model_id,converter_name,ip_address,port_no,meter_limit,user_login_id,mac,company_id,bu_id,plant_id,baud_rate,parity)
            createFolder("Log/","Query executed successfully for save plant converter")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_converter(cnx,campus_id,converter_model_id,converter_id,converter_name,ip_address,port_no,meter_limit,user_login_id,mac,company_id,bu_id,plant_id,baud_rate,parity)
            createFolder("Log/","Query executed successfully for update plant converter")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_converter/", tags=["Master Converter"])
async def remove_converter(converter_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if converter_id == "":
        return _getErrorResponseJson("Converter Id is required")
    
    try:

        await update_converterStatus(cnx, converter_id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
