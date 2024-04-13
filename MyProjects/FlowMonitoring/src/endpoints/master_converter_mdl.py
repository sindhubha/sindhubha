from fastapi import APIRouter
from fastapi import Form,Depends
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
        from src.models.mysql.master_converter_model_model import model_lists,getmodeldtl,save_model,update_model,update_modelstatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.model_model import model_lists,getmodeldtl,save_model,update_model,update_modelstatus
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/converter_model/", tags=["Master Converter Model"])
async def model_list(converter_make_id : int = Form(''),
                     converter_model_id : int = Form(''),
                     cnx: AsyncSession = Depends(get_db)):

    try: 
    
        result = await model_lists(converter_make_id,converter_model_id,cnx)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_converter_model/", tags=["Master Converter Model"])
async def savemodel(converter_make_id : int = Form(''),
                    converter_model_id :int =Form(''),
                    converter_model_name : str = Form(''),
                    user_login_id : str = Form(''),
                    cnx: AsyncSession = Depends(get_db)):
    try:
        if converter_model_name == "" :
            return _getErrorResponseJson("converter_model_name is required...")
        
        if user_login_id == "" :
            return _getErrorResponseJson("user_login_id is required...")
        
        if converter_make_id == "" :
            return _getErrorResponseJson("converter_make_id is required...")
        
        if converter_model_id == "":
            result = await getmodeldtl(cnx,converter_model_name)
            if len(result)>0:
               return _getErrorResponseJson("Given Converter Model Name is Already Exists...")
            
            await save_model(cnx,converter_make_id,converter_model_name, user_login_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_model(cnx,converter_make_id,converter_model_id,converter_model_name, user_login_id)
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_converter_model/", tags=["Master Converter Model"])
async def remove_model(converter_model_id :int =Form(''),
                       status : str = Form(''),
                       cnx: AsyncSession = Depends(get_db)):
    
    if converter_model_id == "":
        return _getErrorResponseJson("model id is required")
    
    try:

        await update_modelstatus(cnx, converter_model_id, status)
        if status !='':
            return _getSuccessResponseJson("status updated successfully.")
        else:
            return _getSuccessResponseJson("deleted successfully.")

    except Exception as e:
        return get_exception_response(e)
    
