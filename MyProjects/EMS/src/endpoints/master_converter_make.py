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
        from src.models.mysql.master_converter_make_model import converter_make_lists,getmodelmakedtl,save_modelmake,update_converter_make,update_converter_makestatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.model_model import converter_make_lists,getmodelmakedtl,save_modelmake,update_converter_make,update_converter_makestatus
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/get_converter_make_list/", tags=["Master Converter Make"])
async def converter_make_list(campus_id : str = Form(''),
                              converter_make_id : int = Form(''),
                              cnx: AsyncSession = Depends(get_db)):

    try: 
        result = await converter_make_lists(campus_id,converter_make_id,cnx)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)

@router.post("/save_converter_make/", tags=["Master Converter Make"])
async def save_converter_make(converter_make_id : int = Form(''),
                              converter_make_name : str = Form(''),
                              user_login_id : str = Form(''),
                              cnx: AsyncSession = Depends(get_db)):
    try:
        if converter_make_name == "" :
            return _getErrorResponseJson("converter_make_name is required...")
        
        if user_login_id == "" :
            return _getErrorResponseJson("user_login_id is required...")
        
        if converter_make_id == "":
            result = await getmodelmakedtl(cnx,converter_make_name)
            if len(result)>0:
               return _getErrorResponseJson("Given Converter Make Name is Already Exists...")
            
            await save_modelmake(cnx,converter_make_name, user_login_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_converter_make(cnx,converter_make_id,converter_make_name, user_login_id)
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_converter_make/", tags=["Master Converter Make"])
async def remove_converter_make(converter_make_id :int =Form(''),
                            status : str = Form(''),
                            cnx: AsyncSession = Depends(get_db)):
    
    if converter_make_id == "":
        return _getErrorResponseJson("converter_make_id is required")
    
    try:

        await update_converter_makestatus(cnx, converter_make_id, status)
        if status !='':
            return _getSuccessResponseJson("status updated successfully.")
        else:
            return _getSuccessResponseJson("deleted successfully.")

    except Exception as e:
        return get_exception_response(e)
    
