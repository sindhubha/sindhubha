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
        from src.models.mysql.master_function_model import function_Lists,getfunctiondtl,save_function,update_function,update_functionStatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

@router.post("/get_function_list/{function_id}", tags=["Master Function"])
@router.post("/get_function_list/", tags=["Master Function"])
async def function_list(request:Request,
                        function_id:int = '',
                        function_type : str = Form(''),
                        cnx:AsyncSession = Depends(get_db)):
    try: 
        base_url = request.url._url
        base_path = base_url.split("/")
        base_path.pop()
        base_path.pop()
        base_path = "/".join(base_path)+"/"

        result = await function_Lists(cnx, function_type,function_id,base_path)

        createFolder("Log/","Query executed successfully for plant function list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_function_details/", tags=["Master Function"])
async def save_function_details(function_id: int = Form(""),
                                function_code:str = Form(""),
                                function_name : str = Form(""),
                                function_type:str= Form(""),
                                image: UploadFile = File(''),
                                old_image : str = Form(''),
                                user_login_id : str = Form(""),                             
                                cnx: Session = Depends(get_db)):
  
    try:
        if function_code == "":
            return _getErrorResponseJson(" function_code is required")
        
        if function_name == "":
            return _getErrorResponseJson(" function_name is required")
                
        if function_id == '':
            result = await getfunctiondtl(cnx, function_id, function_code)

            if len(result)>0:
                return _getErrorResponseJson("Entry Already Exists...")
                
            await save_function(cnx, function_code,function_name,function_type,image,old_image,user_login_id,static_dir)
            
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_function(cnx,function_id, function_code,function_name,function_type,image,old_image,user_login_id,static_dir)
            createFolder("Log/","Query executed successfully for update plant function")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_function/", tags=["Master Function"])
async def remove_function(function_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if function_id == "":
        return _getErrorResponseJson("function id is required")
    
    try:

        await update_functionStatus(cnx, function_id, status)
        if status !='':
            return _getSuccessResponseJson("status updated successfully.")
        else:
            return _getSuccessResponseJson("deleted successfully.")

    except Exception as e:
        return get_exception_response(e)
    
