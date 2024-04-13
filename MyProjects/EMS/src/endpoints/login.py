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
        from src.models.mysql.login_model import loginformmodel,changepassword
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.login_model import loginformmodel,changepassword
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")


router = APIRouter()

@router.post("/login/", tags=["Login"])
async def employee_login(username:str=Form(''),password:str=Form(''),for_android:str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    try: 

        if username == "":
            return _getErrorResponseJson("Username is required")
        if password == "":
            return _getErrorResponseJson("Password is required")
        
        
        result = await loginformmodel(cnx, username, password)

        createFolder("Log/","Query executed successfully for login")

        if len(result) >0:   
            if for_android == 'yes':
                response = [{
                "iserror": False,
                "message": "Successfully login",
                "data": result 
            }]
            else:
                response = {
                    "iserror": False,
                    "message": "Successfully login",
                    "data": result 
                }

            return response
        else:
            if for_android == 'yes':
                response = [{
                "iserror": True,
                "message": "Invalid login",
            }]
            else:
                response = {
                    "iserror": True,
                    "message": "Invalid login",
                }
            return response
    
    except Exception as e:
        return get_exception_response(e)
    
    
@router.post("/change_password/", tags=["Login"])
async def change_password(employee_id:str=Form(''),
                          old_password:str=Form(''),
                          new_password:str=Form(''),
                          retype_password:str=Form(''),
                          cnx: AsyncSession = Depends(get_db)):
    try: 

        if employee_id == "":
            return _getErrorResponseJson("employee_id is required")
        
        if old_password == "":
            return _getErrorResponseJson("old_password is required")
        
        if new_password == "":
            return _getErrorResponseJson("new_password is required")
        
        if retype_password == "":
            return _getErrorResponseJson("retype_password is required")
        
        result = await changepassword(cnx,employee_id ,old_password,new_password ,retype_password)

        response = {
            "iserror": False,
            "message": "password changed successfully",
            "data": result 
        }
        
        return response
    
    except Exception as e:
        return get_exception_response(e)
    


    