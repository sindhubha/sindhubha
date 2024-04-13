from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.user_rights_model import employeelistuser,menu_list,save_userrights,save_menumas
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.user_rights_model import employeelistuser,menu_list,save_userrights
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/employeelist_userrights/")
async def employeelist_userrights(employee_id:str=Form(''),
                                  is_login:str=Form(''),
                                  cnx: AsyncSession = Depends(get_db)):

    try: 
        if is_login == "" :
            return _getErrorResponseJson("is_login is required...")
        
        result = await employeelistuser(cnx,employee_id,is_login)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/user_menu_list/")
async def user_menu_list(employee_id:str=Form(''),
                         cnx: AsyncSession = Depends(get_db)):

    try: 
        if employee_id == "" :
            return _getErrorResponseJson("employee_id is required...")
        
        result = await menu_list(cnx,employee_id)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data1": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_userrights_detail/")
async def save_userrights_detail(employee_id:str=Form(''),
                                 menu:str=Form(''),
                                 cnx: AsyncSession = Depends(get_db)):
    try:
        if employee_id == "" :
            return _getErrorResponseJson("employee_id is required...")
        
        if menu == "" :
            return _getErrorResponseJson("menu is required...")
        
        if employee_id != "":          
            
            await save_userrights(cnx,employee_id, menu)
            return _getSuccessResponseJson("Saved Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_menu_mas_detail/")
async def save_menu_mas_detail(menu:str=Form(''),
                               cnx: AsyncSession = Depends(get_db)):

    
    try:
        if menu == '':
            return _getErrorResponseJson("menu is required")
    
        await save_menumas(cnx,menu)
        return _getSuccessResponseJson("Saved Successfully...")
    except Exception as e:
        return get_exception_response(e)