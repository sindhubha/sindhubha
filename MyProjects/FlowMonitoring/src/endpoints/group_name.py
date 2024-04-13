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
        from src.models.mysql.group_name_model import group_namelist,save_groupname,update_groupname,get_groupname_dtl,update_groupname_status
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.group_name_model import group_namelist,save_groupname,update_groupname,get_groupname_dtl,update_groupname_status
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/group_name_list/")
async def group_name_list(cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await group_namelist(cnx)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_group_name/")
async def save_group_name(id :int = Form(""),
                          group_name :str = Form(""),
                          user_login_id : str = Form(""),
                          cnx: AsyncSession = Depends(get_db)):
  
    try:
        if group_name == "":
            return _getErrorResponseJson(" group_name is required")
        
        if user_login_id == '':
            return _getErrorResponseJson("user_login_id is required")     
        
        if id == '':
            result = await get_groupname_dtl(cnx, group_name)
            if len(result)>0:
                return _getErrorResponseJson("Entry Already Exists...")
            await save_groupname(cnx, group_name,user_login_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_groupname(cnx, id,group_name,user_login_id)
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_group_name/")
async def remove_group_name(id: int = Form(''),
                            status : str = Form(''),
                            cnx: AsyncSession = Depends(get_db)):
    if id == "":
        return _getErrorResponseJson("id is required")
    
    try:

        await update_groupname_status(cnx, id, status)
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
