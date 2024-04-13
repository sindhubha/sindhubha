from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_processtype_model import processtype_Lists,getprocesstypedtl,saveprocesstype,updateprocesstype,updateprocesstypeStatus,changestatus_processtype,get_processtype_name
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_processtype_model import processtype_Lists,getprocesstypedtl,saveprocesstype,updateprocesstype,updateprocesstypeStatus,changestatus_processtype,get_processtype_name

    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/processtype_Lists/", tags=["Master Processtype"])
async def processtype_Lists_api(processtype_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await processtype_Lists(cnx, processtype_id)

        createFolder("Log/","Query executed successfully for equipment class list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "processtype_id": processtype_id,
            "processtype_Lists": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/saveprocesstype/", tags=["Master Processtype"])
async def saveprocesstype_api(processtype_id:str=Form(""),processtype_code:str=Form(""),processtype_name:str=Form(""),user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:
        if processtype_code == "" or processtype_name == "":
            return _getErrorResponseJson("Fields Missing...")
        if processtype_id == "":
            result = await getprocesstypedtl(cnx, processtype_id, processtype_code, processtype_name)
            if len(result)>0:
                return _getErrorResponseJson("Entry Already Exists...")
            
            await saveprocesstype(cnx, processtype_code, processtype_name, user_login_id)
            createFolder("Log/","Query executed successfully for save process type")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await updateprocesstype(cnx, processtype_id, processtype_code, processtype_name, user_login_id)
            createFolder("Log/","Query executed successfully for update process type")
            return _getSuccessResponseJson("Updated Successfully...")
    
    except Exception as e:
        return get_exception_response(e)

@router.post("/remove_processtype/", tags=["Master Processtype"])
async def remove_processtype_api(processtype_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if processtype_id == "":
        return _getErrorResponseJson("process type id is required")
    
    try:

        await updateprocesstypeStatus(cnx, processtype_id)
        createFolder("Log/","Query executed successfully for remove process type")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)

@router.post("/changestatus_processtype/", tags=["Master Processtype"])
async def changestatus_processtype_api(processtype_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if processtype_id == "":
       return _getErrorResponseJson("process type id is required")
    
    try:

        await changestatus_processtype(cnx, processtype_id, active_status)
        createFolder("Log/","Query executed successfully for change process type status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_processtype_name/", tags=["Master Processtype"])
async def get_processtype_name_api(cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_processtype_name(cnx)
        createFolder("Log/","Query executed successfully for get process type name ")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)
    