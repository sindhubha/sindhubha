from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_business_unit_model import business_unit_Lists,getbudtl,savebusiness_unit,updatebusiness_unit,updatebusiness_unitStatus,changestatus_business_unit,get_branch_name
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_business_unit_model import business_unit_Lists,getbudtl,savebusiness_unit,updatebusiness_unit,updatebusiness_unitStatus,changestatus_business_unit,get_branch_name

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/business_unit_Lists/", tags=["Master Business Unit"])
async def business_unit_Lists_api(bu_id:str=Form(""),company_id:str=Form(""),for_android:str=Form(""),campus_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await business_unit_Lists(cnx, bu_id,company_id,campus_id)

        createFolder("Log/","Query executed successfully for bu list")
        if for_android == 'yes':
            response = [{
                "iserror": False,
                "message": "Data Returned Successfully.",
                "bu_id": bu_id,
                "business_unit_Lists": result
            }]
        else:
            response = {
                "iserror": False,
                "message": "Data Returned Successfully.",
                "bu_id": bu_id,
                "business_unit_Lists": result
            }
        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/savebusiness_unit/", tags=["Master Business Unit"])
async def savebusiness_unit_api(bu_id:str=Form(""),bu_code:str=Form(""),bu_name:str=Form(""),company_name:str=Form(""),user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    try:
        if bu_code == "" or bu_name == "":
            return _getErrorResponseJson("Fields Missing...")
        if bu_id == "":
            result = await getbudtl(cnx, bu_id, bu_code, bu_name)
            if len(result)>0:
                return _getErrorResponseJson("Given Bu Code is Already Exists...")
            
            await savebusiness_unit(cnx, bu_code, bu_name, company_name, user_login_id)
            createFolder("Log/","Query executed successfully for save bu")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await updatebusiness_unit(cnx, bu_id, bu_code, bu_name, company_name, user_login_id)
            createFolder("Log/","Query executed successfully for update bu")
            return _getSuccessResponseJson("Updated Successfully...")
           
    except Exception as e:
        return get_exception_response(e)

@router.post("/removebusiness_unit/", tags=["Master Business Unit"])
async def removebusiness_unit_api(bu_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if bu_id == "":
        return _getErrorResponseJson("bu id is required")
    
    try:

        await updatebusiness_unitStatus(cnx, bu_id)
        createFolder("Log/","Query executed successfully for remove bu ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)

@router.post("/changestatus_business_unit/", tags=["Master Business Unit"])
async def changestatus_business_unit_api(bu_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if bu_id == "":
       return _getErrorResponseJson("bu id is required")
    
    try:

        await changestatus_business_unit(cnx, bu_id, active_status)
        createFolder("Log/","Query executed successfully for change bu status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_branch_name/", tags=["Master Business Unit"])
async def get_branch_name_api(company_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_branch_name(cnx, company_id)
        createFolder("Log/","Query executed successfully for get bu ")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)
