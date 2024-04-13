from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_department_model import department_Lists,getdepartmentdtl,save_department,update_department,update_departmentStatus,changestatus_department,get_department_name
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_department_model import department_Lists,getdepartmentdtl,save_department,update_department,update_departmentStatus,changestatus_department,get_department_name

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/department_Lists/", tags=["Master Department"])
async def department_Lists_api(plant_department_id:str=Form(""),plant_id:str=Form(""),bu_id:str=Form(""),company_id:str=Form(""),campus_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await department_Lists(cnx, plant_department_id,plant_id, bu_id, company_id,campus_id)

        createFolder("Log/","Query executed successfully for plant department list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "plant_department_id": plant_department_id,
            "plant_Lists": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_department/", tags=["Master Department"])
async def save_department_api(department_id:str=Form(""),department_code:str=Form(""),department_name:str=Form(""),company_name:str=Form(""),bu_name:str=Form(""),plant_name:str=Form(""),user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
  
    try:
        if department_code == "" or department_name == "":
            return _getErrorResponseJson("Fields Missing...")
        if department_id == "":
            result = await getdepartmentdtl(cnx, department_id, department_code, department_name)
            if len(result)>0:
               return _getErrorResponseJson("Given Department Code Is Already Exists...")
            
            await save_department(cnx, department_code, department_name, company_name, bu_name, plant_name, user_login_id)
            createFolder("Log/","Query executed successfully for save plant department")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_department(cnx, department_id, department_code, department_name, user_login_id)
            createFolder("Log/","Query executed successfully for update plant department")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_department/", tags=["Master Department"])
async def remove_department_api(department_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if department_id == "":
        return _getErrorResponseJson("Department ID is Required")
    
    try:

        await update_departmentStatus(cnx, department_id)
        createFolder("Log/","Query executed successfully for remove plant department ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/changestatus_department/", tags=["Master Department"])
async def changestatus_department_api(department_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if department_id == "":
       return _getErrorResponseJson("Department ID is Required")
    
    try:

        await changestatus_department(cnx, department_id, active_status)
        createFolder("Log/","Query executed successfully for change department status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_department_name/", tags=["Master Department"])
async def get_department_name_api(plant_id:str=Form(""),bu_id:str=Form(""),company_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_department_name(cnx, plant_id, bu_id, company_id)
        createFolder("Log/","Query executed successfully for get plant department name")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)
    
