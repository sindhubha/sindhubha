from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_shift_model import shift_Lists,getshiftdtl,save_shift,update_shift,remove_shift,changestatus_shift
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_shift_model import shift_Lists,getshiftdtl,save_shift,update_shift,remove_shift,changestatus_shift

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")


router = APIRouter()

@router.post("/shift_Lists/", tags=["Master Shift"])
async def shift_Lists_api(shift_id:str=Form(""),plant_id:str=Form(""),bu_id:str=Form(""),company_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await shift_Lists(cnx, shift_id,plant_id, bu_id, company_id)

        createFolder("Log/","Query executed successfully for shift list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "shift_id": shift_id,
            "shift_Lists": result
        }

        return response
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_shift/", tags=["Master Shift"])
async def save_shift_api(shift_id:str=Form(""),plant_name:str=Form(""),no_of_shifts:str=Form(""),company_name:str=Form(""),bu_name:str=Form(""),a_shift_start_time:str=Form(""),b_shift_start_time:str=Form(""),c_shift_start_time:str=Form(""),a_shift_end_time:str=Form(""),b_shift_end_time:str=Form(""),c_shift_end_time:str=Form(""),user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:
        if plant_name == "" or no_of_shifts == "":
            return _getErrorResponseJson("Fields Missing...")
        
        if no_of_shifts == 1 and (a_shift_start_time == ''):
            return _getErrorResponseJson("Fields Missing...")        
        
        if no_of_shifts == 2 and (a_shift_start_time == '' or b_shift_start_time == '') :
            return _getErrorResponseJson("Fields Missing...")
        
        if no_of_shifts == 3 and (a_shift_start_time == '' or b_shift_start_time == '' or c_shift_start_time == ''):
            return _getErrorResponseJson("Fields Missing...")
        
        if shift_id == "":
            result = await getshiftdtl(cnx, shift_id, plant_name, company_name, bu_name)
            if len(result)>0:
                return _getErrorResponseJson("Entry Already Exists...")
            
            await save_shift(cnx, plant_name, no_of_shifts, company_name, bu_name, a_shift_start_time, b_shift_start_time, c_shift_start_time, a_shift_end_time, b_shift_end_time, c_shift_end_time, user_login_id)
            createFolder("Log/","Query executed successfully for save shift")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_shift(cnx, shift_id, plant_name, no_of_shifts, company_name, bu_name, a_shift_start_time, b_shift_start_time, c_shift_start_time, a_shift_end_time, b_shift_end_time, c_shift_end_time, user_login_id)
            createFolder("Log/","Query executed successfully for update shift")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_shift/", tags=["Master Shift"])
async def remove_shift_api(shift_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if shift_id == "":
        return _getErrorResponseJson("shift id is required")
    
    try:

        await remove_shift(cnx, shift_id)
        createFolder("Log/","Query executed successfully for remove shift ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
    
@router.post("/changestatus_shift/", tags=["Master Shift"])
async def changestatus_shift_api(shift_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if shift_id == "":
       return _getErrorResponseJson("shift id is required")
    
    try:

        await changestatus_shift(cnx, shift_id, active_status)
        createFolder("Log/","Query executed successfully for change shift status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)