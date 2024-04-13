from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_plant_model import plant_Lists,getplantdtl,save_plant,update_plant,update_plantStatus,changestatus_plant,get_plant_name,change_posting_plant
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_plant_model import plant_Lists,getplantdtl,save_plant,update_plant,update_plantStatus,changestatus_plant,get_plant_name,change_posting_plant

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/plant_Lists/", tags=["Master Plant"])
async def plant_Lists_api(plant_id:str=Form(""),bu_id:str=Form(""),company_id:str=Form(""),for_android:str=Form(""),campus_id:str = Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await plant_Lists(cnx, plant_id,bu_id, company_id,campus_id)

        createFolder("Log/","Query executed successfully for plant list")
        if for_android == 'yes':
            response = [{
                "iserror": False,
                "message": "Data Returned Successfully.",
                "plant_id": plant_id,
                "plant_Lists": result
            }]
        else:
            response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "plant_id": plant_id,
            "plant_Lists": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_plant/", tags=["Master Plant"])
async def save_plant_api(plant_id:str=Form(""),plant_code:str=Form(""),plant_name:str=Form(""),company_name:str=Form(""),bu_name:str=Form(""),oracle_id:str=Form(""),ramco_id:str=Form(""),plant_address:str=Form(""),plant_pincode:str=Form(""),plant_state:str=Form(""),plant_country:str=Form(""),host_ip:str=Form(""),is_subcategory:str=Form(""),user_login_id:str=Form(""),campus_id:str=Form(""),cnx: AsyncSession = Depends(get_db)): 
    
    try:
        if plant_code == "" or plant_name == "":
            return _getErrorResponseJson("Fields Missing...")
        if plant_id == "":
            result = await getplantdtl(cnx, plant_id, plant_code, plant_name)
            if len(result)>0:
                return _getErrorResponseJson("Entry Already Exists...")
            
            await save_plant(cnx, plant_code, plant_name, company_name, bu_name, oracle_id, ramco_id, plant_address, plant_pincode, plant_state, plant_country, host_ip, is_subcategory, user_login_id,campus_id)
            createFolder("Log/","Query executed successfully for save plant")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_plant(cnx, plant_id, plant_code, plant_name, company_name, bu_name, oracle_id, ramco_id, plant_address, plant_pincode, plant_state, plant_country, host_ip, is_subcategory, user_login_id,campus_id)
            createFolder("Log/","Query executed successfully for update plant")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_plant/", tags=["Master Plant"])
async def remove_plant_api(plant_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if plant_id == "":
        return _getErrorResponseJson("plant id is required")
    
    try:

        await update_plantStatus(cnx, plant_id)
        createFolder("Log/","Query executed successfully for remove plant ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/changestatus_plant/", tags=["Master Plant"])
async def changestatus_plant_api(plant_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if plant_id == "":
       return _getErrorResponseJson("plant id is required")
    
    try:

        await changestatus_plant(cnx, plant_id,active_status)
        createFolder("Log/","Query executed successfully for change plant status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)

@router.post("/get_plant_name/", tags=["Master Plant"])
async def get_plant_name_api(bu_id:str=Form(""),company_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_plant_name(cnx, bu_id, company_id)
        createFolder("Log/","Query executed successfully for get plant ")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/change_posting_plant/", tags=["Master Plant"])
async def change_posting_plant_api(plant_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if plant_id == "":
        return _getErrorResponseJson("plant id is required")
    
    try:

        await change_posting_plant(cnx, plant_id, active_status)
        createFolder("Log/","Query executed successfully for change plant posting status ")
        return _getSuccessResponseJson("Posting Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
