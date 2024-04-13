from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_equipment_class_model import equipment_class_Lists,getequipmentclassdtl,saveequipment_class,updateequipment_class,updateequipment_classStatus,changestatus_equipment_class,get_equipment_class_name
from sqlalchemy.ext.asyncio import AsyncSession
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_equipment_class_model import equipment_class_Lists,getequipmentclassdtl,saveequipment_class,updateequipment_class,updateequipment_classStatus,changestatus_equipment_class,get_equipment_class_name

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/equipment_class_Lists/", tags=["Master Equipment Class"])
async def equipment_class_Lists_api(equipment_class_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await equipment_class_Lists(cnx, equipment_class_id)

        createFolder("Log/","Query executed successfully for equipment class list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "equipment_class_id": equipment_class_id,
            "equipment_class_Lists": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/saveequipment_class/", tags=["Master Equipment Class"])
async def saveequipment_class_api(equipment_class_id:str=Form(""),equipment_class_code:str=Form(""),equipment_class_name:str=Form(""),user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:
        if equipment_class_code == "" or equipment_class_name == "":
            return _getErrorResponseJson("Fields Missing...")
        if equipment_class_id == "":
            result = await getequipmentclassdtl(cnx, equipment_class_id, equipment_class_code, equipment_class_name)
            if len(result)>0:
                return _getErrorResponseJson("Entry Already Exists...")
            
            await saveequipment_class(cnx, equipment_class_code, equipment_class_name, user_login_id)
            createFolder("Log/","Query executed successfully for save equipment class")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await updateequipment_class(cnx, equipment_class_id, equipment_class_code, equipment_class_name, user_login_id)
            createFolder("Log/","Query executed successfully for update equipment class")
            return _getSuccessResponseJson("Updated Successfully...")
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_equipment_class/", tags=["Master Equipment Class"])
async def remove_equipment_class(equipment_class_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if equipment_class_id == "":
        return _getErrorResponseJson("equipment class id is required")
    
    try:

        await updateequipment_classStatus(cnx, equipment_class_id)
        createFolder("Log/","Query executed successfully for remove equipment class ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/changestatus_equipment_class/", tags=["Master Equipment Class"])
async def changestatus_equipment_class_api(equipment_class_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if equipment_class_id == "":
       return _getErrorResponseJson("equipment class id is required")
    
    try:

        await changestatus_equipment_class(cnx, equipment_class_id, active_status)
        createFolder("Log/","Query executed successfully for change equipment class status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_equipment_class_name/", tags=["Master Equipment Class"])
async def get_equipment_class_name_api(cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_equipment_class_name(cnx)
        createFolder("Log/","Query executed successfully for get equipment class name ")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)
    
