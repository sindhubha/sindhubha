from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_equipment_model import equipment_Lists,getequipmentdtl,gettabletip,getiotplcip,save_equipment,update_equipment,update_equipmentStatus,changestatus_equipment,get_equipment_name,equipment_interlock_update,equipment_hb_ht_mt_update,equipment_linespeed_Lists,equipment_is_configured_update,equipment_linespeed_update
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_equipment_model import equipment_Lists,getequipmentdtl,gettabletip,getiotplcip,save_equipment,update_equipment,update_equipmentStatus,changestatus_equipment,get_equipment_name,equipment_interlock_update,equipment_hb_ht_mt_update,equipment_linespeed_Lists,equipment_is_configured_update,equipment_linespeed_update

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/equipment_Lists/", tags=["Master Equipment"])
async def equipment_Lists_api(equipment_id:str=Form(""),bu_id:str=Form(""),company_id:str=Form(""),plant_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await equipment_Lists(cnx, equipment_id,bu_id, company_id, plant_id)

        createFolder("Log/","Query executed successfully for equipment list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "equipment_id": equipment_id,
            "equipment_Lists": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_equipment/", tags=["Master Equipment"])
async def save_equipment_api(equipment_id:str=Form(""),equipment_code:str=Form(""),equipment_name:str=Form(""),company_name:str=Form(""),bu_name:str=Form(""),plant_name:str=Form(""),department_name:str=Form(""),equipment_group_name:str=Form(""),equipment_class_name:str=Form(""),processtype_name:str=Form(""),integrated_tablet_ip:str=Form(""),tablet_ip:str=Form(""),tablet_ip_1:str=Form(""),integrated_line_name:str=Form(""),iot_plc_ip:str=Form(""),mc_capacity:str=Form(""),mc_max_load:str=Form(""),is_configuration:str=Form(""),user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    try:
        if equipment_code == "" or equipment_name == "":
            return _getErrorResponseJson("Fields Missing...")
        
        result = await getequipmentdtl(cnx, equipment_id,company_name,bu_name,plant_name,equipment_code,equipment_name)
        if len(result)>0:
            return _getErrorResponseJson("Entry Already Exists...")
        
        check_tablet_ip = await gettabletip(cnx, equipment_id,tablet_ip)
        if len(check_tablet_ip)>0:
            return _getErrorResponseJson("Tablet IP Address Already Exists...")
        
        check_iot_plc_ip =await getiotplcip(cnx, equipment_id, iot_plc_ip)
        if len(check_iot_plc_ip)>0:
            return _getErrorResponseJson("IoT PLC IP Address Already Exists...")
        
        if equipment_id == "":
            await save_equipment(cnx, equipment_code, equipment_name, company_name, bu_name, plant_name, department_name, equipment_group_name, equipment_class_name, processtype_name, integrated_tablet_ip, tablet_ip,tablet_ip_1, integrated_line_name, iot_plc_ip, mc_capacity, mc_max_load, is_configuration, user_login_id)
            createFolder("Log/","Query executed successfully for save equipment")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_equipment(cnx, equipment_id, equipment_code, equipment_name, company_name, bu_name, plant_name, department_name, equipment_group_name, equipment_class_name, processtype_name, integrated_tablet_ip, tablet_ip, tablet_ip_1, integrated_line_name, iot_plc_ip, mc_capacity, mc_max_load,is_configuration, user_login_id)
            createFolder("Log/","Query executed successfully for update equipment class")
            return _getSuccessResponseJson("Updated Successfully...")
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_equipment/", tags=["Master Equipment"])
async def remove_equipment_api(equipment_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if equipment_id == "":
        return _getErrorResponseJson("equipment id is required")
    
    try:

        await update_equipmentStatus(cnx, equipment_id)
        createFolder("Log/","Query executed successfully for remove equipment  ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/changestatus_equipment/", tags=["Master Equipment"])
async def changestatus_equipment_api(equipment_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if equipment_id == "":
       return _getErrorResponseJson("equipmentid is required")
    
    try:

        await changestatus_equipment(cnx, equipment_id, active_status)
        createFolder("Log/","Query executed successfully for change equipmentstatus ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_equipment_name/", tags=["Master Equipment"])
async def get_equipment_name_api(plant_id:str=Form(""),company_id:str=Form(""),bu_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_equipment_name(cnx, bu_id, company_id, plant_id)
        createFolder("Log/","Query executed successfully for get equipment name ")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/equipment_interlock_update/", tags=["Master Equipment"])
async def equipment_interlock_update_api(equipment_id:str=Form(""),interlock_status:str=Form(""),colum_name:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if equipment_id == "":
       return _getErrorResponseJson("equipment id is required")
    
    try:

        await equipment_interlock_update(cnx, equipment_id, interlock_status, colum_name)
        createFolder("Log/","Query executed successfully for change equipment interlock update")
        return _getSuccessResponseJson("Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/equipment_hb_ht_mt_update/", tags=["Master Equipment"])
async def equipment_hb_ht_mt_update_api(equipment_id:str=Form(""),heart_beat_time:str=Form(""),handling_time:str=Form(""),minor_stoppage_time:str=Form(""),communicate_type:str=Form(""),kep_id:str=Form(""),run_bit_tagname:str=Form(""),interlock_bit_name:str=Form(""),iiot_bypass_bit_name:str=Form(""),product_start_status:str=Form(""),error_bit_tagname:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if equipment_id == "":
       return _getErrorResponseJson("equipment id is required")
    
    try:

        await equipment_hb_ht_mt_update(cnx, equipment_id, heart_beat_time, handling_time, minor_stoppage_time, communicate_type, kep_id, run_bit_tagname, error_bit_tagname, interlock_bit_name, iiot_bypass_bit_name, product_start_status)
        createFolder("Log/","Query executed successfully for change equipment hb_ht_mt update")
        return _getSuccessResponseJson("Updated Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/equipment_linespeed_Lists/", tags=["Master Equipment"])
async def equipment_linespeed_Lists_api(equipment_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await equipment_linespeed_Lists(cnx, equipment_id)

        createFolder("Log/","Query executed successfully for equipment line speed list")
        
        result = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "equipment_id": equipment_id,
            "equipment_Lists": result
        }

        return result
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/equipment_is_configured_update/", tags=["Master Equipment"])
async def equipment_is_configured_update_api(id:str=Form(""),is_configured:str=Form(""),colum_name:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    # is_configured = 'is_configured'
    if id == "":
       return _getErrorResponseJson("id is required")   
    try:

        await equipment_is_configured_update(cnx, id, is_configured, colum_name)
        createFolder("Log/","Query executed successfully for change equipment is configured update")
        return _getSuccessResponseJson("Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/equipment_linespeed_update/", tags=["Master Equipment"])
async def equipment_linespeed_update_api(id:str=Form(""),select_parameters:str=Form(""),where_parameters:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if id == "":
       return _getErrorResponseJson("id is required")
    
    try:

        await equipment_linespeed_update(cnx, id, select_parameters, where_parameters)
        createFolder("Log/","Query executed successfully for change equipment line speed update")
        return _getSuccessResponseJson("Updated Successfully.")

    except Exception as e:
        return get_exception_response(e)