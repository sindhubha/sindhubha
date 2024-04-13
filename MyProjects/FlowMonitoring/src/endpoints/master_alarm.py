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
        from src.models.mysql.master_alarm_model import alarm_Lists,getalarmdtl,save_alarm,update_alarm,update_alarmStatus,alarm_popup_status
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.master_alarm_model import alarm_Lists,getalarmdtl,save_alarm,update_alarm,update_alarmStatus,alarm_popup_status
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/alarm_list/", tags=["Master Alarm"])
async def get_alarmlist(company_id: str=Form(''),
                        alarm_target_id: str=Form(''),
                        alarm_type : str = Form(''),
                        bu_id : str = Form(''),
                        plant_id : str = Form(''),
                        plant_department_id : str = Form(''),
                        equipment_group_id : str = Form(''),
                        cnx: Session = Depends(get_db)):
    try: 
        
        result = await alarm_Lists(cnx, company_id,alarm_target_id,alarm_type,bu_id,plant_id,plant_department_id,equipment_group_id)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_alarm_detail/", tags=["Master Alarm"])
async def save_alarm_detail(alarm_target_id:str=Form(''),
                            company_id:str=Form(''),
                            bu_id:str=Form(''),
                            plant_id : str = Form(''),
                            plant_department_id : str = Form(''),
                            equipment_group_id : str = Form(''),
                            parameter_name:str=Form(''),
                            meter_id:str=Form(''),
                            alarm_name:str=Form(''),
                            alarm_type:str=Form(''),
                            alarm_duration:int=Form(''),
                            color_1:str=Form(''),
                            color_2:str=Form(''),
                            color_3:str=Form(''),
                            conditions:str=Form(''),
                            login_id:str=Form(''),
                            cnx: AsyncSession = Depends(get_db)):
 
  
    try:
        if company_id == "" :
            return _getErrorResponseJson("company_id is required...")
        
        if parameter_name == "" :
            return _getErrorResponseJson("parameter_name is required...")
        
        if alarm_name == "" :
            return _getErrorResponseJson("alarm_name is required...")
        
        if alarm_type == "" :
            return _getErrorResponseJson("alarm_type is required...")
        if alarm_type == "time_based":
            if alarm_duration == '':
                return _getErrorResponseJson("alarm duration is required")
            
            color_1 = 0
            color_2 = 0
            color_3 = 0
        elif alarm_type == 'time_based_condition':
            if alarm_duration == '':
                return _getErrorResponseJson("alarm duration is required")
            if color_1 == '':
                return _getErrorResponseJson("color_1 is required")
            if conditions == '':
                return _getErrorResponseJson("condition is required")
            
        else:
            alarm_duration = 0
            
            if color_1 == '':
                return _getErrorResponseJson("color_1 is required")
            
            if color_2 == '':
                return _getErrorResponseJson("color_2 is required")
            
            if color_3 == '':
                return _getErrorResponseJson("color_3 is required")
            
        if alarm_target_id == "":
            result = getalarmdtl(cnx, alarm_target_id,  alarm_name)
            if len(result)>0:
               return _getErrorResponseJson("Entry Already Exists...")
            
            await save_alarm(cnx,company_id,bu_id,plant_id ,plant_department_id ,equipment_group_id ,parameter_name,meter_id,alarm_name,alarm_type,alarm_duration,color_1,color_2,color_3,login_id,conditions)
            createFolder("Log/","Query executed successfully for save plant alarm")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_alarm(cnx, alarm_target_id,company_id,bu_id,plant_id ,plant_department_id ,equipment_group_id ,parameter_name,meter_id,alarm_name,alarm_type,alarm_duration,color_1,color_2,color_3,login_id,conditions)
            createFolder("Log/","Query executed successfully for update plant alarm")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_alarm_detail/", tags=["Master Alarm"])
async def remove_alarm_detail(alarm_target_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if alarm_target_id == "":
        return _getErrorResponseJson("alarm id is required")
    
    try:

        await update_alarmStatus(cnx, alarm_target_id, status)
        if status !='':
            return _getSuccessResponseJson("status updated successfully.")
        else:
            return _getSuccessResponseJson("deleted successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_alarm_popup_status/", tags=["Master Alarm"])
async def update_alarm_popup_status(company_id: str = Form(''),                             
                                    cnx: AsyncSession = Depends(get_db)):
    
    try:
        if company_id == "":
            return _getErrorResponseJson("alarm id is required")
        await alarm_popup_status(cnx,company_id)
        return _getSuccessResponseJson("status updated successfully.")

    except Exception as e:
        return get_exception_response(e)