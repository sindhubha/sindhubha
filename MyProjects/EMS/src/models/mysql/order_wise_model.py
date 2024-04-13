from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.parse_date import parse_date
from datetime import date

async def orderwise(cnx,table_name,obj):

    if obj !='':
        obj_data = json.loads(obj)
        for row in obj_data:
            id = row["id"]
            sno = row["sno"]
            
            if table_name == 'plant':
                sql = text(f" update master_plant set plant_order = {sno} where plant_id = {id} ")
                
            if table_name == 'plant_department':
                sql = text(f" update master_plant_department set plant_department_order = {sno} where plant_department_id = {id} ")
            
            if table_name == 'equipment_group':
                sql = text(f" update master_equipment_group set equipment_group_order = {sno} where equipment_group_id = {id} ")
                
            if table_name == 'function_1' or table_name == 'function_2' or table_name == 'function' :
                sql = text(f" update master_function set function_order = {sno} where function_id = {id}")
                
            if table_name == 'meter':
                sql = text(f" update master_meter set meter_order = {sno} where meter_id = {id} ")
            
            if table_name == 'equipment':
                sql = text(f" update master_equipment set equipment_order = {sno} where equipment_id = {id} ")
            
            await cnx.execute(sql)
            await cnx.commit()
       
        