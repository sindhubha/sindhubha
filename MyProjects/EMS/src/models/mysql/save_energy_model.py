from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

async def save_energy_dtl(cnx,obj):
    if obj !='':
        obj_data = json.loads(obj)
        for row in obj_data:
            id = row["id"]
            meter_id = row["meter_id"]  
            mill_date = row["mill_date"]
            initial_kwh = row["initial_kwh"]
            shift1_kwh = row["shift1_kwh"]
            shift2_kwh = row["shift2_kwh"]
            shift3_kwh = row["shift3_kwh"]
            user_login_id = row["user_login_id"]
            if id == '':
                sql = text(f'''insert into ems_v1.energy (meter_id,mill_date,initial_kwh,shift1_kwh,shift2_kwh,shift3_kwh,created_on,created_by)
                          values('{meter_id}','{mill_date}','{initial_kwh}','{shift1_kwh}','{shift2_kwh}','{shift3_kwh}',NOW(), '{user_login_id}')''')
            else:
                sql = text(f'''update ems_v1.energy set mill_date = '{mill_date}', initial_kwh = '{initial_kwh}',
                          shift1_kwh = '{shift1_kwh}', shift2_kwh = '{shift2_kwh}', shift3_kwh = '{shift3_kwh}', modified_on = NOW(),
                          modified_by = '{user_login_id}',meter_id = '{meter_id}'
                          where id = '{id}' ''')
            await cnx.execute(sql)
            await cnx.commit()
                   
    