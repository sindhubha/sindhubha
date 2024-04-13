from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

async def parameter_roundoff_list(cnx,plant_id):
    where = ""
    if plant_id != '':
        where += f' where mpf.plant_id = {plant_id}'
    sql = text(f'''select * 
               from ems_v1.master_parameter_roundoff mpf 
               {where}''')
    data = await cnx.execute(sql)
    data = data.fetchall()      
    return data
    

async def update_parameterroundoff(cnx,plant_id,obj):
 
    obj_data = json.loads(obj)
    sel = {}
    for data in obj_data:
        for key, value in data.items():
            sel[key] = value
        if plant_id == 'all' or plant_id == 0:
            sql = text(f'''UPDATE ems_v1.master_parameter_roundoff SET {', '.join([f"{key} = '{value}'" for key, value in sel.items()])}  ''')
        else:
            sql = text(f'''UPDATE ems_v1.master_parameter_roundoff SET {', '.join([f"{key} = '{value}'" for key, value in sel.items()])} WHERE plant_id = '{plant_id}' ''')
        await cnx.execute(sql)
        await cnx.commit()
        createFolder("Log/","data.. "+str(sql))
    
    
    
