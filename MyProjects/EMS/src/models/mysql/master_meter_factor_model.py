from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder
import socket

async def meter_factor_list(cnx,id,plant_id,meter_id):

    where = ''
    if plant_id != '' and plant_id != "0":
        where += f'and  mf.plant_id = {plant_id}'

    if meter_id != '':
        where += f'and  mf.meter_id = {meter_id}'
    
    if id != '':
        where += f'and  mf.id = {id}'

    sql = text(f'''
            select 
               mf.*,
               mm.meter_code,
               mm.meter_name,
               IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	           IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
            from 
               ems_v1.master_meter_factor mf
               inner join ems_v1.master_meter mm on mm.meter_id = mf.meter_id
               left join master_employee cu on cu.employee_id=mf.created_by
	           left join master_employee mu on mu.employee_id=mf.modified_by   
            where mf.meter_id = mm.meter_id  and mm.meter_id != 'delete' {where}''')
    
    data = await cnx.execute(sql)
    data = data.fetchall()      
    return data
    
async def update_meter_factor(cnx,plant_id,meter_id,obj,user_login_id,request):
    
    obj_data = json.loads(obj)
    sel = {}    
    
    client_host = request.client.host
    
    for data in obj_data:
        for key, value in data.items():
            sel[key] = value
        if plant_id == "0":
            sql = text(f'''UPDATE ems_v1.master_meter_factor SET {', '.join([f"{key} = '{value}'" for key, value in sel.items()])}, modified_on = now(),modified_by = '{user_login_id}',ip_address = '{client_host}' WHERE meter_id = '{meter_id}' ''')
        else:
            sql = text(f'''UPDATE ems_v1.master_meter_factor SET {', '.join([f"{key} = '{value}'" for key, value in sel.items()])}, modified_on = now(),modified_by = '{user_login_id}',ip_address = '{client_host}' WHERE meter_id = '{meter_id}' and plant_id = '{plant_id}' ''')
        await cnx.execute(sql)
        await cnx.commit()
        createFolder("Log/","data.. "+str(sql))
            
   
    
    
