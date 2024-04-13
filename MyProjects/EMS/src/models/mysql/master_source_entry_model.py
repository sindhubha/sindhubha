from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image,id
from src.models.parse_date import parse_date
from datetime import datetime,date, timedelta
from dateutil.relativedelta import relativedelta
import json
from log_file import createFolder


async def source_entry_list(cnx, id,period_type,source_type):

    where = ""
    if id !='' and id != None:
        where += f" AND s.id = {id}"  

    if period_type !='' and period_type != None:
        where += f" AND s.period_type = '{period_type}' " 
    if source_type!= '':
        where +=f" and s.source_type = '{source_type}'"  
    
    query = text(f"""
        SELECT                
            s.*,
            c.campus_name,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        FROM 
            ems_v1.master_source_entry s
            left join ems_v1.master_employee cu on cu.employee_id=s.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=s.modified_by                
	        left join ems_v1.master_campus c on c.campus_id=s.campus_id                
        WHERE 
            s.status != 'delete'{where}
    """)
   
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def save_source_entry(cnx, campus_id,period_type,energy_source_name,user_login_id,source_type):

    query = text(f"""
            INSERT INTO ems_v1.master_source_entry (campus_id,period_type,
            energy_source_name,created_on, created_by,source_type
            )
            VALUES (
                '{campus_id}','{period_type}','{energy_source_name}',  now(), '{user_login_id}','{source_type}'
            )
        """) 
    await cnx.execute(query)
    await cnx.commit()
    
async def update_source_entry(cnx, id,campus_id,period_type,energy_source_name,user_login_id,source_type):

    query =text(f"""
        UPDATE 
            ems_v1.master_source_entry
        SET 
            campus_id = '{campus_id}',
            period_type = '{period_type}',
            energy_source_name = '{energy_source_name}', 
            modified_on = NOW(),
            modified_by = '{user_login_id}',
            source_type = '{source_type}'
            WHERE id = {id} 
    """)
    
    await cnx.execute(query)
    await cnx.commit()
    
async def update_sourceentryStatus(cnx, id, status):
    if status != '':
        query=f''' Update ems_v1.master_source_entry Set status = '{status}' Where id='{id}' '''
    else: 
        query=f''' Update ems_v1.master_source_entry Set status = 'delete' Where id='{id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    
async def source_entry_data(cnx,campus_id,mill_date,period_type):
    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
    # mill_date = await parse_date(mill_date)
    where = ''
    if campus_id != '':
        where += f" and mse.campus_id = {campus_id}"
        
    if period_type == 'date':
        query = f'''
            SELECT
                IFNULL(msed.id, '') AS id,
                mse.energy_source_name,
                mse.id as energy_source_id,
                IFNULL(msed.consumption, 0) AS consumption,
                IFNULL(msed.consumption_total, 0) AS consumption_total,
                msed.created_on,
                msed.modified_on,
                IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	            IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        
            FROM
                master_source_entry mse    
            LEFT JOIN master_source_entry_date msed ON
                mse.campus_id = msed.campus_id
                AND mse.id = msed.energy_source_id
                AND msed.mill_date = '{mill_date}'
                AND mse.period_type = 'date'
            left join ems_v1.master_employee cu on cu.employee_id=msed.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=msed.modified_by            
            where mse.status = 'active' and mse.period_type = 'date'and mse.source_type = 'external'{where}
        '''

        res = await cnx.execute(query)
        res = res.fetchall()
        
    if period_type == "month":
        month_year=f"""{mill_month[mill_date.month]}-{str(mill_date.year)}"""

        query_m = f'''
                select
                    ifnull(msed.id,'')id,
                    mse.energy_source_name,
                    mse.id as energy_source_id,
                    ifnull(msed.consumption,0)consumption,
                    ifnull(msed.consumption_total,0)consumption_total,
                    msed.created_on,
                    msed.modified_on,
                    IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                from
                    master_source_entry mse
                    left join master_source_entry_date msed
                        on
                            mse.campus_id=msed.campus_id
                            and mse.id=msed.energy_source_id
                            and  DATE_FORMAT(msed.mill_date ,'%m-%Y')='{month_year}'
                            and mse.period_type='month' 
                    left join ems_v1.master_employee cu on cu.employee_id=msed.created_by
	                left join ems_v1.master_employee mu on mu.employee_id=msed.modified_by
                where mse.status = 'active' and mse.period_type = 'month'  and mse.source_type = 'external'{where}'''
        
        res_m = await cnx.execute(query_m)
        res = res_m.fetchall()
        
    return res

async def save_source_entry_data(cnx, obj,obj2,mill_date_month,campus_id,mill_date,user_login_id):
    createFolder("Source_entry/",f" obj_data = {obj},obj_month = {obj2}")
   
    obj_data = json.loads(obj.strip('"'))
    obj_month = json.loads(obj2.strip('"'))
   
    mill_date = '-'.join(reversed(mill_date.split('-')))
    mill_date_month = '-'.join(reversed(mill_date_month.split('-')))
    
    for row in obj_data:
        id = row.get('id', '')
        energy_source_id = row.get('energy_source_id', '')
        consumption = row.get('consumption', '')
        consumption_total = row.get('consumption_total', '')
        if id == '':
            query = text(f"""
                    INSERT INTO ems_v1.master_source_entry_date (campus_id,
                    energy_source_id,consumption,consumption_total,mill_date,created_on, created_by
                    )
                    VALUES (
                        '{campus_id}','{energy_source_id}','{consumption}', '{consumption}','{mill_date}', now(), '{user_login_id}'
                    )
                """) 

        else:
            query = f'''
                    update master_source_entry_date
                    set campus_id = '{campus_id}',
                    energy_source_id = '{energy_source_id}',
                    consumption = '{consumption}',
                    consumption_total = '{consumption}',
                    modified_on = now(),
                    modified_by = '{user_login_id}'
                    where id = {id} '''
        await cnx.execute(query)
        await cnx.commit()


    for row_m in obj_month:
        id = row_m.get('id', '')
        energy_source_id = row_m.get('energy_source_id', '')
        consumption = row_m.get('consumption', '')
        consumption_total = row_m.get('consumption_total', '')
        if id == '':
            query_m = text(f"""
                    INSERT INTO ems_v1.master_source_entry_date (campus_id,
                    energy_source_id,consumption,consumption_total,mill_date,created_on, created_by
                    )
                    VALUES (
                        '{campus_id}','{energy_source_id}','{consumption}', '{consumption_total}','{mill_date_month}', now(), '{user_login_id}'
                    )
                """) 
            await cnx.execute(query_m)
            await cnx.commit()
            sql_mail = f"update  master_mail set send_date = now() where campus_id = '{campus_id}' and report = 'Energy Statement With Tariff'"
            await cnx.execute(sql_mail)
            await cnx.commit()
        else:
            query_m = f'''
                    update master_source_entry_date
                    set campus_id = '{campus_id}',
                    energy_source_id = '{energy_source_id}',
                    consumption = '{consumption}',
                    consumption_total = '{consumption_total}',
                    mill_date = '{mill_date_month}',
                    modified_on = now(),
                    modified_by = '{user_login_id}'
                    where id = {id} '''
            await cnx.execute(query_m)
            await cnx.commit()
            
            sql_mail = f"update  master_mail set send_date = now() where campus_id = '{campus_id}' and report = 'Energy Statement With Tariff'"
            await cnx.execute(sql_mail)
            await cnx.commit()

        

