from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image,id

async def campus_Lists(cnx, campus_id):
    try:
        where = ""
        if campus_id !='':
            where = f" AND c.campus_id = {campus_id}"    
        # where += f" and mm.campus_id in ({','.join(str(x) for x in campus_id)})  
        query = text(f"""
            SELECT                
                c.*,
                IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	            IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user

            FROM 
                ems_v1.master_campus c
                left join ems_v1.master_employee cu on cu.employee_id=c.created_by
	            left join ems_v1.master_employee mu on mu.employee_id=c.modified_by                
            WHERE 
                c.status != 'delete'{where}
        """)
   
        data = await cnx.execute(query)
        data = data.fetchall()
            
        return data
    except Exception as e:
        return get_exception_response(e)

async def getcampusdtl(cnx,  campus_code):
    
    query=f'''select * from ems_v1.master_campus where 1=1 and status<>'delete' and campus_code= '{campus_code}' '''

    data = await cnx.execute(query)
    data = data.fetchall()
    
    return data
    
async def save_campus(cnx,campus_code,campus_name,demand_meter_limit,user_login_id):

    try: 
        
        query = text(f"""
                INSERT INTO ems_v1.master_campus (
                campus_name, campus_code, created_on, created_by,demand_meter_limit
                )
                VALUES (
                    '{campus_name}', '{campus_code}',  now(), '{user_login_id}','{demand_meter_limit}'
                )
            """) 
    
        await cnx.execute(query)
        await cnx.commit()
        print(query)

    except Exception as e:
        return get_exception_response(e)
    
async def update_campus(cnx,campus_id,campus_code,campus_name,demand_meter_limit,user_login_id):
    try:
        query =text(f"""
            UPDATE 
                ems_v1.master_campus
            SET 
                campus_name = '{campus_name}', 
                campus_code = '{campus_code}',
                modified_on = NOW(),
                modified_by = '{user_login_id}',
                demand_meter_limit = '{demand_meter_limit}'
                WHERE campus_id = {campus_id} 
        """)
        
        await cnx.execute(query)
        await cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    
async def update_campusStatus(cnx, campus_id, status):
    if status != '':
        query=f''' Update ems_v1.master_campus Set status = '{status}' Where campus_id='{campus_id}' '''
    else: 
        query=f''' Update ems_v1.master_campus Set status = 'delete' Where campus_id='{campus_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    

