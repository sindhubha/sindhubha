from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image

async def holiday_type_list(cnx, id):

    where = '' 
    if id !='':
        where = f" and ht.id = {id}"   
    # where += f" and mm.machine_id in ({','.join(str(x) for x in machine_id)})  
    query = text(f"""
        SELECT                
            ht.*,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        FROM 
            ems_v1.master_holiday_type ht
            left join ems_v1.master_employee cu on cu.employee_id=ht.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=ht.modified_by                
            where ht.status !='delete'{where}
    """)
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def save_holidaytype(cnx,holiday_type,user_login_id):

    data = ''
    query = text(f"""
            INSERT INTO ems_v1.master_holiday_type (
            holiday_type, created_on, created_by
            )
            VALUES (
                '{holiday_type}',  now(), '{user_login_id}'
            )
        """)
    await cnx.execute(query)
    await cnx.commit()

    return data
    
async def update_holidaytype(cnx,id,holiday_type,user_login_id):

    query =text(f"""
            UPDATE ems_v1.master_holiday_type
            SET holiday_type = '{holiday_type}', modified_on = now(),
            modified_by = '{user_login_id}'
            WHERE id = {id} 
        """)
    
    await cnx.execute(query)
    await cnx.commit()
    
async def update_holidaytype_status(cnx, id, status):
    try:
        if status !='':               
            query = text(f'''UPDATE ems_v1.master_holiday_type  SET status = '{status}' WHERE id = {id}''')        
        else:
            query = text(f'''UPDATE ems_v1.master_holiday_type  SET status = 'delete' WHERE id = {id}''')        
        await cnx.execute(query)
        await cnx.commit()
    except Exception as e:
        return get_exception_response(e)
    

def getholidaytypedtl(cnx, holiday_type):
   
      
    query=f'''select * from master_holiday_type where 1=1 and status<>'delete' and holiday_type='{holiday_type}' '''

    result = cnx.execute(text(query)).fetchall()
    
    return result