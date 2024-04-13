from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def group_namelist(cnx):

        
    query = text(f'''select * from ems_v1.master_group_name where status!='delete' ''')
    data = await cnx.execute(query)
    data = data.fetchall()
    
    return data
    
async def get_groupname_dtl(cnx, group_name):
    
    select_query = text(f'''select * from ems_v1.master_group_name  where group_name = '{group_name}' and status!='delete' ''')
    data = await cnx.execute(select_query)
    data = data.fetchall()
    
    return data

async def save_groupname(cnx,group_name,user_login_id):
  
    query = text(f'''INSERT INTO ems_v1.master_group_name 
                     (group_name,created_on,created_by)
                     values('{group_name}',getdate(),'{user_login_id}')''')
    await cnx.execute(query)
    result = await cnx.execute("SELECT LAST_INSERT_ID()")
    insert_id =  result.fetchone()
    await cnx.commit()
    return insert_id
  
async def update_groupname(cnx, id,group_name,user_login_id):
 
    query = text(f'''UPDATE ems_v1.master_group_name
                    SET group_name ='{group_name}',modified_on= getdate(),modified_by = '{user_login_id}' 
                    where id = {id} ''')
    
    await cnx.execute(query)
    await cnx.commit()
    
    
async def update_groupname_status(cnx, id, status):
    
    if status !='':
        query = text(f" UPDATE ems_v1.master_group_name  SET status = '{status}' WHERE id = '{id}' ")
        await cnx.execute(query)
    else:
        query = text(f" UPDATE ems_v1.master_group_name  SET status = 'delete' WHERE id = '{id}' ")                
        await cnx.execute(query)
    await cnx.commit()

