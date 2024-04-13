from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image,id

async def source_Lists(cnx, source_id):

    where = ""
    if source_id !='':
        where = f" AND s.source_id = {source_id}"   
    # where += f" and mm.source_id in ({','.join(str(x) for x in source_id)})  
    query = text(f"""
        SELECT                
            s.*,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        FROM 
            ems_v1.master_source s
            left join ems_v1.master_employee cu on cu.employee_id=s.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=s.modified_by                
        WHERE 
            s.status != 'delete'{where}
    """)
   
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def save_source(cnx,source_name,user_login_id):

    query = text(f"""
            INSERT INTO ems_v1.master_source (
            source_name,created_on, created_by
            )
            VALUES (
                '{source_name}',  now(), '{user_login_id}'
            )
        """) 
    await cnx.execute(query)
    await cnx.commit()
    
async def update_source(cnx,source_id,source_name,user_login_id):

    query =text(f"""
        UPDATE 
            ems_v1.master_source
        SET 
            source_name = '{source_name}', 
            modified_on = NOW(),
            modified_by = '{user_login_id}'
            WHERE source_id = {source_id} 
    """)
    
    await cnx.execute(query)
    await cnx.commit()
    
async def update_sourceStatus(cnx, source_id, status):
    if status != '':
        query=f''' Update ems_v1.master_source Set status = '{status}' Where source_id='{source_id}' '''
    else: 
        query=f''' Update ems_v1.master_source Set status = 'delete' Where source_id='{source_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    

