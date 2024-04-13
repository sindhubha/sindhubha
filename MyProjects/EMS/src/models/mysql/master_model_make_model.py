from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def model_make_lists(model_make_id,cnx):

    where = ''
    if model_make_id != '':
        where += f" and mm.model_make_id = {model_make_id}"

    query= text(f'''
                 SELECT
                    mm.*,
                    IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
                    IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                FROM
                    ems_v1.master_model_make mm
                    left join ems_v1.master_employee cu on cu.employee_id=mm.created_by
                    left join ems_v1.master_employee mu on mu.employee_id=mm.modified_by
                WHERE
                    mm.status != 'delete' {where}
                ''')    
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def getmodelmakedtl(cnx, model_make_name):
    select_query = text(f'''SELECT * FROM ems_v1.master_model_make WHERE model_make_name = '{model_make_name}' and status != 'delete' ''')
    data = await cnx.execute(select_query)
    data = data.fetchall()      
    return data

async def save_modelmake(cnx,model_make_name, user_login_id):
 
    query = text(f'''
                    INSERT INTO ems_v1.master_model_make (model_make_name, created_on, created_by)
                    VALUES ('{model_make_name}', NOW() , '{user_login_id}')
                    ''')
    await cnx.execute(query)
    await cnx.commit()

    
async def update_model_make(cnx,model_make_id,model_make_name, user_login_id):
    query = (f'''
                UPDATE ems_v1.master_model_make SET model_make_name = '{model_make_name}', modified_on = NOW(),
                modified_by = '{user_login_id}'
                WHERE model_make_id = {model_make_id}
                ''')
   
    await cnx.execute(text(query))
    await cnx.commit()

async def update_model_makestatus(cnx, model_make_id, status):
    
    if status !='':    
        query = text(f'''
                    UPDATE ems_v1.master_model_make SET status = '{status}'
                    WHERE model_make_id = {model_make_id}
                    ''')
    else:
        query = text(f'''
                    UPDATE ems_v1.master_model_make SET status = 'delete'
                    WHERE model_make_id = {model_make_id}
                    ''')
    await cnx.execute(query)
    await cnx.commit()

