from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def model_lists(model_make_id,model_id,cnx):

    where = ''
    if model_make_id != '':
        where+= f" and mm.model_make_id = {model_make_id}"

    if model_id != '':
        where+= f" and mm.model_id = {model_id}"

    query= text(f'''
        SELECT
            mm.*,
            mmm.model_make_name,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
            IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_us
        FROM
            ems_v1.master_model mm
            left join ems_v1.master_employee cu on cu.employee_id=mm.created_by
            left join ems_v1.master_employee mu on mu.employee_id=mm.modified_by
            inner join ems_v1.master_model_make mmm on mmm.model_make_id = mm.model_make_id
        WHERE
            mm.status != 'delete' and mmm.status = 'active'{where}
        ''')    
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def getmodeldtl(cnx, model_name):

    select_query = text(f'''SELECT * FROM ems_v1.master_model WHERE model_name = '{model_name}' and status != 'delete' ''')
    data = await cnx.execute(select_query)
    data = data.fetchall()      
    return data

async def save_model(cnx,model_make_id,model_name, user_login_id):
 
    query = text(f'''
                    INSERT INTO ems_v1.master_model (model_make_id,model_name, created_on, created_by)
                    VALUES ({model_make_id},'{model_name}', NOW() , '{user_login_id}')
                    ''')
    await cnx.execute(query)
    await cnx.commit()
    
async def update_model(cnx,model_make_id,model_id,model_name, user_login_id):
    query = text(f'''
                UPDATE ems_v1.master_model SET model_name = '{model_name}', modified_on = NOW(),
                modified_by = '{user_login_id}', model_make_id = {model_make_id}
                WHERE model_id = {model_id}
                ''')
    await cnx.execute(query)
    await cnx.commit()

async def update_modelstatus(cnx, model_id, status):
    if status !='':    
        query = text(f'''
                    UPDATE ems_v1.master_model SET status = '{status}'
                    WHERE model_id = {model_id}
                    ''')
    else:
        query = text(f'''
                    UPDATE ems_v1.master_model SET status = 'delete'
                    WHERE model_id = {model_id}
                    ''')
    await cnx.execute(query)
    await cnx.commit()

