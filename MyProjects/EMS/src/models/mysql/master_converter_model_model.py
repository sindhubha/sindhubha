from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def model_lists(converter_make_id,converter_model_id,cnx):

    where = ''
    if converter_model_id != '':
        where+= f" and mm.converter_model_id in ({converter_model_id})"
    if converter_make_id != '':
        where+= f" and mm.converter_make_id = {converter_make_id}"

    
    query= text(f'''
        SELECT
            mm.*,
            mmm.converter_make_name,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
            IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_us
        FROM
            ems_v1.master_converter_model mm
            left join ems_v1.master_employee cu on cu.employee_id=mm.created_by
            left join ems_v1.master_employee mu on mu.employee_id=mm.modified_by
            inner join ems_v1.master_converter_make mmm on mmm.converter_make_id = mm.converter_make_id
        WHERE
            mm.status != 'delete' and mmm.status = 'active'{where}
        ''')    
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def getmodeldtl(cnx, converter_model_name):

    select_query = text(f'''SELECT * FROM ems_v1.master_converter_model WHERE converter_model_name = '{converter_model_name}' and status != 'delete' ''')
    data = await cnx.execute(select_query)
    data = data.fetchall()      
    return data

async def save_model(cnx,converter_make_id,converter_model_name, user_login_id):
 
    query = text(f'''
                    INSERT INTO ems_v1.master_converter_model (converter_make_id,converter_model_name, created_on, created_by)
                    VALUES ({converter_make_id},'{converter_model_name}', NOW() , '{user_login_id}')
                    ''')
    await cnx.execute(query)
    await cnx.commit()
    
async def update_model(cnx,converter_make_id,converter_model_id,converter_model_name, user_login_id):
    query = text(f'''
                UPDATE ems_v1.master_converter_model SET converter_model_name = '{converter_model_name}', modified_on = NOW(),
                modified_by = '{user_login_id}', converter_make_id = {converter_make_id}
                WHERE converter_model_id = {converter_model_id}
                ''')
    await cnx.execute(query)
    await cnx.commit()

async def update_modelstatus(cnx, converter_model_id, status):
    if status !='':    
        query = text(f'''
                    UPDATE ems_v1.master_converter_model SET status = '{status}'
                    WHERE converter_model_id = {converter_model_id}
                    ''')
    else:
        query = text(f'''
                    UPDATE ems_v1.master_converter_model SET status = 'delete'
                    WHERE converter_model_id = {converter_model_id}
                    ''')
    await cnx.execute(query)
    await cnx.commit()

