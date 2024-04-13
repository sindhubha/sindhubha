from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response

async def converter_make_lists(campus_id,converter_make_id,cnx):

    where = ''
    if converter_make_id != '':
        print(converter_make_id)
        where += f" and mm.converter_make_id  in ({converter_make_id})"

    if campus_id != '' and campus_id != "0" :
        sql = text(f"SELECT * FROM master_converter_detail WHERE campus_id = '{campus_id}'")
        res = await cnx.execute(sql)
        res = res.fetchall()
        converter_model_ids = [row["converter_model_id"] for row in res]

        if len(converter_model_ids) > 0:
            sql2 = text(f"SELECT * FROM master_converter_model WHERE converter_model_id IN ({','.join(map(str, converter_model_ids))})")
            res2 = await cnx.execute(sql2)
            res2 = res2.fetchall()
            converter_make_ids = [row2["converter_make_id"] for row2 in res2]

            if len(converter_make_ids) !=  0 and converter_make_id == '':
                where += f" AND mm.converter_make_id IN ({','.join(map(str, converter_make_ids))})"

    query= text(f'''
                 SELECT
                    mm.*,
                    IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
                    IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                FROM
                    ems_v1.master_converter_make mm
                    left join ems_v1.master_employee cu on cu.employee_id=mm.created_by
                    left join ems_v1.master_employee mu on mu.employee_id=mm.modified_by
                WHERE
                    mm.status != 'delete' {where}
                ''')
    print(query)    
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def getmodelmakedtl(cnx, converter_make_name):
    select_query = text(f'''SELECT * FROM ems_v1.master_converter_make WHERE converter_make_name = '{converter_make_name}' and status != 'delete' ''')
    data = await cnx.execute(select_query)
    data = data.fetchall()      
    return data

async def save_modelmake(cnx,converter_make_name, user_login_id):
 
    query = text(f'''
                    INSERT INTO ems_v1.master_converter_make (converter_make_name, created_on, created_by)
                    VALUES ('{converter_make_name}', NOW() , '{user_login_id}')
                    ''')
    await cnx.execute(query)
    await cnx.commit()

    
async def update_converter_make(cnx,converter_make_id,converter_make_name, user_login_id):
    query = (f'''
                UPDATE ems_v1.master_converter_make SET converter_make_name = '{converter_make_name}', modified_on = NOW(),
                modified_by = '{user_login_id}'
                WHERE converter_make_id = {converter_make_id}
                ''')
   
    await cnx.execute(text(query))
    await cnx.commit()

async def update_converter_makestatus(cnx, converter_make_id, status):
    
    if status !='':    
        query = text(f'''
                    UPDATE ems_v1.master_converter_make SET status = '{status}'
                    WHERE converter_make_id = {converter_make_id}
                    ''')
    else:
        query = text(f'''
                    UPDATE ems_v1.master_converter_make SET status = 'delete'
                    WHERE converter_make_id = {converter_make_id}
                    ''')
    await cnx.execute(query)
    await cnx.commit()

