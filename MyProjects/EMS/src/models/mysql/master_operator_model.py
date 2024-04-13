from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text


async def operator_Lists(cnx, operator_id='',plant_id = '',company_id = '',bu_id = ''):
    where = ""
    if operator_id != "" and operator_id != "0":
        where += f" and mt.employee_id = '{operator_id}'"
    if plant_id != "" and plant_id != '0':
        where += f" and mt.plant_id = '{plant_id}' "
    if company_id != "" and company_id != "0":
        where += f" and mt.company_id = '{company_id}' "
    if bu_id != "" and bu_id != "0":
        where += f" and mt.bu_id = '{bu_id}' "
    order_by = " order by employee_id "

    query = f''' SELECT
                mt.*,MD5(mt.password_login) AS password,
                mt.employee_id as operator_id,
                mt.employee_code as operator_code,
                mt.employee_name as operator_name,
                concat(mt.employee_code,'-',mt.employee_name) as operator_actual,
                ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
                ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
                ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
                ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
                ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name
            FROM
                master_employee mt
                left join master_employee cu on cu.employee_id=mt.created_by
                left join master_employee mu on mu.employee_id=mt.modified_by
                INNER JOIN master_company mc on mc.company_id = mt.company_id
                INNER JOIN master_business_unit mb on mb.bu_id = mt.bu_id
                INNER JOIN master_plant mp on mp.plant_id = mt.plant_id
            WHERE mt.status <> 'delete' AND mt.employee_type = 'Operator' {where} {order_by} '''
    print(query)

    data = await cnx.execute(query)
    data = data.fetchall()      
    return data

async def getoperatordtl(cnx, operator_id, operator_code, operator_name):
    where = ""
    if operator_id != "" :
        where += f" and employee_id <> '{operator_id}'"

    query = f" select * from master_employee where 1=1 and status<>'delete' and employee_code= '{operator_code}' {where}"

    data = await cnx.execute(query)
    data = data.fetchall()      
    return data


async def save_operator(operator_code, operator_name, company_name, bu_name, plant_name, password, user_login_id, cnx):
    query = f'''insert into master_employee (employee_code, employee_name, company_id, bu_id, plant_id, password_login, employee_type, is_login, created_on, created_by)
                value ('{operator_code}', '{operator_name}', '{company_name}', '{bu_name}', '{plant_name}', MD5('{password}'), 'Operator', 'yes', now(), '{user_login_id}') '''

    await cnx.execute(text(query))
    await cnx.commit()


async def update_operator(operator_id, operator_code, operator_name, company_name, bu_name, plant_name, password,  user_login_id, cnx):
    query = f''' update master_employee set 
                 employee_code = '{operator_code}', employee_name = '{operator_name}', 
                 company_id = '{company_name}', bu_id = '{bu_name}', 
                 plant_id = {plant_name}, password_login = MD5('{password}'), 
                 employee_type = 'Operator', is_login = 'yes',
                 sync_status = 'update',
                 modified_on = now(), modified_by = {user_login_id}
                 where employee_id = {operator_id} '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def update_operatorStatus(operator_id, cnx, status='delete'):
    query = f''' update master_employee set sync_status = 'update', status = '{status}' where employee_id = '{operator_id}' '''

    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def changestatus_operator(operator_id, active_status, cnx):
    status = ""
    if active_status == "inactive":
        status = "active"
    elif active_status == 'active':
        status = "inactive"
    query = f''' update master_employee set sync_status = 'update', status = '{status}' where employee_id = '{operator_id}' '''

    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def reset_password(operator_id, cnx):
    query = f''' Update master_employee Set sync_status = 'update',password_login = MD5(employee_code),is_first_password='yes' Where employee_id = '{operator_id}' '''

    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")
