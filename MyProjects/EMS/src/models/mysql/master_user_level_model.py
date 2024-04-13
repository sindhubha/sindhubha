from sqlalchemy import text

from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text


async def user_levelLists(user_level_id, cnx):
    where = ""
    order_by = ""

    if user_level_id != "":
        where += f' AND mt.user_level_id = "{user_level_id}" '
    order_by += ' ORDER BY mt.user_level_id'

    query = f'''SELECT
                mt.*,
                ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
                ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
            FROM
                master_user_level mt
                left join master_employee cu on cu.employee_id=mt.created_by
                left join master_employee mu on mu.employee_id=mt.modified_by
            WHERE mt.status <> 'delete' {where} {order_by} '''

    get_data_list = await cnx.execute(text(query))
    get_data_list = get_data_list.fetchall()
    return get_data_list

async def getuser_leveldtl(user_level_id, user_level_code, cnx):
    where = ""
    if user_level_id != "":
        where += f" and user_level_id <> {user_level_id}"
    query = f'''select * from master_user_level WHERE 1=1 and status <> 'delete' and user_level_code = '{user_level_code}' {where} '''
    user_data = await cnx.execute(text(query))
    user_data = user_data.fetchall()

    return user_data

async def saveuser_level(user_level_code, user_level_name, user_login_id, cnx):
    query = f'''INSERT INTO master_user_level (user_level_code, user_level_name, created_by, created_on) VALUE ('{user_level_code}', '{user_level_name}',  '{user_login_id}', now()) '''
    await cnx.execute(text(query))
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()

    await update_plant_wise_sync(cnx, 'master_user_level')
    return insert_id


async def updateuser_level(user_level_id, user_level_code, user_level_name, user_login_id, cnx):
    query = f''' UPDATE master_user_level SET user_level_code = '{user_level_code}', 
            user_level_name='{user_level_name}', 
            sync_status = 'update',
            modified_on = now(),
            modified_by='{user_login_id}' 
            WHERE user_level_id = '{user_level_id}' AND status <> 'delete' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_user_level')

async def changestatus_user_level(user_level_id, active_status, cnx):

    status = ""
    if active_status == "active":
        status = "inactive"
    elif active_status == "inactive":
        status = "active"
    query = f''' UPDATE master_user_level SET
            sync_status = 'update',
            status = '{status}' 
            WHERE user_level_id = '{user_level_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_user_level')

async def updateuser_levelStatus(user_level_id, cnx):
    query = f''' UPDATE master_user_level SET 
            sync_status = 'update',
            modified_on = now(),
            status = 'delete' 
            WHERE user_level_id = '{user_level_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_user_level')


async def get_user_level(user_level_id, cnx):
    where = ""
    if user_level_id != "" and user_level_id != 'all':
        where += f''' AND user_level_id = '{user_level_id}' '''
    query = f''' SELECT * FROM master_user_level WHERE 1=1 and status = 'active' {where} '''
    get_data = await cnx.execute(text(query))
    get_data = get_data.fetchall()

    return get_data
