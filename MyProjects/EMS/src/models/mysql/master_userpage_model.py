from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
import json
from sqlalchemy import text


async def userpagelist(userpage_id, cnx):
    where = ""
    if userpage_id != "":
        where += f" and mt.employee_id = '{userpage_id}' "
    order_by = " order by mt.employee_id"
    query = f""" SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
            ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name,
            ifnull(concat(mul.user_level_code,'-',mul.user_level_name),'') as user_level_name
		FROM
			master_employee mt
			LEFT JOIN master_employee cu on cu.employee_id=mt.created_by
			LEFT JOIN master_employee mu on mu.employee_id=mt.modified_by
            LEFT JOIN master_company mc on mc.company_id = mt.company_id
            LEFT JOIN master_business_unit mb on mb.bu_id = mt.bu_id
            LEFT JOIN master_plant mp on mp.plant_id = mt.plant_id
            INNER JOIN master_user_level mul on mul.user_level_id = mt.user_level_id
		WHERE mt.status <> 'delete' and (mt.employee_type = 'Level 0' OR mt.employee_type = 'Company' OR mt.employee_type = 'BU' OR mt.employee_type = 'Plant') {where} {order_by}""" 
    get_data = await cnx.execute(text(query))
    get_data = get_data.fetchall()
    
    return get_data


async def getuserpagedtl(userpage_id, userpage_code, userpage_name, cnx):
    where = "" 
    if userpage_id != "" :
        where += f" and employee_id <> {userpage_id}"
        pass
    query = f""" SELECT * FROM master_employee WHERE 1=1 and status<>'delete' and employee_code = '{userpage_code}' {where}"""

    get_user_data = await cnx.execute(text(query))
    get_user_data = get_user_data.fetchall()
    await update_plant_wise_sync(cnx, "master_employee")
    return get_user_data

async def check_email(userpage_id, userpage_code, email_id, cnx):
    where = ""

    if userpage_id != "":
        where += f" AND employee_id <> '{userpage_id}' " 
    query = f""" SELECT * FROM master_employee WHERE 1=1 AND status<>'delete' AND  email = '{email_id}' {where}"""
    get_user_data = await cnx.execute(text(query))
    get_user_data = get_user_data.fetchall()
    await update_plant_wise_sync(cnx, "master_employee")
    return get_user_data


async def saveuserpage(userpage_code, userpage_name, company_id, bu_id, plant_id, mobile_no, email_id, user_designation, password, user_level_id, user_login_id,is_campus, cnx):
    employee_type = ""
    if user_level_id == 1:
        employee_type = "Level 0"
    elif user_level_id == 2:
        employee_type = "Company"
    elif user_level_id == 3:
        employee_type = "BU"
    elif user_level_id == 4:
        employee_type = "Plant"
    
    query = f""" INSERT INTO master_employee (employee_code, employee_name, company_id, bu_id, plant_id, mobile_no, email, designation, password_login, user_level_id, employee_type, created_on, is_login, created_by,is_campus) VALUE ('{userpage_code}', '{userpage_name}', '{company_id}', '{bu_id}', '{plant_id}', '{mobile_no}', '{email_id}', '{user_designation}', MD5('{password}'), '{user_level_id}', '{employee_type}', now(), 'yes', '{user_login_id}','{is_campus}')"""
    
    await cnx.execute(text(query))
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")
    return insert_id


async def updateuserpage(userpage_id, userpage_code, userpage_name, company_id, bu_id, plant_id, mobile_no, email_id, user_designation, password, user_level_id, user_login_id, is_campus,cnx):
    query = f''' UPDATE master_employee SET 
            employee_code = '{userpage_code}',
            employee_name = '{userpage_name}',
            company_id = '{company_id}',
            bu_id = '{bu_id}',
            plant_id = '{plant_id}',
            mobile_no = '{mobile_no}',
            email = '{email_id}',
            designation = '{user_designation}',
            user_level_id = '{user_level_id}',
            is_login = 'yes',
            sync_status = 'update',
            modified_on = now(),
            modified_by = '{user_login_id}',
            is_campus = '{is_campus}'
            WHERE
            employee_id = '{userpage_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def updateuserpageStatus(userpage_id, cnx, status='delete'):
    query = f""" UPDATE master_employee SET sync_status = 'update', status = '{status}' WHERE employee_id = {userpage_id}"""
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def changestatus_userpage(userpage_id, active_status, cnx):
    status = ""
    if active_status == "inactive":
        status = "active"
    elif active_status == "active":
        status = "inactive"
    query = f""" UPDATE master_employee SET sync_status = 'update', status = '{status}' WHERE employee_id = '{userpage_id}' """
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def reset_password(userpage_id, cnx):
    query = f""" UPDATE master_employee SET sync_status = 'update', password_login = MD5(employee_code) WHERE  employee_id = '{userpage_id}' """
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, "master_employee")


async def get_rights_userLists(cnx, userpage_id, is_login):
    where = ""
    if userpage_id != "":
        where += f" and mt.employee_id = '{userpage_id}' "
    if is_login != "":
        where += f" and mt.is_login = 'yes'"
    
    order_by = "order by mt.employee_id"

    query = f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
            ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name
		FROM
			master_employee mt
			LEFT JOIN master_employee cu on cu.employee_id=mt.created_by
			LEFT JOIN master_employee mu on mu.employee_id=mt.modified_by
            LEFT JOIN master_company mc on mc.company_id = mt.company_id
            LEFT JOIN master_business_unit mb on mb.bu_id = mt.bu_id
            LEFT JOIN master_plant mp on mp.plant_id = mt.plant_id
		WHERE mt.status <> 'delete' and mt.employee_type <> 'Admin' {where} {order_by} '''
    print(query)
    get_data = await cnx.execute(text(query))
    get_data = get_data.mappings().all()

    return get_data


async def getallMenus(cnx):
    query = """SELECT 
                *,
                0 AS u_r_id,
                'yes' AS add_op,
                'yes' AS edit_op,
                'yes' AS delete_op
            FROM
                menu_mas
            WHERE STATUS='active'
            ORDER BY slno"""
    print(query)
    get_data = await cnx.execute(text(query))
    get_data = get_data.mappings().all()
    print(11111)
    return get_data


async def getUserAccessMenus(employee_id, cnx):
    query = f"""SELECT
                m.*,
                u.id AS u_r_id,
                IFNULL(u.id,0) AS u_r_id,
                IFNULL(u.add_op,0) AS add_op,
                IFNULL(u.edit_op,0) AS edit_op,
                IFNULL(u.delete_op,0) AS delete_op
            FROM
                menu_mas m,
                user_rights u
            WHERE
                m.status='active' and
                m.menu_id=u.menu_id and
                u.userid = '{employee_id}'
            ORDER BY m.slno """
    get_data = await cnx.execute(text(query))
    get_data = get_data.mappings().all()

    return get_data


async def getUserMenus(employee_id, cnx):
    query = f"""SELECT
                m.*,
                IFNULL(u.id,0) AS u_r_id,
                IFNULL(u.add_op,0) AS add_op,
                IFNULL(u.edit_op,0) AS edit_op,
                IFNULL(u.delete_op,0) AS delete_op
            FROM
                menu_mas m
                LEFT OUTER JOIN (SELECT * FROM user_rights WHERE userid= '{employee_id}') u
                    ON m.menu_id=u.menu_id
            WHERE m.status='active' 
            ORDER BY m.slno"""

    get_data = await cnx.execute(text(query))
    get_data = get_data.mappings().all()
    return get_data


async def save_user_rights(employee_id, obj, cnx):
    query = f" DELETE FROM user_rights WHERE userid = '{employee_id}'"
    print(query)
    cnx.execute(text(query))
    cnx.commit()
    user_list = json.loads(obj)
    for data in user_list:
        query = f"INSERT INTO user_rights (menu_id, add_op, edit_op, delete_op, userid) VALUE ( '{data['menu_id']}', '{data['add_op']}', '{data['edit_op']}', '{data['delete_op']}', '{employee_id}') "
        await cnx.execute(text(query))
        await cnx.commit()
    await update_plant_wise_sync(cnx, "user_rights")
