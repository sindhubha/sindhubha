from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text


async def plant_Lists(cnx, plant_id, bu_id, company_id,campus_id):
    where = ""
    orderby = ""
    
    if plant_id != "" and plant_id != "0":
        where += f"and mt.plant_id = '{plant_id}' "

    if bu_id != "" and bu_id != 'all' and bu_id != "0":
        where += f''' and mt.bu_id = '{bu_id}' '''

    if company_id != "" and company_id != 'all' and company_id != "0":
        where += f''' and mt.company_id = '{company_id}' '''
    
    if campus_id != "" and campus_id != 'all' and campus_id != "0":
        where += f''' and c.campus_id = '{campus_id}' '''
    
    orderby += "mt.plant_id"

    query=text(f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            c.campus_name
		FROM
			master_plant mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER join master_company mc on mc.company_id = mt.company_id
            INNER join master_business_unit mb on mb.bu_id = mt.bu_id
            left join master_campus c on c.campus_id = mt.campus_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}''')
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data

async def getplantdtl(cnx, plant_id, plant_code, plant_name):
    where=""

    if plant_id != "":
        where += f"and plant_id <> '{plant_id}' "
      
    query=text(f'''select * from master_plant where 1=1 and status<>'delete' and plant_code = '{plant_code}' {where}''')

    data = await cnx.execute(query)
    data = data.fetchall()      
    return data

async def save_plant(cnx, plant_code, plant_name, company_name, bu_name, oracle_id, ramco_id, plant_address, plant_pincode, plant_state, plant_country, host_ip, is_subcategory, user_login_id,campus_id):
    query= f'''insert into master_plant(plant_code,plant_name,company_id,bu_id,oracle_id,ramco_id,plant_address,plant_pincode,plant_state,plant_country,host_ip, is_subcategory, created_on,created_by,campus_id)
             values('{plant_code}','{plant_name}','{company_name}','{bu_name}','{oracle_id}','{ramco_id}','{plant_address}','{plant_pincode}','{plant_state}','{plant_country}','{host_ip}','{is_subcategory}',now(),'{user_login_id}','{campus_id}')
    '''
    print(query)
    await cnx.execute(text(query))
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()
    
    await update_plant_wise_sync(cnx, 'master_plant')

    query = text(f'insert into ems_v1.master_parameter_roundoff (plant_id) values({insert_id})')
    await cnx.execute(query)
    await cnx.commit()

    sql = text(f'''
            INSERT INTO  power_report_fields_original (report_id, field_code, field_name, is_show, slno, field_name_display, plant_id,unit)
                SELECT pr.report_id, pfo.field_code, pfo.field_name, pfo.is_show, pfo.slno, pfo.field_name_display, {insert_id}, pfo.unit
                FROM (SELECT DISTINCT report_id FROM power_report where report_type = 'report') pr
                CROSS JOIN power_report_fields_original pfo
                where pfo.plant_id = 0  and pfo.report_id = 0 
            order by
                pr.report_id
        ''')
    await cnx.execute(sql)
    await cnx.commit()
    sql = text(f'''
        INSERT INTO  power_report_fields_original (report_id, field_code, field_name, is_show, slno, field_name_display, plant_id,unit)
            SELECT pr.report_id, pfo.field_code, pfo.field_name, pfo.is_show, pfo.slno, pfo.field_name_display, {insert_id}, pfo.unit
            FROM (SELECT DISTINCT report_id FROM power_report where report_type = 'dashboard') pr
            CROSS JOIN power_report_fields_original pfo
            where pfo.plant_id = 0 and pfo.report_id = 0 and pfo.report_type = 'dashboard'
        order by
            pr.report_id
    ''')
    await cnx.execute(sql)
    await cnx.commit()
    return insert_id

async def update_plant(cnx, plant_id, plant_code, plant_name, company_name, bu_name, oracle_id, ramco_id, plant_address, plant_pincode, plant_state, plant_country, host_ip, is_subcategory, user_login_id,campus_id):
    query=f''' update 
                    master_plant
                set 
                    plant_code = '{plant_code}',
                    plant_name = '{plant_name}',
                    company_id = '{company_name}',
                    bu_id = '{bu_name}',
                    oracle_id = '{oracle_id}',
                    ramco_id = '{ramco_id}',
                    plant_address = '{plant_address}',
                    plant_pincode = '{plant_pincode}',
                    plant_state = '{plant_state}',
                    plant_country = '{plant_country}',
                    host_ip = '{host_ip}',
                    sync_status = 'update',
                    is_subcategory = '{is_subcategory}',
                    modified_on = now(),
                    modified_by = '{user_login_id}',
                    campus_id = '{campus_id}'
                where 
                    plant_id = '{plant_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    
    sql = f"update master_meter set campus_id = '{campus_id}' where plant_id = '{plant_id}'"
    await cnx.execute(text(sql))
    await cnx.commit()

    await update_plant_wise_sync(cnx, 'master_plant')

async def update_plantStatus(cnx, plant_id,status='delete'):
      query=f''' Update master_plant Set sync_status = 'update', status = '{status}' where plant_id = '{plant_id}'
      '''
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_plant')

async def changestatus_plant(cnx, plant_id, active_status):
    status = ''

    if status == 'inactive':
        status = 'active'
    elif status == 'active':
        status = 'inactive'
    
    query = f''' Update master_plant Set sync_status = 'update',status = '{active_status}' Where plant_id='{plant_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_plant')

async def get_plant_name(cnx, bu_id, company_id):
    where = ""
    
    if bu_id != "" and bu_id != 'all':
        where += f''' and bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all':
        where += f''' and company_id = '{company_id}' '''
        
    query = f''' select * from master_plant where 1=1 and status = 'active' {where} '''
    
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data

async def change_posting_plant(cnx, plant_id, active_status):
    status = ''

    if active_status == 'yes':
        status = 'no'
    elif active_status == 'no':
        status = 'yes'

    query = f''' Update master_plant Set sync_status = 'update',is_posting = '{status}' Where plant_id = '{plant_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_plant')

