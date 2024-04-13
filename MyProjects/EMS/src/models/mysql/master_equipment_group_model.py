from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text

async def equipment_group_Lists( equipment_group_id,company_id,cnx):
    where = ""
    orderby = ""

    if equipment_group_id != "":
        where += f"and mt.equipment_group_id = '{equipment_group_id}' "
    if company_id != "" and company_id != 'all' and company_id != "0":
        where += f''' and mt.company_id = '{company_id}' '''
    
    orderby += "mt.equipment_group_id"

    query=f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name
		FROM
			master_equipment_group mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER JOIN master_company mc on mc.company_id = mt.company_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def getequipmentgroupdtl(cnx, equipment_group_id, equipment_group_code, equipment_group_name):
    where=""

    if equipment_group_id != "":
        where += f"and equipment_group_id <> '{equipment_group_id}' "
      
    query=f'''select * from master_equipment_group where 1=1 and status<>'delete' and equipment_group_code='{equipment_group_code}' {where}'''

    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def saveequipment_group(cnx, equipment_group_code, equipment_group_name, company_name, user_login_id):
      query= f'''insert into master_equipment_group(equipment_group_code,equipment_group_name,company_id,created_on,created_by)
               values('{equipment_group_code}','{equipment_group_name}','{company_name}',now(),'{user_login_id}')
      '''
      await cnx.execute(text(query))
      insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
      insert_id = insert_id.first()[0]
      await cnx.commit()
      
      await update_plant_wise_sync(cnx, 'master_equipment_group')
      return insert_id

async def updateequipment_group(cnx, equipment_group_id, equipment_group_code, equipment_group_name, company_name, user_login_id):
    query=f''' update 
                    master_equipment_group
                set 
                    equipment_group_code = '{equipment_group_code}',
                    equipment_group_name = '{equipment_group_name}',
                    company_id = '{company_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    equipment_group_id = '{equipment_group_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_equipment_group')

async def updateequipment_groupStatus(cnx, equipment_group_id, status='delete'):
    
      query=f''' Update master_equipment_group Set sync_status = 'update',status = '{status}' Where equipment_group_id='{equipment_group_id}' '''
      
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_equipment_group')

async def changestatus_equipment_group(cnx, equipment_group_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_equipment_group Set sync_status = 'update',status = '{status}' Where equipment_group_id= '{equipment_group_id}' '''
    print(query)
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx,'master_equipment_group')

async def get_equipment_group_name(cnx, company_id, equipment_class_id):
    
    query = f''' select * from master_equipment_group where 1=1 and status = 'active'  '''

    if company_id != "" and company_id != 'all':
        query += f''' and company_id = '{company_id}' '''
    if equipment_class_id != "" and equipment_class_id != 'all':
        query += f''' AND equipment_group_id IN (SELECT equipment_group_id FROM master_equipment WHERE equipment_class_id = '{equipment_class_id}') '''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def get_equipment_group_plant(cnx, plant_id, plant_department_id, equipment_class_id):
    where = ""

    if plant_department_id != "":
        where += f''' and md.plant_department_id='{plant_department_id}' '''
    if equipment_class_id != "":
        where += f''' and me.equipment_class_id='{equipment_class_id}' '''

    query=f''' SELECT
					mt.*
				FROM
					master_equipment_group mt
					INNER JOIN master_company mc ON mc.company_id=mt.company_id AND mc.status='active'
					INNER JOIN master_plant mp ON mp.company_id=mc.company_id AND mp.status='active'
					INNER JOIN master_plant_wise_department md ON md.company_id=mc.company_id AND md.plant_id=mp.plant_id AND md.status='active'
					INNER JOIN master_equipment me ON me.company_id=mc.company_id AND me.plant_id=mp.plant_id AND me.plant_department_id=md.plant_department_id AND me.equipment_group_id=mt.equipment_group_id
				WHERE
					mt.status='active' AND 
					mp.plant_id='{plant_id}' {where}
				GROUP BY mt.equipment_group_id
				ORDER BY mt.equipment_group_id '''
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data