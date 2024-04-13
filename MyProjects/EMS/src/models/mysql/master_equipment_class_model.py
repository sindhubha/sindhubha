from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text

async def equipment_class_Lists(cnx, equipment_class_id):
    where = ""
    orderby = ""
    
    if equipment_class_id != "":
        where += f"and mt.equipment_class_id = '{equipment_class_id}'"
    orderby += "mt.equipment_class_id"

    query=f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
		FROM
			master_equipment_class mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def getequipmentclassdtl(cnx, equipment_class_id, equipment_class_code, equipment_class_name):
    where=""

    if equipment_class_id != "":
        where += f"and equipment_class_id <> '{equipment_class_id}' "
      
    query=f'''select * from master_equipment_class where 1=1 and status<>'delete' and equipment_class_code='{equipment_class_code}' {where}'''

    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def saveequipment_class(cnx, equipment_class_code, equipment_class_name, user_login_id):
      query= f'''insert into master_equipment_class(equipment_class_code,equipment_class_name,created_on,created_by)
               values('{equipment_class_code}','{equipment_class_name}',now(),'{user_login_id}')
      '''
      await cnx.execute(text(query))
      insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
      insert_id = insert_id.first()[0]
      await cnx.commit()
      
      await update_plant_wise_sync(cnx, 'master_equipment_class')
      return insert_id

async def updateequipment_class(cnx, equipment_class_id, equipment_class_code, equipment_class_name, user_login_id):
    query=f''' update 
                    master_equipment_class
                set 
                    equipment_class_code = '{equipment_class_code}',
                    equipment_class_name = '{equipment_class_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    equipment_class_id = '{equipment_class_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_equipment_class')

async def updateequipment_classStatus(cnx, equipment_class_id, status='delete'):
    
      query=f''' Update master_equipment_class Set sync_status = 'update',status = '{status}' Where equipment_class_id='{equipment_class_id}' '''
      
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_equipment_class')

async def changestatus_equipment_class(cnx, equipment_class_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_equipment_class Set sync_status = 'update',status = '{status}' Where equipment_class_id='{equipment_class_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_equipment_class')

async def get_equipment_class_name(cnx):
    query = f''' select * from master_equipment_class where 1=1 and status = 'active' '''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data