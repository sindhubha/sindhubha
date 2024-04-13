from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text

async def processtype_Lists(cnx, processtype_id):
    where = ""
    orderby = ""
    
    if processtype_id != "":
        where += f"and mt.mfg_process_type_id = '{processtype_id}'"
    orderby += "mt.mfg_process_type_id"

    query=text(f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
		FROM
			master_mfg_process_type mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}''')
    
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data

async def getprocesstypedtl(cnx, processtype_id, processtype_code, processtype_name):
    where=""

    if processtype_id != "":
        where += f"and mfg_process_type_id <> '{processtype_id}' "
      
    query=f'''select * from master_mfg_process_type where 1=1 and status<>'delete' and mfg_process_type_code='{processtype_code}' {where}'''

    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def saveprocesstype(cnx, processtype_code, processtype_name, user_login_id):
      query= f'''insert into master_mfg_process_type(mfg_process_type_code,mfg_process_type_name,created_on,created_by)
               values('{processtype_code}','{processtype_name}',now(),'{user_login_id}')
      '''
      await cnx.execute(text(query))
      insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
      insert_id = insert_id.first()[0]
      await cnx.commit()
      
      await update_plant_wise_sync(cnx, 'master_mfg_process_type')
      return insert_id

async def updateprocesstype(cnx, processtype_id, processtype_code, processtype_name, user_login_id):
    query=f''' update 
                    master_mfg_process_type
                set 
                    mfg_process_type_code = '{processtype_code}',
                    mfg_process_type_name = '{processtype_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    mfg_process_type_id = '{processtype_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_mfg_process_type')

async def updateprocesstypeStatus(cnx, processtype_id, status='delete'):
    
      query=f''' Update master_mfg_process_type Set sync_status = 'update',status = '{status}' Where mfg_process_type_id='{processtype_id}' '''
      
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_mfg_process_type')

async def changestatus_processtype(cnx, processtype_id,active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    if active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_mfg_process_type Set sync_status = 'update',status = '{status}' Where mfg_process_type_id='{processtype_id}' '''
    print(query)
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_mfg_process_type')


async def get_processtype_name(cnx):
    query = f''' select * from master_mfg_process_type where 1=1 and status = 'active' '''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

