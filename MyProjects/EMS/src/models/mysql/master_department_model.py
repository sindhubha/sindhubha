from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text
async def department_Lists(cnx, plant_department_id,plant_id, bu_id, company_id,campus_id):
    where = ""
    orderby = ""
    
    if plant_department_id != "" and plant_department_id != "0":
        where += f"and mt.plant_department_id = '{plant_department_id}' "
    if bu_id != "" and bu_id != 'all' and bu_id != "0":
        where += f''' and mt.bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all' and company_id != "0":
        where += f''' and mt.company_id = '{company_id}' '''
    if plant_id != "" and plant_id != 'all' and plant_id != "0":
        where += f''' and mt.plant_id = '{plant_id}' '''
    if campus_id != "" and campus_id != 'all' and campus_id != "0":
        where += f''' and mp.campus_id = '{campus_id}' '''
    
    orderby += "mt.plant_department_id"

    query=f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name
		FROM
            master_plant_wise_department mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER JOIN master_company mc on mc.company_id = mt.company_id
			INNER JOIN master_business_unit mb on mb.bu_id = mt.bu_id
            INNER JOIN master_plant mp on mp.plant_id = mt.plant_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()
    return result

async def getdepartmentdtl(cnx, plant_department_id, department_code, department_name):
    where=""

    if plant_department_id != "":
        where += f"and plant_department_id <> '{plant_department_id}' "
      
    query=f'''select * from master_plant_wise_department where 1=1 and status<>'delete' and plant_department_code='{department_code}' {where}'''

    result = await cnx.execute(text(query))
    result = result.fetchall()
    return result

async def save_department(cnx, department_code, department_name, company_name, bu_name, plant_name, user_login_id):
      query= f'''insert into master_plant_wise_department(plant_department_code,plant_department_name,company_id,bu_id,plant_id,created_on,created_by)
               values('{department_code}','{department_name}','{company_name}','{bu_name}','{plant_name}',now(),'{user_login_id}')
      '''
      await cnx.execute(text(query))
      result = await cnx.execute("SELECT LAST_INSERT_ID()")
      insert_id = result.first()[0]
      await cnx.commit()
      
      update_plant_wise_sync(cnx, 'master_plant_wise_department')
      return insert_id

async def update_department(cnx, department_id, department_code, department_name, user_login_id):
    query=f''' update 
                    master_plant_wise_department
                set 
                    plant_department_code = '{department_code}',
                    plant_department_name = '{department_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    plant_department_id = '{department_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_plant_wise_department')

async def update_departmentStatus(cnx, plant_department_id, status='delete'):

      query=f''' Update master_plant_wise_department Set sync_status = 'update',status = '{status}' Where plant_department_id='{plant_department_id}' '''
      
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_plant_wise_department')

async def changestatus_department(cnx, plant_department_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_plant_wise_department Set sync_status = 'update',status = '{status}' Where plant_department_id='{plant_department_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_plant_wise_department')

async def get_department_name(cnx, plant_id, bu_id, company_id):
    where = ""
    
    if bu_id != "" and bu_id != 'all':
        where += f''' and bu_id = '{bu_id}' '''
    if company_id != "" and company_id != 'all':
        where += f''' and company_id = '{company_id}' '''
    if plant_id != "" and plant_id != 'all':
        where += f''' and plant_id = '{plant_id}' '''
    
        
    query = f''' select * from master_plant_wise_department where 1=1 and status = 'active' {where} '''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()

    return result