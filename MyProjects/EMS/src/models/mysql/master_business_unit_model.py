from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text

async def business_unit_Lists(cnx, bu_id,company_id,campus_id):
    where = ""
    orderby = ""
    tablename = ''
    group_by = ''

    if campus_id != '' and campus_id != "0":
        tablename = f''' inner join master_plant mp on mp.bu_id = mt.bu_id'''
        where = f"and mp.campus_id = '{campus_id}' "
        group_by = "group by mt.bu_id"

    if bu_id != "" and bu_id != "0":
        where += f"and mt.bu_id = '{bu_id}' "
    
    if company_id != "" and company_id != 'all' and company_id != "0":
        where += f''' and mt.company_id = '{company_id}' '''
    
    orderby += "mt.bu_id"

    query= f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name
		FROM
			master_business_unit mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER join master_company mc on mc.company_id = mt.company_id
            {tablename}
		WHERE mt.status <> 'delete' {where} 
        {group_by}
        ORDER BY {orderby}'''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def getbudtl(cnx, bu_id, bu_code, bu_name):
    where=""

    if bu_id != "":
        where += f"and bu_id <> '{bu_id}' "
      
    query= f'''select * from master_business_unit where 1=1 and status<>'delete' and bu_code= '{bu_code}' {where}'''

    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def savebusiness_unit(cnx, bu_code, bu_name, company_name, user_login_id):
      query= f'''insert into master_business_unit(bu_code,bu_name, company_id, created_on,created_by )
               values('{bu_code}','{bu_name}','{company_name}',now(),'{user_login_id}')
      '''
      await cnx.execute(text(query))
      insert_id = await cnx.execute("SELECT LAST_INSERT_ID()")
      insert_id = insert_id.first()[0]
      await cnx.commit()
      
      await update_plant_wise_sync(cnx, 'master_business_unit')
    #   update_is_assign(cnx, 'master_company', 'master_business_unit', insert_id, company_name, 'company_id', 'bu_id') 
      return insert_id
      
async def updatebusiness_unit(cnx, bu_id, bu_code, bu_name, company_name, user_login_id):
    query= f''' update 
                    master_business_unit
                set 
                    bu_code = '{bu_code}',
                    bu_name = '{bu_name}',
                    company_id = '{company_name}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    bu_id = '{bu_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_business_unit')


async def updatebusiness_unitStatus(cnx, bu_id, status='delete'):
      query=f''' Update master_business_unit Set sync_status = 'update', status = '{status}' where bu_id = '{bu_id}'
      '''
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_business_unit')
      
async def changestatus_business_unit(cnx, bu_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_business_unit Set sync_status = 'update',status = '{status}' Where bu_id='{bu_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_business_unit')

async def get_branch_name(cnx, company_id):
    where = ""
    
    if company_id != "" and company_id != 'all':
        where += f''' and company_id = '{company_id}' '''
        
    query = f''' select * from master_business_unit where 1=1 and status = 'active' {where} '''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()

    return result



    

    