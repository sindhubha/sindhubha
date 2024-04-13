from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from src.models.image import save_image
from sqlalchemy import text
import os
from src.models.save_pdf import save_pdf_file

async def companyLists(cnx, base_path, company_id,campus_id):
    where = ""
    orderby = ""
    tablename = ''
    group_by = ''
    if campus_id != '' and campus_id != "0":
        tablename = f''' inner join master_plant mp on mp.company_id = mt.company_id'''
        where = f"and mp.campus_id = '{campus_id}' "
        group_by = "group by mt.company_id"
    if company_id != "" and company_id != "0":
        where = f"and mt.company_id = '{company_id}' "
    orderby += "mt.company_id"

    query= text(f''' SELECT
			mt.*,
            CONCAT('{base_path}attachments/company/group_logo/', mt.group_logo) AS group_logo_url,
            CONCAT('{base_path}attachments/company/company_logo/', mt.company_logo) AS company_logo_url,
            CONCAT('{base_path}attachments/company/company_pdf_file/', mt.pdf_attach) as pdf_file_url,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
		FROM
			master_company mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
            {tablename}
		WHERE mt.status <> 'delete' {where}
        {group_by}
        ORDER BY {orderby}''')
    
    # result = cnx.execute(text(query)).fetchall()
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def getcompanydtl(cnx, company_id, company_code, company_name):
    where=""

    if company_id != "":
        where += f"and company_id <> '{company_id}' "
      
    query=text(f'''select * from master_company where 1=1 and status<>'delete' and company_code= '{company_code}' {where}''')

    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def savecompany(cnx, company_code, company_name, oracle_id, ramco_id, group_logo_old, company_logo_old, pdf_attach_old, group_logo, company_logo, pdf_attach, user_login_id, static_dir):

    if group_logo_old == "":
        group_logo_image = await save_image(group_logo, f"{static_dir}/company/group_logo")
    else:
        group_logo_image = group_logo_old
    
    if company_logo_old == "":
        company_logo_image = await save_image(company_logo, f"{static_dir}/company/company_logo")
        
    else:
        company_logo_image = company_logo_old
    
    if pdf_attach_old == "":
        pdf_attach_file = await save_pdf_file(pdf_attach, f"{static_dir}/company/company_pdf_file")
    else:
        pdf_attach_file = pdf_attach_old
        
    
    query= f'''insert into master_company(company_code,company_name, oracle_id, ramco_id,group_logo,company_logo,pdf_attach,created_on,created_by )
            values('{company_code}','{company_name}','{oracle_id}','{ramco_id}','{group_logo_image}','{company_logo_image}','{pdf_attach_file}',now(),'{user_login_id}')
    '''
    await cnx.execute(text(query))
    result = await cnx.execute("SELECT LAST_INSERT_ID()")
    insert_id =  result.first()[0]

    # Commit the changes
    await cnx.commit()
    
    await update_plant_wise_sync(cnx,'master_company')

    return insert_id

async def updatecompany(cnx, company_id, company_code, company_name, oracle_id, ramco_id, group_logo_old, company_logo_old, pdf_attach_old, group_logo, company_logo, pdf_attach, user_login_id, static_dir):
    # group_logo_image = save_image(group_logo_old, f"{static_dir}/company/group_logo")
    # company_logo_image = save_image(company_logo_old, f"{static_dir}/company/company_logo")

    if group_logo_old == "":
        group_logo_image = await save_image(group_logo, f"{static_dir}/company/group_logo")
    else:
        group_logo_image = group_logo_old

    if company_logo_old == "":
        company_logo_image = await save_image(company_logo, f"{static_dir}/company/company_logo")
        
    else:
        company_logo_image = company_logo_old
    
    if pdf_attach_old == "":
        pdf_attach_file = await save_pdf_file(pdf_attach, f"{static_dir}/company/company_pdf_file")
    else:
        pdf_attach_file = pdf_attach_old
        
    query=f''' update 
                    master_company 
                set 
                    company_code = '{company_code}',
                    company_name = '{company_name}',
                    oracle_id = '{oracle_id}',
                    ramco_id = '{ramco_id}',
                    group_logo = '{group_logo_image}',
                    company_logo = '{company_logo_image}',
                    pdf_attach = '{pdf_attach_file}',
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    company_id = '{company_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_company')

async def updatecompanyStatus(cnx, company_id, status='delete'):
      
      query=f''' Update master_company Set sync_status = 'update', status = '{status}' where company_id = '{company_id}'
      '''
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_company')

async def changestatus_company(cnx, company_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_company Set sync_status = 'update',status = '{status}' Where company_id='{company_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_company')

async def get_company_name(cnx):
    
    query = f''' select * from master_company where 1=1 and status = 'active' '''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result



     

     
      



      
      
      