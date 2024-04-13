from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image
import json
from src.models.mysql.master_meter_model import meter_Lists
from log_file import createFolder
from src.models.parse_date import parse_date
from  datetime import date,timedelta

async def budget_entry_list(cnx,reporting_department_id,plant_id)  :
    department_id2 = 0
    department_id = 0
    where = '' 
    if reporting_department_id !='':
        where = f" and mbe.reporting_department_id = {reporting_department_id}"   

    if plant_id !='' and plant_id != 0:
        where = f" and mbe.plant_id = {plant_id}"   
    
    query = text(f"""
        SELECT                
            mbe.*,
            IFNULL(mbe.company_id,'')as company_id,
            IFNULL(mbe.plant_id,'')as plant_id,
            IFNULL(mbe.bu_id,'')as bu_id,
            IFNULL(mc.company_code,'')as company_code,
            IFNULL(mc.company_name,'')as company_name,
            IFNULL(mb.bu_code,'')as bu_code,
            IFNULL(mb.bu_name,'')as bu_name,
            IFNULL(mp.plant_code,'')as plant_code,
            IFNULL(mp.plant_name,'')as plant_name, 
            '' as department_dtl,
            '' as utility_department_dtl,
            IFNULL(mbe.utility_department_ids,'') as utility_department_ids,
            IFNULL(mbe.department_ids,'') as department_ids,
            IFnull(c.campus_name,'') as campus_name,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        from 
            ems_v1.master_budget_entry mbe
            left join ems_v1.master_employee cu on cu.employee_id=mbe.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=mbe.modified_by
            left JOIN ems_v1.master_company mc ON mc.company_id = mbe.company_id
            left JOIN ems_v1.master_business_unit mb ON mb.bu_id = mbe.bu_id
            left JOIN ems_v1.master_plant mp ON mp.plant_id = mbe.plant_id
            left JOIN ems_v1.master_campus c ON c.campus_id = mbe.campus_id
        where mbe.status != 'delete' {where}
    """)
    data = await cnx.execute(query)
    data = data.fetchall()  
    result = []
    for row in data:
        department_id_list = row["department_ids"].strip(",").split(",")     
        department_id_list2 = row["utility_department_ids"].strip(",").split(",")     
        department_dtl = ""
        utility_department_dtl = ""

        for department_id in department_id_list:
            if department_id != '':
                sub_query = text(f"SELECT * FROM ems_v1.master_plant_wise_department WHERE plant_department_id = {department_id}")
                sub_data = await cnx.execute(sub_query)
                sub_data = sub_data.fetchall()
                for sub_row in sub_data:
                    if department_dtl != "":
                        department_dtl += ' \n '
                    department_dtl += f'''{sub_row['plant_department_name']}'''
                    print(department_dtl)

        for department_id2 in department_id_list2:
            if department_id2 != '':
                sub_query = text(f"SELECT * FROM ems_v1.master_plant_wise_department WHERE plant_department_id = {department_id2}")
                sub_data = await cnx.execute(sub_query)
                sub_data = sub_data.fetchall()
                for sub_row in sub_data:
                    if utility_department_dtl != "":
                        utility_department_dtl += ' \n '
                    utility_department_dtl += f'''{sub_row['plant_department_name']}'''
                    print(utility_department_dtl)

        new_row = dict(row)
        new_row["department_dtl"] = department_dtl
        new_row["utility_department_dtl"] = utility_department_dtl
        result.append(new_row)
    return result

async def savebudget(cnx,campus_id,company_id,bu_id,plant_id,reporting_department, department_ids,utility_department_ids,is_corporate,financial_year,budget,user_login_id):
    if department_ids == 'all':
        query = f"select GROUP_CONCAT(plant_department_id SEPARATOR ',') AS department_ids   from master_plant_wise_department where plant_id = {plant_id} group by plant_id"
        data = await cnx.execute(query)
        data = data.fetchall()  
        if len(data)>0:
            for row in data:
                department_ids = row["department_ids"]
        print(department_ids)

    sql = f'''insert into master_budget_entry(campus_idcompany_id,bu_id,plant_id,reporting_department,department_ids,utility_department_ids,financial_year,budget,created_on,created_by)
                values('{campus_id}','{company_id}','{bu_id}','{plant_id}','{reporting_department}','{department_ids}','{utility_department_ids}','{financial_year}','{budget}',now(),{user_login_id})'''
    await cnx.execute(sql)
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()
    
    department_id_list = department_ids.split(",")  
    for department_id in department_id_list: 
            query = text(f'''insert into master_budget_department(reporting_department_id,department_id,financial_year,is_corporate)
                            values({insert_id},{department_id},'{financial_year}','{is_corporate}') ''')
            await cnx.execute(query)
            await cnx.commit()

    if utility_department_ids != '':
        uitility_department_id_list = utility_department_ids.split(",")  
        for department_id in uitility_department_id_list: 
                query_u = text(f'''insert into master_budget_department(reporting_department_id,department_id,financial_year,department_type)
                                values({insert_id},{department_id},'{financial_year}','utility') ''')
                await cnx.execute(query_u)
                await cnx.commit()
    
async def updatebudget(cnx,campus_id,reporting_department_id,company_id,bu_id,plant_id,reporting_department, department_ids,utility_department_ids,is_corporate,financial_year,budget,user_login_id):
    if department_ids == 'all':
        query = f"select GROUP_CONCAT(plant_department_id SEPARATOR ',') AS department_ids from master_plant_wise_department where plant_id = {plant_id} group by plant_id"
        data = await cnx.execute(query)
        data = data.fetchall()  
        if len(data)>0:
            for row in data:
                department_ids = row["department_ids"]
        print(department_ids)
    sql1 = f" delete from master_budget_department where reporting_department_id = {reporting_department_id}"
    await cnx.execute(sql1)
    await cnx.commit() 

    sql2 = f'''update  master_budget_entry 
            set 
                campus_id = '{campus_id}',
                company_id = '{company_id}',
                bu_id = '{bu_id}',
                plant_id = '{plant_id}',
                reporting_department = '{reporting_department}',
                department_ids = '{department_ids}',
                utility_department_ids = '{utility_department_ids}',
                financial_year = '{financial_year}',
                modified_on = now(),
                modified_by = {user_login_id},
                budget  = '{budget}' 
            where reporting_department_id = {reporting_department_id}'''
    await cnx.execute(sql2)
    await cnx.commit()

    departmant_id_list = department_ids.split(",")  
    for department_id in departmant_id_list: 
        query = text(f'''insert into master_budget_department(reporting_department_id,department_id,financial_year,is_corporate)
                            values('{reporting_department_id}',{department_id},'{financial_year}','{is_corporate}') ''')
        await cnx.execute(query)
        await cnx.commit()

    if utility_department_ids != '':

        uitility_department_id_list = utility_department_ids.split(",")  
        for department_id in uitility_department_id_list: 
                query_u = text(f'''insert into master_budget_department(reporting_department_id,department_id,financial_year,department_type)
                                values({reporting_department_id},{department_id},'{financial_year}','utility') ''')
                await cnx.execute(query_u)
                await cnx.commit()        

async def update_budget_status(cnx, reporting_department_id, status):
    
    if status != '':
        query=f''' Update ems_v1.master_budget_entry Set status = '{status}' Where reporting_department_id='{reporting_department_id}' '''
        await cnx.execute(text(query))
        await cnx.commit()
        
    else: 
        query=f''' Update ems_v1.master_budget_entry Set status = 'delete' Where reporting_department_id='{reporting_department_id}' '''
        await cnx.execute(text(query))
        await cnx.commit()

        sql = f" delete from ems_V1.master_budget_department where reporting_department_id = '{reporting_department_id}'"
        await cnx.execute(text(sql))
        await cnx.commit()

async def getbudgetentryname(cnx,reporting_department,plant_id):

    query = f" select * from master_budget_entry where reporting_department = '{reporting_department}'  and plant_id = '{plant_id}' and status !='delete' "
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def budget_rate(cnx, id,campus_id, month,is_year_wise,source_type):
    where = ''
    on_con = ''
    if id != '':
        where +=f" and mbr.id = {id}"
    if campus_id != '':
        where +=f" and mse.campus_id = {campus_id}"
    month = await parse_date(month)
    
    if is_year_wise != 'yes':
       start_year = month.year
       on_con = f" and DATE_FORMAT(mbr.month, '%Y') = {start_year}" 
    else:
       on_con = f" and mbr.month = '{month}'"

    if source_type == 'internal':

        query = f'''
                select
                IFNULL(mbr.id, '') AS id, 
                ifnull(mbr.budget,0) as budget,
                ifnull(mbr.actual,0) as actual,
                ifnull(mbr.actual_total,0) as actual_total,
                ifnull(mbr.budget_mix,0) as budget_mix,
                mse.campus_id,
                mbr.month,
                mse.energy_source_name,
                mbr.created_on,
                mbr.modified_on,
                IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	            IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                FROM
                    master_source_entry mse  
                    left join master_budget_rate mbr on mbr.energy_source_name = mse.energy_source_name and mbr.campus_id = mse.campus_id and mbr.source_type = 'internal' {on_con}
                    left join ems_v1.master_employee cu on cu.employee_id=mbr.created_by
	                left join ems_v1.master_employee mu on mu.employee_id=mbr.modified_by
                where  mse.status = 'active' and mse.source_type = 'internal' {where}  '''
        data = await cnx.execute(query)
        data = data.fetchall()
    else:
        query = f'''
                select
                IFNULL(mbr.id, '') AS id, 
                ifnull(mbr.budget,0) as budget,
                ifnull(mbr.actual,0) as actual,
                ifnull(mbr.actual_total,0) as actual_total,
                ifnull(mbr.budget_mix,0) as budget_mix,
                mse.campus_id,
                mbr.month,
                mse.energy_source_name,
                mbr.created_on,
                mbr.modified_on,
                IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	            IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                FROM
                    master_source_entry mse  
                    left join master_budget_rate mbr on mbr.energy_source_name = mse.energy_source_name and mbr.campus_id = mse.campus_id and mbr.source_type = 'external' {on_con}
                    left join ems_v1.master_employee cu on cu.employee_id=mbr.created_by
	                left join ems_v1.master_employee mu on mu.employee_id=mbr.modified_by
                where  mse.status = 'active' and mse.source_type = 'external' {where}  '''
        data = await cnx.execute(query)
        data = data.fetchall()
    
    return data

async def savebudget_rate(cnx,campus_id,obj,month,source_type,user_login_id,is_year_wise):
    data = json.loads(obj)
    for row in data:
        energy_source_name = row["energy_source_name"]
        budget = row["budget"]
        id = row["id"]
        from_date = await parse_date(month)
        if id == '':
            budget_mix = row["budget_mix"]
            for i in range(0,12):
                date = from_date.replace(day=1, month=from_date.month + i)
                query = f'''insert into master_budget_rate(campus_id,energy_source_name,month,budget,source_type,created_on,created_by,budget_mix)
                            values('{campus_id}','{energy_source_name}','{date}','{budget}','{source_type}',now(),'{user_login_id}','{budget_mix}')'''
                await cnx.execute(text(query))
                await cnx.commit()
        else:

            if is_year_wise == 'yes':
                budget_mix = row["budget_mix"]
                to_date = from_date.replace(day=1, month=from_date.month + 11)
                query = f'''update master_budget_rate set 
                            budget = '{budget}',
                            budget_mix = '{budget_mix}',
                            modified_on = now(),
                            modified_by = {user_login_id}
                            where  month >= '{from_date}' and month <= '{to_date}' and energy_source_name = '{energy_source_name}' and source_type = '{source_type}' and campus_id = {campus_id} '''
                await cnx.execute(text(query))
                await cnx.commit()
                
            else:
                actual = row["actual"]
                actual_total = row["actual_total"]
                query = f'''update master_budget_rate set 
                            actual = '{actual}',
                            actual_total = '{actual_total}',
                            budget = '{budget}',
                            source_type = '{source_type}',
                            modified_on = now(),
                            modified_by = {user_login_id}
                            where  id = '{id}' '''
                await cnx.execute(text(query))
                await cnx.commit()