from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

async def holidaylist(cnx, id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,holiday_type):

    data2 = ''
    where = ''
    if id != '':
        where +=f'and mh.id = {id}'

    if company_id != '':
        where+=f" and mhm.company_id = {company_id}"

    if bu_id != '':
        where+=f" and mhm.bu_id = {bu_id}"

    if plant_id != '':
        where+=f" and mhm.plant_id = {plant_id}"

    if plant_department_id != '':
        where+=f" and mhm.plant_department_id = {plant_department_id}"

    if equipment_group_id != '':
        where+=f" and mhm.equipment_group_id = {equipment_group_id}"

    query = text(f'''
            select 
                mh.id ,
                mh.holiday_year,
                '' as equipment_dtl,
                GROUP_CONCAT(mhm.equipment_id SEPARATOR ',') AS equipment_id,
                mh.status,
                mhm.company_id,
                mhm.bu_id,
                mhm.plant_id,
                mhm.plant_department_id,
                mhm.equipment_group_id,
                IFNULL(mc.company_code,'') company_code,
                IFNULL(mc.company_name,'') company_name,
                IFNULL(bu.bu_code,'') bu_code,
                IFNULL(bu.bu_name,'') bu_name,
                IFNULL(md.plant_code,'') plant_code,
                IFNULL(md.plant_name,'') plant_name,
                IFNULL(ms.plant_department_code,'') plant_department_code,
                IFNULL(ms.plant_department_name,'') plant_department_name,
                IFNULL(mmt.equipment_group_code,'') equipment_group_code,
                IFNULL(mmt.equipment_group_name,'') equipment_group_name,
                IfNULL(concat(min(cu.employee_code),'-',min(cu.employee_name)),'') as created_user,
	            IfNULL(concat(min(mu.employee_code),'-',min(mu.employee_name)),'') as modified_user,
                mh.created_on ,
                mh.modified_on 
            from 
                ems_v1.master_holiday mh
                inner join ems_v1.master_holiday_meter mhm on mh.id = mhm.ref_id
                left join ems_v1.master_company mc on mc.company_id = mhm.company_id
                left join ems_v1.master_business_unit bu on bu.bu_id = mhm.bu_id
                left join ems_v1.master_plant md on mhm.plant_id = md.plant_id
                left join ems_v1.master_plant_wise_department ms on mhm.plant_department_id = ms.plant_department_id
                left join ems_v1.master_equipment_group mmt on mhm.equipment_group_id = mmt.equipment_group_id
                left join ems_v1.master_employee cu on cu.employee_id=mh.created_by
                left join ems_v1.master_employee mu on mu.employee_id=mh.modified_by 
                inner join ems_v1.master_equipment me on me.equipment_id = mhm.equipment_id
            where 
                mh.status != 'delete' {where}
            group by mh.id 
             
            ''')
    # print(query)
    data=await cnx.execute(query)
    data=data.fetchall()
    
    result = []
          
    for row in data:
        equipment_id = row["equipment_id"] 
        equipment_id_list = equipment_id.split(",")   
        equipment_dtl = ""

        for equipment_id in equipment_id_list:                             
            sub_query = text(f"SELECT * FROM ems_v1.master_equipment WHERE equipment_id = {equipment_id}")
            sub_data = await cnx.execute(sub_query)
            sub_data = sub_data.fetchall()

            for sub_row in sub_data:
                if equipment_dtl != "":
                    equipment_dtl += '\n' 
                equipment_dtl += f'''{sub_row['equipment_name']}''' 
                          
        new_row = dict(row)
        new_row["equipment_dtl"] = equipment_dtl
        result.append(new_row)

    where_d = ''
    if id != '':
        if holiday_type != '':
            where_d += f" and holiday_type = '{holiday_type}'"
        query2 = text(f''' 
                    select 
                        id,
                        ref_id,
                        DATE_FORMAT(holiday_date,'%d-%m-%Y') as holiday_date,
                        description,
                        holiday_type,
                        ifnull(Weekend_day,'') Weekend_day
                    from 
                        ems_v1.master_holiday_date 
                    where ref_id = {id} {where_d}''')
        data2 = await cnx.execute(query2)
        data2 = data2.fetchall()
                       
    return {"data":result,"data2":data2}

async def save_holiday_dtl(cnx, year ,company_id,bu_id,equipment_id ,plant_id ,plant_department_id ,equipment_group_id ,obj,obj2,user_login_id):
    
    if company_id == None:
        company_id = ''
        
    if bu_id == None:
        bu_id = ''

    if plant_id == None:
        plant_id = ''

    if plant_department_id == None:
        plant_department_id = ''

    if equipment_group_id == None:
        equipment_group_id = ''
    
    query = text(f''' insert into ems_v1.master_holiday (holiday_year,created_on,created_by)
                     values({year}, now(), {user_login_id})''')
    createFolder("Holiday_Log/", "master_holiday" + str(query)) 
    await cnx.execute(query)
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()
        
    equipment_id_list = equipment_id.split(",")  
    for equipment_id in equipment_id_list: 
        query2 = text(f''' insert into ems_v1.master_holiday_meter (ref_id, company_id, bu_id,equipment_id, plant_id, plant_department_id, equipment_group_id)
                    values ({insert_id},'{company_id}','{bu_id}','{equipment_id}','{plant_id}','{plant_department_id}','{equipment_group_id}')''')
        createFolder("Holiday_Log/", "master_holiday_meter" + str(query2)) 
        await cnx.execute(query2)
        await cnx.commit()

    obj_data = json.loads(obj)

    if obj !="":
        for row in obj_data:
            holiday_date = row["holiday_date"]
            description = row["description"]
            holiday_type = row["holiday_type"]
            holiday_date = '-'.join(reversed(holiday_date.split('-')))

            query3 = text(f'''INSERT INTO ems_v1.master_holiday_date 
                            (ref_id,holiday_date,description,holiday_type)
                            values({insert_id},'{holiday_date}','{description}','{holiday_type}')''')
            createFolder("Holiday_Log/", "master_holiday_date" + str(query3)) 
            await cnx.execute(query3)
            await cnx.commit()

    if obj2 !="":
        obj_data2 = json.loads(obj2)

        for row in obj_data2:
            holiday_date = row["holiday_date"]
            print("holiday_date",holiday_date)
            Weekend_day = row["Weekend_day"]
            holiday_type = row["holiday_type"]
            holiday_date = '-'.join(reversed(holiday_date.split('-')))
        
            query3 = text(f'''INSERT INTO ems_v1.master_holiday_date 
                            (ref_id,holiday_date,holiday_type,Weekend_day)
                            values({insert_id},'{holiday_date}','{holiday_type}','{Weekend_day}')''')
            await cnx.execute(query3)
            await cnx.commit()

    return insert_id
    
async def upadte_holiday_dtl(cnx, year ,company_id,bu_id,equipment_id ,plant_id ,plant_department_id ,equipment_group_id ,obj, obj2,id ,user_login_id):

    if company_id == None:
        company_id = ''

    if bu_id == None:
        bu_id = ''

    if plant_id == None:
        plant_id = ''

    if plant_department_id == None:
        plant_department_id = ''

    if equipment_group_id == None:
        equipment_group_id = ''

    del_query1=text(f'''DELETE FROM ems_v1.master_holiday_meter  where ref_id = '{id}' ''')
    del_query2=text(f'''DELETE FROM ems_v1.master_holiday_date  where ref_id = '{id}' ''')
    await cnx.execute(del_query1)
    await cnx.execute(del_query2)
    await cnx.commit()

    query = text(f''' update ems_v1.master_holiday
                        set
                        holiday_year = '{year}',
                        modified_on = now(),
                        modified_by = '{user_login_id}' 
                        where id = {id}''')
    createFolder("Holiday_Log/", "master_holiday" + str(query)) 
    await cnx.execute(query)
    await cnx.commit()

    equipment_id_list = equipment_id.split(",")  
    for equipment_id in equipment_id_list: 
        query2 = text(f''' insert into ems_v1.master_holiday_meter (ref_id, company_id,bu_id,equipment_id, plant_id, plant_department_id, equipment_group_id)
                    values ({id},'{company_id}','{bu_id}','{equipment_id}','{plant_id}','{plant_department_id}','{equipment_group_id}')''')
        createFolder("Holiday_Log/", "master_holiday_meter" + str(query2)) 
        await cnx.execute(query2)
        await cnx.commit()

    obj_data = json.loads(obj)

    if obj !="":
        for row in obj_data:
            holiday_date = row["holiday_date"]
            description = row["description"]
            holiday_type = row["holiday_type"]
            holiday_date = '-'.join(reversed(holiday_date.split('-')))
            print("holiday_date",holiday_date)

            query3 = text(f'''
                        insert into  ems_v1.master_holiday_date
                        (holiday_date,description,holiday_type,ref_id)
                        values('{holiday_date}','{description}','{holiday_type}','{id}')
                             ''')
            createFolder("Holiday_Log/", "master_holiday_date" + str(query3)) 
            await cnx.execute(query3)
            await cnx.commit()

    if obj2 !="":
        obj_data2 = json.loads(obj2)
        for row in obj_data2:
            holiday_date = row["holiday_date"]
            holiday_type = row["holiday_type"]
            Weekend_day = row["Weekend_day"]
            holiday_date = '-'.join(reversed(holiday_date.split('-')))

            query3 = text(f'''
                        insert into  ems_v1.master_holiday_date
                        (holiday_date,holiday_type,ref_idWeekend_day)
                        values('{holiday_date}','{holiday_type}','{id}','{Weekend_day}')
                             ''')
            await cnx.execute(query3)
            await cnx.commit()

async def upadte_holidaystatus(cnx, id, status):

    if status !='':
        query = text(f" UPDATE master_holiday SET status = '{status}' WHERE id = '{id}' ")
        await cnx.execute(query)
    else:
        query = text(f" UPDATE master_holiday SET status = 'delete' WHERE id = '{id}' ")                
        await cnx.execute(query)

        del_query1=text(f'''DELETE FROM master_holiday_meter  where ref_id = '{id}' ''')
        del_query2=text(f'''DELETE FROM master_holiday_date  where ref_id = '{id}' ''')
        await cnx.execute(del_query1)
        await cnx.execute(del_query2)

    await cnx.commit()
    createFolder("Holiday_Log/","query execute sucessfully")
