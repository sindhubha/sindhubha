from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image
import json
from src.models.mysql.master_meter_model import meter_Lists
from log_file import createFolder
import re

async def meter_mapping_list(cnx, id,campus_id,plant_id,equipment_id):

    where = '' 
    if id !='':
        where = f" and mec.id = {id}"   

    if equipment_id !='' and equipment_id != 0:
        where = f" and mec.equipment_id = {equipment_id}"   

    if plant_id !='' and plant_id != "0":
        where = f" and mp.plant_id = {plant_id}"  
         
    if campus_id !='' and campus_id != "0":
        where = f" and mp.campus_id = {campus_id}"   
    
    query = text(f"""
        SELECT                
            mec.*,
            mc.company_code,
            mc.company_name,
            mb.bu_code,
            mb.bu_name,
            mp.plant_code,
            mp.plant_name,
            pd.plant_department_code,
            pd.plant_department_name,
            mec.equipment_id,
            GROUP_CONCAT(mem.meter_id SEPARATOR ',') AS meter_ids, 
            CASE WHEN mm.is_poll_meter = 'yes' then mm.meter_id ELSE '' end as first_meter_id, 
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        from 
            ems_v1.master_equipment_calculations mec
            left join ems_v1.master_employee cu on cu.employee_id=mec.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=mec.modified_by
            inner JOIN ems_v1.master_equipment_meter mem ON mem.equipment_id = mec.equipment_id  
            inner join master_meter mm on mm.meter_id = mem.meter_id 
            INNER JOIN ems_v1.master_company mc ON mc.company_id = mec.company_id
            INNER JOIN ems_v1.master_business_unit mb ON mb.bu_id = mec.bu_id
            INNER JOIN ems_v1.master_plant mp ON mp.plant_id = mec.plant_id
            INNER JOIN ems_v1.master_plant_wise_department pd ON pd.plant_department_id = mec.plant_department_id 
        where mec.status != 'delete' {where}
        group  by mec.id
    """)
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data

async def getmeterdtl(cnx,meter_ids,formula):
    balance_meters = ''
    first_operator = ''
    meters = meter_ids.split(',')
    first_meter = int(meters[0])
    for char in formula:
        if char in ['+', '-']:
            # Found the first operator
            first_operator = char
            break

    balance_meters = ','.join(str(meter) for meter in meters[1:] )
    createFolder("Log/",f"balance_meters-{balance_meters}")
    equipment_id = 0
    res = await meter_Lists(cnx,'','',first_meter,'','','','','','','','','','','','','','','','','','')
    print(res)
    if len(res)>0:
        for row in res["data"]:
            equipment_id = row["equipment_id"]
            print(equipment_id)
    if equipment_id == 0:
        equipment_id = first_meter
        meter_communication = 'common'
        poll_meter_id = first_meter
    else:
        meter_communication = 'equipment'
        poll_meter_id = first_meter
    result = {"equipment_id":equipment_id, "meter_communication":meter_communication,"poll_meter_id":poll_meter_id,"balance_meters":balance_meters,"first_meter":first_meter}
    return result

def extract_no_poll_meters(formula):
    
    meter_codes = re.findall(r'\bdict\[(\d+)\]', formula)
    
    # Find meter codes inside parentheses
    inside_parentheses = re.findall(r'\((.*?)\)', formula)
    inside_parentheses_codes = [code for sublist in inside_parentheses for code in re.findall(r'\bdict\[(\d+)\]', sublist)]
    
    # Find meter codes outside of parentheses and subtraction operations
    no_poll_meters = [code for code in meter_codes if code not in inside_parentheses_codes and '+' in formula]


    return no_poll_meters

async def poll_meter(cnx,meter_ids,formula):
    npollmeter = ''
    query_c = f"select * from master_equipment_meter where meter_id in ({meter_ids}) and meter_communication = 'common'"
    createFolder("Log/",f"query_c-{query_c}")
    c_meter = await cnx.execute(query_c)
    c_meter = c_meter.fetchall()
    if len(c_meter) > 0:
        bc_meter = [meter['meter_id'] for meter in c_meter]
        meter_ids = meter_ids.split(',')
        npollmeter = ','.join(str(meter) for meter in meter_ids if str(meter) not in bc_meter)
    else:
        meter_ids = meter_ids.split(',')
        npollmeter = ','.join(str(meter) for meter in meter_ids)
    createFolder("Log/",f"npollmeter-----{npollmeter}")
    return npollmeter

async def save_metermapping(cnx,company_id,bu_id,plant_id,plant_department_id,parameter,meter,user_login_id):
    
    data = json.loads(meter)
    createFolder("Log/",f"{data}")
    if len(data) > 0:
        for record in data:
           formula1 =  record["formula1"]
           formula2 =  record["formula2"]
           meter_ids =  record["meter_ids"]
           print("meter_ids",meter_ids)
           res = await getmeterdtl(cnx,meter_ids,formula1)
           no_m = extract_no_poll_meters(formula2)
           no_m = ",".join(no_m) if no_m else ''
           equipment_id = res["equipment_id"]
           meter_communication = res["meter_communication"]
           first_meter = res["first_meter"]
                          

           sql = f'''insert into master_equipment_calculations (company_id,bu_id,plant_id,plant_department_id,equipment_id,formula1,formula2,parameter,created_on,created_by,meter_communication)
                    values('{company_id}','{bu_id}','{plant_id}','{plant_department_id}','{equipment_id}','{formula1}','{formula2}','{parameter}',now(),{user_login_id},'{meter_communication}')'''
           await cnx.execute(sql)
           await cnx.commit()
        
           meter_id_list = meter_ids.split(",")  
           for meter_id in meter_id_list: 
                query = text(f'''insert into master_equipment_meter(equipment_id,meter_id,meter_communication)
                                  values({equipment_id},{meter_id},'{meter_communication}') ''')
                await cnx.execute(query)
                await cnx.commit()

        if len(no_m) != 0:
            sql = text(f" update master_meter set is_poll_meter = 'no' where meter_id in ({no_m})")
            createFolder("Log/",f"npollmeter-----{sql}")
            await cnx.execute(sql)
            await cnx.commit()

        sql = text(f" update master_meter set is_poll_meter = 'yes' where meter_id = {first_meter}")
        createFolder("Log/",f"ypollmeter-----{sql}")
        await cnx.execute(sql)
        await cnx.commit()
  
async def update_metermapping(cnx,id,company_id,bu_id,plant_id,plant_department_id,parameter,meter,user_login_id):
    data = json.loads(meter)
    if len(data) > 0:
        for record in data:
            formula1 =  record["formula1"]
            formula2 =  record["formula2"]
            meter_ids =  record["meter_ids"]
            res = await getmeterdtl(cnx,meter_ids)
            res = await getmeterdtl(cnx,meter_ids,formula1)
            no_m = extract_no_poll_meters(formula2)
            no_m = ",".join(no_m) if no_m else ''
            for row in res:
                equipment_id = row["equipment_id"]
                meter_communication = row["meter_communication"]
                first_meter = row["first_meter"]
            query = text(f" update master_meter set is_poll_meter = 'yes' where meter_id in({meter_ids}) ")
            await cnx.execute(query)
            await cnx.commit()

            sql1 = f" delete from master_equipment_meter where equipment_id = {equipment_id} and meter_communication = {meter_communication}"
            await cnx.execute(sql1)
            await cnx.commit() 

            sql2 = f'''update  master_equipment_calculations 
                    set 
                        equipment_id = {equipment_id},
                        formula1 = '{formula1}',
                        formula2 = '{formula2}',
                        parameter = '{parameter}',
                        company_id = '{company_id}',
                        bu_id = '{bu_id}',
                        plant_id = '{plant_id}',
                        plant_department_id = '{plant_department_id}',
                        modified_on = now(),
                        modified_by = {user_login_id}
                        meter_communication  = '{meter_communication}' 
                    where id = {id}'''
            await cnx.execute(sql2)
            await cnx.commit()

            meter_id_list = meter_ids.split(",")  
            for meter_id in meter_id_list: 
                sql3 = text(f'''insert into master_equipment_meter(equipment_id,meter_id,meter_communication)
                                values({equipment_id},{meter_id},'{meter_communication}') ''')
                await cnx.execute(sql3)
                await cnx.commit()
                
        if len(no_m) >= 0:
            sql = f" update master_meter set is_poll_meter = 'no' where meter_id in ({no_m})"
            await cnx.execute(sql)
            await cnx.commit()
        
        sql = text(f" update master_meter set is_poll_meter = 'yes' where meter_id = {first_meter}")
        createFolder("Log/",f"ypollmeter-----{sql}")
        await cnx.execute(sql)
        await cnx.commit()
    
async def update_metermappingStatus(cnx,id,status):
    
    if status != '':
        query=f''' Update ems_v1.master_equipment_calculations Set status = '{status}' Where id='{id}' '''
        await cnx.execute(text(query))
        await cnx.commit()
        
    else: 
        balance_meters = ''
        res = await get_metermappingdtl(cnx,id)
        for row in res["results"]:
            equipment_id = row["equipment_id"]
            meter_communication = row["meter_communication"] 
        balance_meters = res["balance_meters"]

        query=f''' Update ems_v1.master_equipment_calculations Set status = 'delete' Where id='{id}' '''
        await cnx.execute(text(query))
        await cnx.commit()

        sql = f" delete from ems_V1.master_equipment_meter where equipment_id = '{equipment_id}' and meter_communication = '{meter_communication}'"
        await cnx.execute(text(sql))
        await cnx.commit() 

        query=f''' Update ems_v1.master_meter Set is_poll_meter = 'yes' Where meter_id in ('{balance_meters}') '''
        createFolder("Log/",f"poll_meter updata -----{query}")
        await cnx.execute(text(query))
        await cnx.commit()   

async def get_metermappingdtl(cnx,id):
    where = ''
    meter_communication = ''
    equipment_id = ''
    balance_meters = ''
    sql = f"select * from master_equipment_calculations where  status != 'delete'  and id = '{id}'"
    data = await cnx.execute(sql)
    data = data.fetchall()

    for row in data :
        meter_communication = row["meter_communication"]
        equipment_id = row["equipment_id"]

    if len(data)>0:
        sql2 = f"select * from master_equipment_meter where meter_communication ='{meter_communication}' and equipment_id = '{equipment_id}' "
        data2 = await cnx.execute(sql2)
        data2 = data2.fetchall()
        if len(data2)>0:
            bc_meter = [meter['meter_id'] for meter in data2]
            meter_ids = ','.join(str(meter) for meter in bc_meter)
    

        res = await getmeterdtl(cnx,meter_ids,'')
        
        equipment_id = res["equipment_id"]
        meter_communication = res["meter_communication"]
        balance_meters = res["balance_meters"]

    result = {"results":data, "balance_meters":balance_meters}
    
    return result

async def check_metermappingdtl(cnx,meter,id):
    where = ''
    meter_communication = ''
    equipment_id = ''
    if meter!= '':
        data = json.loads(meter)
        print("date")
        if len(data) > 0:
            for record in data:
                meter_ids =  record["meter_ids"]
            res = await getmeterdtl(cnx,meter_ids,'')
            equipment_id = res["equipment_id"]
            meter_communication = res["meter_communication"]

    if equipment_id!= '':
        where +=f" and equipment_id = '{equipment_id}'"

    if meter_communication!= '':
        where +=f" and meter_communication = '{meter_communication}'"

    if id != '':
        where +=f" and id = {id}"

    query=text(f''' select * from master_equipment_calculations Where status != 'delete' {where}  ''')
    print("query")
    results = await cnx.execute(query)
    results = results.fetchall()
    return results