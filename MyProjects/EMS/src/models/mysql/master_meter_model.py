from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder
from datetime import date

async def meter_Lists(cnx, company_id,bu_id,meter_id,type_value,type_id,is_critical,model_name,plant_id,plant_department_id,function_id,function2_id,holiday,selected,campus_id,meter,equipment_id,equipment_group_id,meter_type,selected_meter,ip_address,source):
    
        if type_id != '':
            value = type_id.split(",")
            if len(value) > 1:
                values = tuple(value)
                type_id = ",".join(values)
            else:
                type_id = value[0]

        data2 = ''      
        where = ""
        where1 = ""
        
        if company_id != '' and company_id != 0:
            where += f" and mm.company_id = {company_id}" 
            where1 += f" and mm.company_id = {company_id}" 

        if bu_id != '' and bu_id != "0":
            where += f" and mm.bu_id = {bu_id}" 
            where1 += f" and mm.bu_id = {bu_id}" 

        if meter_id != '' and meter_id != "0":
            where += f" and mm.meter_id = {meter_id}"  
            where1 += f" and mm.meter_id = {meter_id}" 

        if type_value != '' and type_id != '':
            if type_value == 'plant':
                where += f" and mm.plant_id in ({','.join(str(x) for x in value)})"
          
            elif type_value == 'plant_department':
                where += f" and mm.plant_department_id in ({','.join(str(x) for x in value)})"

            elif type_value == 'function':
                where += f" and mm.function_id in ({','.join(str(x) for x in value)})"
                
            elif type_value == 'function_1':
                where += f" and mm.function_id in ({','.join(str(x) for x in value)})"

            elif type_value == 'function_2':
                where += f" and mm.function2_id in ({','.join(str(x) for x in value)})"
                
            elif type_value == 'campus':
                where += f" and c.campus_id in ({','.join(str(x) for x in value)})"
                
        if is_critical == "yes" or is_critical == "no"  :
            where += f" and mm.major_nonmajor = '{is_critical}' "   
            where1 += f" and mm.major_nonmajor = '{is_critical}' "   
        
        if model_name != '':
            where += f" and mm.model_name"  
            where1 += f" and mm.model_name"  
        
        if plant_id != '' and plant_id != "0":
            where += f" and mm.plant_id = {plant_id}"
            where1 += f" and mm.plant_id = {plant_id}"
            
        if plant_department_id != '' and plant_department_id != "0":
            where +=f" and mm.plant_department_id = {plant_department_id}"
            where1 +=f" and mm.plant_department_id = {plant_department_id}"
            
        if function_id != '' and function_id != 0:
            where += f" and mm.function_id = {function_id}"                         
            where1 += f" and mm.function_id = {function_id}"                         
        
        if function2_id !='' and function2_id != "0":
            where += f" and mm.function2_id = {function2_id}"   
            where1 += f" and mm.function2_id = {function2_id}"   

        if campus_id !='' and campus_id != "0":
            where += f" and mm.campus_id = {campus_id}"   
            where1 += f" and mm.campus_id = {campus_id}" 
        
        if ip_address !='' and ip_address != "0":
            where += f" and mm.ip_address = '{ip_address}'"   
           
        if selected_meter != '' and selected_meter != None:
            where += f" and mm.meter_id <> '{selected_meter}'"

        if meter!= '' :
            where += f" and mm.meter = '{meter}'"  
            
        if equipment_id!= '' :
            where += f" and mm.equipment_id = '{equipment_id}'" 

        if equipment_group_id!= '' :
            where += f" and mm.equipment_group_id = '{equipment_group_id}'"  
        
        if meter_type!='':
            where += f" and mm.meter_type = '{meter_type}'"

        if source!='':
            where += f" and mm.source = '{source}'"
            
        if holiday !='':
            query1 = text(f'''
                        SELECT 
                            min(mhm.meter_id) as meter_id
                        FROM 
                            ems_v1.master_holiday mh
                        INNER JOIN ems_v1.master_holiday_meter mhm ON mh.id = mhm.ref_id
                        WHERE mh.status = 'active' and mh.holiday_year = '{holiday}'
                        GROUP BY mhm.meter_id 
                          ''') 
            data2 = await cnx.execute(query1)
            data2 = data2.fetchall()
            createFolder("Log/","Issue in returning data "+str(data2))
            meter_id= []
            if len(data2)>0:
                for record in data2:
                    meter_id.append(record["meter_id"]) 
                        
                where += f" and mm.meter_id  not in ({','.join(str(x) for x in meter_id)})"                      
                if selected != '':
                    where1 += f" and mm.meter_id  in ({','.join(str(x) for x in meter_id)})"
                    sql  = text(f"""
                            SELECT 
                                mm.meter_code,
                                mm.meter_name,
                                mm.meter_id
                            FROM 
                                ems_v1.master_meter mm
                                INNER JOIN master_company mc ON mm.company_id = mc.company_id
                                INNER JOIN master_business_unit mb ON mm.bu_id = mb.bu_id
                                INNER JOIN master_plant md ON mm.plant_id = md.plant_id
                                INNER JOIN master_plant_wise_department ms ON mm.plant_department_id = ms.plant_department_id
                                INNER JOIN master_converter_detail mcd ON mm.converter_id = mcd.converter_id
                                LEFT JOIN ems_v1.master_function mf ON mm.function_id = mf.function_id
                                LEFT JOIN ems_v1.master_function mff ON mm.function2_id = mff.function_id
                                inner join ems_v1.master_campus c on c.campus_id = mm.campus_id
                            WHERE 
                                mm.status != 'delete' and c.status = 'active'  {where1}  """)
                    print(sql)
                    data2 = await cnx.execute(sql)
                    data2 = data2.fetchall()

        query = text(f"""
                    SELECT mm.*,
                        mc.company_code,
                        mc.company_name,
                        mb.bu_code,
                        mb.bu_name,
                        c.campus_code,
                        c.campus_name,
                        md.plant_code,
                        md.plant_name,
                        ms.plant_department_code,
                        ms.plant_department_name,
                        mcd.converter_name,
                        mf.function_code,
                        mf.function_name,
                        mmm.model_make_name,
                        mmm.model_make_id,
                        mcm.converter_model_name,
                        mcm.converter_model_id,
                        mmk.converter_make_name,
                        mmk.converter_make_id,
                        IFNULL(meg.equipment_group_code,'')as equipment_group_code,
                        IFNULL(meg.equipment_group_name,'')as equipment_group_name,
                        IFNULL(me.equipment_code,'')equipment_code,
                        IFNULL(me.equipment_name,'')equipment_name,
                        IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                    IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                    FROM 
                        master_meter mm
                        left join master_employee cu on cu.employee_id=mm.created_by
	                    left join master_employee mu on mu.employee_id=mm.modified_by                    
                        INNER JOIN master_company mc ON mm.company_id = mc.company_id
                        INNER JOIN master_business_unit mb ON mm.bu_id = mb.bu_id
                        INNER JOIN master_plant md ON mm.plant_id = md.plant_id
                        INNER JOIN master_plant_wise_department ms ON mm.plant_department_id = ms.plant_department_id
                        INNER JOIN master_converter_detail mcd ON mm.converter_id = mcd.converter_id
                        LEFT JOIN ems_v1.master_function mf ON mm.function_id = mf.function_id
                        LEFT JOIN ems_v1.master_function mff ON mm.function2_id = mff.function_id
                        INNER JOIN ems_v1.master_campus c on c.campus_id = mm.campus_id
                        LEFT JOIN master_equipment_group meg on meg.equipment_group_id = mm.equipment_group_id
                        LEFT JOIN master_equipment me on me.equipment_id = mm.equipment_id
                        inner join master_model m on m.model_id = mm.model_name
                        inner join master_model_make mmm on mmm.model_make_id = m.model_make_id
                        left join master_converter_model mcm on mcm.converter_model_id = mcd. converter_model_id
                        left join master_converter_make mmk on mmk.converter_make_id = mcm. converter_make_id    
                    WHERE 
                        mm.status != 'delete' and c.status = 'active' {where}""")
        data = await cnx.execute(query)
        data = data.fetchall()
        print(query)
        return {"data":data,"data2":data2}
    
async def getmeterdtl(cnx,  meter_code):
    where=""
      
    query=f'''select * from ems_v1.master_meter where 1=1 and status<>'delete' and meter_code='{meter_code}' {where}'''

    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def save_meter(cnx, company_id,bu_id,plant_id,plant_department_id,campus_id,function_id,converter_id,meter_id,meter_code,meter_name,ip_address,port,major_nonmajor,model_name,IMEI,user_login_id,function2_id,address,parameter, sub_parameter,meter_type,source,physical_location,max_demand,max_pf,main_demand_meter,consumption_type,is_poll_meter,meter,equipment_group_id,equipment_id,mac):

        query = text(f"""
                INSERT INTO ems_v1.master_meter (
                    company_id,  bu_id, plant_id, plant_department_id,equipment_group_id,equipment_id,  meter_code, meter_name, address,
                    ip_address, port, created_on, created_by,  parameter, sub_parameter, major_nonmajor,  model_name, converter_id,function_id,
                    IMEI, mac,function2_id, meter_type,source,  physical_location,campus_id,max_pf,max_demand,consumption_type,meter 
                    ,is_poll_meter,main_demand_meter
    
                )
                VALUES (
                    {company_id},{bu_id}, {plant_id}, {plant_department_id} ,'{equipment_group_id}','{equipment_id}', '{meter_code}', '{meter_name}','{address}',
                    '{ip_address}' ,{port}, NOW(), {user_login_id} ,'{parameter}','{sub_parameter}', '{major_nonmajor}', '{model_name}',{converter_id}, '{function_id}',
                    '{IMEI}','{mac}','{function2_id}','{meter_type}','{source}','{physical_location}',{campus_id},'{max_pf}' ,'{max_demand}','{consumption_type}','{meter}',
                    '{is_poll_meter}','{main_demand_meter}')
            """)
        await cnx.execute(query)  
        result = await cnx.execute("SELECT LAST_INSERT_ID()")
        insert_id =  result.first()[0]
        await cnx.commit()

        if insert_id !='': 
            query1 = text(f'''select * from ems_v1.master_meter_factor where meter_id = {insert_id}''')
            record = await cnx.execute(query1)
            record = record.fetchall()
            if len(record) == 0:
                query2 = text(f'''insert into  ems_v1.master_meter_factor 
                (meter_id,plant_id,created_on,created_by)
                values({insert_id},{plant_id},now(),'{user_login_id}')

                ''')
                await cnx.execute(query2)
                await cnx.commit()

             
            sql = text(f''' select * from ems_v1.current_power where meter_id = {insert_id}''') 
            data = await cnx.execute(sql)
            data = data.fetchall()

            if len(data)==0:                       
                sql1 = text(f"select * from ems_v1.master_meter where meter_id = {insert_id}")

                data1 = await cnx.execute(sql1)
                data1 = data1.fetchall()
                for row in data1:
                    meter_id = row["meter_id"]
                    company_id = row["company_id"]
                    bu_id = row["bu_id"]
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    

                sql2= text(f" select * from ems_v1.master_shifts  where company_id = {company_id} and bu_id = {bu_id} and plant_id = {plant_id} AND status = 'active' ")
                data2 = await cnx.execute(sql2)
                data2 = data2.fetchall()

                mill_date = date.today()
                mill_shift = 1
                
                if len(data2)>0:
                    for row in data2:
                        mill_date = row["mill_date"]
                        mill_shift = row["mill_shift"]  

                if mill_date != '' and mill_shift != '':      
                    sql3 = text(f'''
                                INSERT INTO ems_v1.current_power (meter_id, date_time, date_time1,
                                mill_date, mill_shift,company_id, bu_id, plant_id, plant_department_id)
                                VALUES ({meter_id}, NOW(), NOW(), '{mill_date}', '{mill_shift}',{company_id},
                                {bu_id}, {plant_id}, {plant_department_id})
                                ''')  
                    await cnx.execute(sql3)
                    await cnx.commit()
                    createFolder("Log/"," current power" +str(sql3))

                    query4=f''' update ems_v1.master_meter set meter_order=meter_id where meter_id= '{insert_id}' '''
                    await cnx.execute(text(query4))
                    await cnx.commit()
        
        return insert_id
    
async def update_meter(cnx, company_id,bu_id,plant_id,plant_department_id,campus_id,function_id,converter_id,meter_id,meter_code,meter_name,ip_address,port,major_nonmajor,model_name,IMEI,user_login_id,function2_id,address,parameter, sub_parameter,meter_type,source,physical_location,max_demand,max_pf,main_demand_meter,consumption_type,is_poll_meter,meter,equipment_group_id,equipment_id,mac):
    
        sql = text(f'''INSERT INTO ems_v1.master_meter_history (
                    company_id, meter_name, meter_code, bu_id, plant_id, plant_department_id, converter_id, function_id,campus_id,
                    ip_address, port, modified_on, modified_by, major_nonmajor, model_name, IMEI,
                    function2_id,address,parameter, sub_parameter,meter_type,source,physical_location,max_demand,max_pf,main_demand_meter,consumption_type,is_poll_meter,meter,equipment_group_id,equipment_id,mac
                )
                VALUES (
                    {company_id},'{meter_name}', '{meter_code}', {bu_id}, {plant_id}, {plant_department_id}, {converter_id}, '{function_id}',
                    {campus_id}, '{ip_address}',{port}, now(), {user_login_id}, '{major_nonmajor}', '{model_name}','{IMEI}',
                    '{function2_id}', '{address}','{parameter}','{sub_parameter}','{meter_type}','{source}',
                    '{physical_location}','{max_demand}','{max_pf}','{main_demand_meter}','{consumption_type}','{is_poll_meter}','{meter}','{equipment_group_id}','{equipment_id}','{mac}'
                )  
                ''')
            
        await cnx.execute(sql)
        await cnx.commit() 

        query =text(f"""
                UPDATE ems_v1.master_meter
                SET company_id = {company_id}, campus_id = {campus_id}, meter_code = '{meter_code}',meter_name = '{meter_name}',
                bu_id = {bu_id}, plant_department_id = {plant_department_id},converter_id = {converter_id}, plant_id = {plant_id},function_id = '{function_id}',
                ip_address = '{ip_address}', port = {port}, modified_on = NOW(), modified_by = {user_login_id}, 
                major_nonmajor = '{major_nonmajor}', model_name = '{model_name}',  IMEI = '{IMEI}',function2_id='{function2_id}'
                ,address = '{address}', parameter = '{parameter}', sub_parameter = '{sub_parameter}', meter_type = '{meter_type}', source = '{source}',
                physical_location = '{physical_location}',max_demand = '{max_demand}',max_pf = '{max_pf}', main_demand_meter = '{main_demand_meter}',consumption_type = '{consumption_type}',
                is_poll_meter = '{is_poll_meter}', meter = '{meter}',equipment_group_id = '{equipment_group_id}',equipment_id ='{equipment_id}',mac = '{mac}'
                WHERE meter_id = '{meter_id}'
            """) 
        
        await cnx.execute(query)
        await cnx.commit()
        
        sql = text(f"update master_meter_factor set plant_id = '{plant_id}' where meter_id = {meter_id}")
        await cnx.execute(sql)
        await cnx.commit()

        sql = text(f''' select * from ems_v1.current_power where meter_id = {meter_id}''') 
        data = await cnx.execute(sql)
        data = data.fetchall()
                              
        sql1 = text(f"select * from ems_v1.master_meter where meter_id = {meter_id}")
        data1 = await cnx.execute(sql1)
        data1 = data1.fetchall()

        for row in data1:
            meter_id = row["meter_id"]
            company_id = row["company_id"]
            bu_id = row["bu_id"]
            plant_id = row["plant_id"]
            plant_department_id = row["plant_department_id"]
            campus_id = row["campus_id"]  

        sql2= text(f" select * from ems_v1.master_shifts  where company_id = {company_id} and bu_id = {bu_id} and plant_id = {plant_id} AND status = 'active' ")
        data2 = await cnx.execute(sql2)
        data2 = data2.fetchall()

        mill_date = date.today()
        mill_shift = 1

        
        if len(data2)>0:
            for row in data2:
                mill_date = row["mill_date"]
                mill_shift = row["mill_shift"] 

        if len(data)==0: 
            if mill_date != '' and mill_shift != '':             
                sql3 = text(f'''
                            INSERT INTO ems_v1.current_power (meter_id, date_time, date_time1,
                            mill_date, mill_shift,company_id, bu_id, plant_id, plant_department_id, )
                            VALUES ({meter_id}, NOW(), NOW(), '{mill_date}', '{mill_shift}',{company_id},
                            {bu_id}, {plant_id}, {plant_department_id})
                            ''')  
                await cnx.execute(sql3)
                await cnx.commit()
                createFolder("Log/"," current power" +str(sql3))

                # query4=f''' update ems_v1.master_meter set meter_order=meter_id where meter_id='{meter_id}' '''
                # await cnx.execute(text(query4))
                # await cnx.commit()
        
        else:
            sql3 = text(f'''update current_power  
                        set mill_date = '{mill_date}',
                        mill_shift = '{mill_shift}',
                        company_id = '{company_id}',
                        bu_id = '{bu_id}',
                        plant_id = '{plant_id}',
                        plant_department_id = '{plant_department_id}'  where meter_id = '{meter_id}' ''')
            
            await cnx.execute(sql3)
            await cnx.commit()
        
async def update_meterStatus(cnx, meter_id, status):

    if status != '':
        query=f''' Update ems_v1.master_meter Set status = '{status}' Where meter_id='{meter_id}' '''
        await cnx.execute(text(query))
        await cnx.commit()

    else: 
        query=f''' Update ems_v1.master_meter Set status = 'delete' Where meter_id='{meter_id}' '''
        await cnx.execute(text(query))
        await cnx.commit()
        
        sql = f" Delete from ems_v1.current_power where meter_id = '{meter_id}' "
        await cnx.execute(text(sql))
        await cnx.commit()
      
    
    
async def meter_historylist(cnx,meter_id,company_id,plant_id,plant_department_id,campus_id):

        where = ''
        if meter_id !=''and meter_id !='all' and meter_id !='0':
            where = f' and mh.meter_id = {meter_id}'

        if company_id != '' and company_id !='0':
            where = f' and mh.company_id = {company_id}'

        if plant_id != ''and plant_id != "0":
            where = f' and mh.plant_id = {plant_id}'

        if plant_department_id != '' and plant_department_id!='0':
            where = f' and mh.plant_department_id = {plant_department_id}'

        if campus_id != '' and campus_id != "0":
            where = f' and c.campus_id = {campus_id}'

        query = text(f'''
                    SELECT 
                        mc.company_code ,
                        mc.company_name ,
                        mb.bu_code ,
                        mb.bu_name ,
                        md.plant_code ,
                        md.plant_name ,
                        ms.plant_department_code ,
                        ms.plant_department_name ,
                        c.campus_code ,                        
                        c.campus_name ,
                        mf.function_code ,
                        mf.function_name ,
                        mcd.converter_name ,
                        IFNULL(meg.equipment_group_code,'')as equipment_group_code,
                        IFNULL(meg.equipment_group_name,'')as equipment_group_name,
                        IFNULL(me.equipment_code,'')equipment_code,
                        IFNULL(me.equipment_name,'')equipment_name,
                        mh.*,
                        IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                    IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                    FROM 
                        ems_v1.master_meter_history mh
                        left join ems_v1.master_employee cu on cu.employee_id=mh.created_by
	                    left join ems_v1.master_employee mu on mu.employee_id=mh.modified_by
                        INNER JOIN ems_v1.master_company mc ON mh.company_id = mc.company_id
                        INNER JOIN ems_v1.master_business_unit mb ON mh.bu_id = mb.bu_id
                        INNER JOIN ems_v1.master_plant md ON mh.plant_id = md.plant_id
                        INNER JOIN ems_v1.master_plant_wise_department ms ON mh.plant_department_id = ms.plant_department_id
                        INNER JOIN ems_v1.master_function mf ON mh.function_id = mf.function_id
                        INNER JOIN ems_v1.master_converter_detail mcd ON mh.converter_id = mcd.converter_id   
                        Left Join ems_v1.master_campus c on c.campus_id = mh.campus_id 
                        LEFT JOIN master_equipment_group meg on meg.equipment_group_id = mh.equipment_group_id
                        LEFT JOIN master_equipment me on me.equipment_id = mh.equipment_id               
                    WHERE 1=1  {where}''')
        data = await cnx.execute(query)
        data = data.fetchall()
    
        return data
   
async def check_import_meter(cnx):
    query = f'''         
        SELECT 
            mmt.meter_name,
            mmt.meter_code,
            mc.company_name,
            mc.company_code,
            bu.bu_name,
            bu.bu_code,
            md.plant_name,
            md.plant_code,
            ms.plant_department_name,
            ms.plant_department_code,
            mcd.converter_name,
            meg.equipment_group_name,
            meg.equipment_group_code,
            me.equipment_name,
            me.equipment_code,
            CASE
                WHEN mm.meter_code IS NULL THEN 'New'
                WHEN mm.status = 'delete' THEN 'Deleted'
                ELSE
                    CASE
                        WHEN
                            mmt.converter_id <> mm.converter_id
                            OR mmt.plant_department_id <> mm.plant_department_id
                            OR mmt.model_name <> mm.model_name
                            OR mmt.physical_location <> mm.physical_location
                            OR mmt.source <> mm.source
                            OR mmt.meter_type <> mm.meter_type
                            OR mmt.address <> mm.address    
                            OR mmt.meter_code <> mm.meter_code    
                            OR mmt.meter_name <> mm.meter_name    
                            OR mmt.IMEI <> mm.IMEI    
                            OR mmt.major_nonmajor <> mm.major_nonmajor    
                            
                        THEN 'Updated'
                        ELSE 'Already Exist'
                    END
            END AS change_status,
            CASE
                WHEN mmt.converter_id <> mm.converter_id THEN 'Converter Name'
                WHEN mmt.plant_department_id <> mm.plant_department_id THEN 'Department'
                WHEN mmt.model_name <> mm.model_name THEN 'Model'
                WHEN mmt.physical_location <> mm.physical_location THEN 'Physical Location'
                WHEN mmt.source <> mm.source THEN 'Source'
                WHEN mmt.meter_type <> mm.meter_type THEN 'Meter Type'
                WHEN mmt.address <> mm.address  THEN 'Slave id'
                WHEN mmt.meter_name <> mm.meter_name   THEN 'Meter Name'
                WHEN mmt.IMEI <> mm.IMEI  THEN 'IMEI'
                WHEN mmt.major_nonmajor <> mm.major_nonmajor  THEN 'Is Critical'
                ELSE 'No Changes'
            END AS changed_column,
            mmt.*
        FROM master_meter_temp mmt
        LEFT JOIN master_meter mm ON mm.meter_code = mmt.meter_code
        LEFT JOIN master_company mc ON mc.company_id = mmt.company_id
        LEFT JOIN master_business_unit bu ON bu.bu_id = mmt.bu_id
        LEFT JOIN master_plant md ON md.plant_id = mmt.plant_id
        LEFT JOIN master_plant_wise_department ms ON ms.plant_department_id = mmt.plant_department_id
        LEFT JOIN ems_v1.master_converter_detail mcd ON mcd.converter_id = mmt.converter_id 
        LEFT JOIN master_equipment_group meg on meg.equipment_group_id =  mm.equipment_group_id
        LEFT JOIN master_equipment me on me.equipment_id = mm.equipment_id
        '''
            
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def save_excel_meter(cnx,user_login_id):
    get_list =await check_import_meter(cnx)
    
    for record in get_list:
        meter_name = record["meter_name"]
        meter_code = record["meter_code"]
        company_id = record["company_id"]
        bu_id = record["bu_id"]
        plant_id = record["plant_id"]
        plant_department_id = record["plant_department_id"]
        converter_id = record["converter_id"]
        campus_id = record["campus_id"]
        ip_address = record["ip_address"]
        port = record["port"]
        major_nonmajor = record["major_nonmajor"]
        address = record["address"]
        meter_type = record["meter_type"]
        source = record["source"]
        physical_location = record["physical_location"]
        model_name = record["model_name"]
        IMEI = record["IMEI"]
        mac = record["mac"]
        meter = record["meter"]
        equipment_group_id = record["equipment_group_id"]
        equipment_id = record["equipment_id"]
        max_demand = record["max_demand"]
        max_pf = record["max_pf"]
        query = text(f"""
        INSERT INTO ems_v1.master_meter (
            company_id, meter_name, meter_code, bu_id, plant_id, plant_department_id, converter_id, campus_id,
            ip_address, port, created_on, created_by, major_nonmajor, model_name,  IMEI,  
            address,meter_type,source,physical_location,mac,meter,equipment_group_id,equipment_id,max_demand,max_pf
        )
        VALUES (
            {company_id},'{meter_name}', '{meter_code}', {bu_id}, {plant_id}, {plant_department_id}, {converter_id}, 
            {campus_id}, '{ip_address}',{port}, NOW(), '{user_login_id}', '{major_nonmajor}', '{model_name}', '{IMEI}',
            '{address}','{meter_type}','{source}','{physical_location}','{mac}','{meter}','{equipment_group_id}','{equipment_id}','{max_demand}','{max_pf}')
        """)
        await cnx.execute(query)
        result = await cnx.execute("SELECT LAST_INSERT_ID()")
        insert_id = result.first()[0]
        await cnx.commit()

        if insert_id !='': 
            query1 = text(f'''select * from ems_v1.master_meter_factor where meter_id = {insert_id}''')
            record = await cnx.execute(query1)
            record = record.fetchall()

            if len(record) == 0:
                query2 = text(f'''insert into ems_v1.master_meter_factor 
                (meter_id,plant_id,created_on,created_by)
                values({insert_id},{plant_id},now(),'{user_login_id}')
                ''')
                await cnx.execute(query2)
                await cnx.commit()

            
            sql = text(f''' select * from ems_v1.current_power where meter_id = {insert_id}''') 
            data = await cnx.execute(sql)
            data = data.fetchall()
            print(sql)

            if len(data)== 0:                       
                sql1 = text(f"select * from ems_v1.master_meter where meter_id = {insert_id}")
                data1 = await cnx.execute(sql1)
                data1 = data1.fetchall()

                for row in data1:
                    meter_id = row["meter_id"]
                    company_id = row["company_id"]
                    bu_id = row["bu_id"]
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    print(meter_id)

                sql2= text(f" select * from ems_v1.master_shifts where company_id = {company_id} and bu_id = {bu_id} and plant_id = {plant_id} AND status = 'active' ")
                data2 = await cnx.execute(sql2)
                data2 = data2.fetchall()
                mill_date = date.today()
                mill_shift = 1
                
                if len(data2)>0:
                    for row in data2:
                        mill_date = row["mill_date"]
                        mill_shift = row["mill_shift"]  

                if mill_date != '' and mill_shift != '':      
                    sql3 = text(f'''
                                INSERT INTO ems_v1.current_power (meter_id, date_time, date_time1,
                                mill_date, mill_shift,company_id, bu_id, plant_id, plant_department_id)
                                VALUES ({meter_id}, NOW(), NOW(), '{mill_date}', '{mill_shift}',{company_id},
                                {bu_id}, {plant_id}, {plant_department_id})
                                ''')  
                    await cnx.execute(sql3)
                    await cnx.commit()

                    query4=f''' update ems_v1.master_meter set meter_order=meter_id where meter_id= '{insert_id}' '''
                    await cnx.execute(text(query4))
                    await cnx.commit()

async def checkdemanddtl(cnx,campus_id,main_demand_meter,meter_id):
    data = ''
    where = ''
    demand_meter_cound = 0
    if meter_id != '':
        where = f" and meter_id <> '{meter_id}'"
    
    sql1 = text(f"select count(meter_id) cound from master_meter where campus_id = '{campus_id}' and   main_demand_meter = 'yes' and status <>'delete' {where} group by campus_id ")
    data = await cnx.execute(sql1)
    data = data.fetchall()
    if len(data)>0:
        for row in data:
            demand_meter_cound = row["cound"]
    sql2 = text(f"select demand_meter_limit from master_campus where campus_id = '{campus_id}' ")
    data2 = await cnx.execute(sql2)
    data2 = data2.fetchall()
    for i in data2:
        demand_meter_limit = i["demand_meter_limit"]

    return {"demand_meter_cound":demand_meter_cound,"demand_meter_limit":demand_meter_limit}

async def checkslaveid(cnx,ip_address,address,meter_id):
    where = ''
    if meter_id != '':
        where = f" and meter_id <> '{meter_id}'"
    sql = text(f" select * from master_meter where ip_address = '{ip_address}' and address = '{address}' and status <> 'delete' {where} ")
    createFolder("Log/","query:"+str(sql))
    data = await cnx.execute(sql)
    data = data.fetchall()
    return data

async def checkconverterlimt(cnx,converter_id,meter_id):
    data = ''
    where = ''
    count = 0
    if meter_id != '':
        where = f" and meter_id <> '{meter_id}'"
    sql = text(f"select * from master_converter_detail where converter_id = '{converter_id}' ")
    data = await cnx.execute(sql)
    data = data.fetchall()
    for row in data :
        meter_limit = row["meter_limit"]

    sql = f" select count(*) as count from  master_meter where converter_id = {converter_id} and status <> 'delete' {where}"
    data2 = await cnx.execute(sql)
    data2 = data2.fetchall()

    if len(data2)> 0:
        for i in data2:
            count = i["count"]
    
    return {"meter_limit":meter_limit,"count":count}

async def transformer_meter_list(cnx,campus_id):
    where =''
    if campus_id != '' and campus_id != 0:
        where +=f"and mp.campus_id = {campus_id}"
    query = f'''
            select  
                GROUP_CONCAT(mm.meter_id SEPARATOR ',') AS meter_ids,
                GROUP_CONCAT(mm.meter_name SEPARATOR ',') AS meter_names,
                '' as meter_dtl,
                mm.campus_id,
                mc.campus_name,
                mm.main_transformer_meter,
                IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
                mm.tm_modified_by,
                mm.tm_modified_on
            from 
                master_meter mm
                inner join master_plant mp on mp.plant_id = mm.plant_id
                inner join master_campus mc on mc.campus_id = mp.campus_id
                left join ems_v1.master_employee mu on mu.employee_id=mm.modified_by
            where mm.status = 'active' and mm.main_transformer_meter = 'yes' {where}
            group by mp.campus_id

            '''
    data = await cnx.execute(query)
    data = data.fetchall()  
    result = []
    for row in data:
        meter_id_list = row["meter_ids"].split(",")     
        meter_dtl = ""
        for meter_id in meter_id_list:                             
            sub_query = text(f"SELECT * FROM ems_v1.master_meter WHERE meter_id = {meter_id}")
            sub_data = await cnx.execute(sub_query)
            sub_data = sub_data.fetchall()
            for sub_row in sub_data:
                if meter_dtl != "":
                    meter_dtl += '\n' 
                meter_dtl += f'''{sub_row['meter_code']}-{sub_row['meter_name']}''' 
                print(meter_dtl)           
        new_row = dict(row)
        new_row["meter_dtl"] = meter_dtl
        result.append(new_row)         
    return result   

async def save_transformer_meter(cnx,campus_id,meter_ids,uesr_login_id):
    sql = f"update master_meter set main_transformer_meter = 'no' where campus_id = {campus_id}"
    await cnx.execute(text(sql))
    await cnx.commit()

    sql1 = f"update master_meter set main_transformer_meter = 'yes', tm_modified_on = now(), tm_modified_by = '{uesr_login_id}' where campus_id = {campus_id} and meter_id in ({meter_ids})"
    print(sql1)
    await cnx.execute(text(sql1))
    await cnx.commit()

async def transformer_submeter_list(cnx,campus_id,main_transformer_meter_id):
    where = ''
    if campus_id != '':
        where +=f" and mp.campus_id = '{campus_id}'"
    if main_transformer_meter_id != '':
        where +=f" and tm.main_transformer_meter_id = '{main_transformer_meter_id}'"
    query = f'''
            select  
                GROUP_CONCAT(tm.sub_meter_id SEPARATOR ',') AS meter_ids,
                '' as meter_dtl,
                mm.campus_id,
                mc.campus_name,
                tm.main_transformer_meter_id,
                IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as created_user,
                tm.created_on,
                tm.created_by
            from 
                master_transformer_meter tm
                inner join master_meter mm on mm.meter_id = tm.sub_meter_id
                inner join master_plant mp on mp.plant_id = mm.plant_id
                inner join master_campus mc on mc.campus_id = mp.campus_id
                left join ems_v1.master_employee mu on mu.employee_id=tm.created_by
            where mm.status = 'active'  {where}
            group by tm.main_transformer_meter_id

            '''
    data = await cnx.execute(query)
    data = data.fetchall()  
    result = []
    for row in data:
        meter_id_list = row["meter_ids"].split(",")     
        meter_dtl = ""
        for meter_id in meter_id_list:                             
            sub_query = text(f"SELECT * FROM ems_v1.master_meter WHERE meter_id = {meter_id}")
            sub_data = await cnx.execute(sub_query)
            sub_data = sub_data.fetchall()
            for sub_row in sub_data:
                if meter_dtl != "":
                    meter_dtl += '\n' 
                meter_dtl += f'''{sub_row['meter_code']}-{sub_row['meter_name']}''' 
                print(meter_dtl)           
        new_row = dict(row)
        new_row["meter_dtl"] = meter_dtl
        result.append(new_row)         
    return result 

async def save_subtransformer_meter(cnx,main_transformer_meter_id,meter_ids,user_login_id):
    
    delete_query = f" delete from master_transformer_meter where main_transformer_meter_id = '{main_transformer_meter_id}'"
    await cnx.execute(delete_query)
    await cnx.commit()
    
    meter_id_list = meter_ids.split(",")  
    for meter_id in meter_id_list:
        sql =f'''insert into master_transformer_meter(main_transformer_meter_id,meter_ids,sub_meter_id,created_on,created_by)
                values('{main_transformer_meter_id}','{meter_ids}','{meter_id}',now(),'{user_login_id}')'''
        await cnx.execute(sql)
        await cnx.commit()