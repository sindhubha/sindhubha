from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder

async def alarm_Lists(cnx, company_id,alarm_target_id,alarm_type,bu_id,plant_id,plant_department_id,equipment_group_id):

        where = ""
        if alarm_target_id != '':
            where += f"and at.alarm_target_id = '{alarm_target_id}' "
            
        if alarm_type != '':
            where += f" and at.alarm_type= '{alarm_type}' "

        if company_id !='' and company_id != "0":
            where += f" and at.company_id= '{company_id}' "

        if bu_id !='' and bu_id !="0":
            where += f" and at.bu_id= '{bu_id}' "

        if plant_id !='' and plant_id!= "0":
            where += f" and at.plant_id= '{plant_id}' "

        if plant_department_id !='' and plant_department_id != "0":
            where += f" and at.plant_department_id= '{plant_department_id}' "

        if equipment_group_id !='' and equipment_group_id !="0":
            where += f" and at.equipment_group_id= '{equipment_group_id}' "

        query=text(f''' 
                SELECT 
                    at.*, 
                    '' as meter_dtl,
                    mb.bu_code,
                    mb.bu_name,
                    md.plant_code,
                    md.plant_name,
                    ms.plant_department_code,
                    ms.plant_department_name,
                    mmt.equipment_group_code,
                    mmt.equipment_group_name,
                    IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	                IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
                FROM 
                    ems_v1.master_alarm_target at
                    left join ems_v1.master_employee cu on cu.employee_id=at.created_by
	                left join ems_v1.master_employee mu on mu.employee_id=at.modified_by
                    left join ems_v1.master_company mc on mc.company_id=at.company_id
                    left join ems_v1.master_business_unit mb on mb.bu_id=at.bu_id
                    left join ems_v1.master_plant md on md.plant_id=at.plant_id
                    left join ems_v1.master_plant_wise_department ms on ms.plant_department_id=at.plant_department_id
                    left join ems_v1.master_equipment_group mmt on mmt.equipment_group_id=at.equipment_group_id 
                WHERE 
                    at.status <> 'delete'
                    {where} 
                ''')
        
        data = await cnx.execute(query)
        data = data.fetchall()
        result = []
        for row in data:
            meter_id_list = row["meter_id"].strip(",").split(",")     
            meter_dtl = ""
            for meter_id in meter_id_list:                             
                sub_query = text(f"SELECT * FROM ems_v1.master_meter WHERE meter_id = {meter_id}")
                sub_data = await cnx.execute(sub_query)
                sub_data = sub_data.fetchall()
                for sub_row in sub_data:
                    if meter_dtl != "":
                        meter_dtl += '\n' 
                    meter_dtl += f'''{sub_row['meter_name']}''' 
                    print(meter_dtl)           
            new_row = dict(row)
            new_row["meter_dtl"] = meter_dtl
            result.append(new_row)            
        
        return result
    
    
async def getalarmdtl(cnx, alarm_target_id,  alarm_name):
    where=""

    if alarm_target_id != "":
        where += f"and alarm_target_id <> '{alarm_target_id}' "
      
    query=f'''select * from ems_v1.master_alarm_target where 1=1 and status<>'delete' and alarm_name='{alarm_name}' {where}'''

    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def save_alarm(cnx,company_id,bu_id,plant_id ,plant_department_id ,equipment_group_id ,parameter_name,meter_id,alarm_name,alarm_type,alarm_duration,color_1,color_2,color_3,login_id,conditions):
 
        where =''
        if company_id!='' and company_id!= 0 :
            where += f'where mm.company_id = {company_id}'
        
        if bu_id!='' and company_id!= 0:
            where += f'and mm.bu_id = {bu_id}'
        
        if plant_id!='' and company_id!= 0:
            where += f'and mm.plant_id = {plant_id}'
        
        if plant_department_id!='' and company_id!= 0:
            where += f'and mm.plant_department_id = {plant_department_id}'
        
        if equipment_group_id!='' and company_id!= 0:
            where += f'and mm.equipment_group_id = {equipment_group_id}'

        if meter_id == '' or meter_id == 'all':
            query = text(f'''
                        select 
                            DISTINCT mm.meter_id 
                        from 
                            ems_v1.master_meter mm
                        {where}          
            ''') 
            print(query)
            data=await cnx.execute(query)
            data=data.fetchall()

            meter_id = []  
            if len(data) > 0:
                for record in data:
                    meter_id.append(str(record["meter_id"]))  
            meter_id = ",".join(meter_id)  

        if meter_id !='':
            value = meter_id.split(",")
            if len(value) > 1:
                values = ",".join(value)  
                meter_id = f",{values},"  
            else:
                meter_id = f",{value[0]},"

        query= text(f'''INSERT INTO ems_v1.master_alarm_target (meter_id,parameter_name,alarm_name,color_1,color_2,color_3,
                       created_on,created_by, alarm_duration, alarm_type ,company_id,conditions)
                       VALUES ('{meter_id}','{parameter_name}','{alarm_name}','{color_1}', '{color_2}','{color_3}',
                       now(),'{login_id}', '{alarm_duration}', '{alarm_type}',{company_id},{conditions}) ''')
            
        await cnx.execute(query)
        insert_id =  await cnx.execute(text("SELECT LAST_INSERT_ID()"))
        insert_id = insert_id.first()[0]
        await cnx.commit()

        return insert_id

    
async def update_alarm(cnx, alarm_target_id,company_id,bu_id,plant_id ,plant_department_id ,equipment_group_id ,parameter_name,meter_id,alarm_name,alarm_type,alarm_duration,color_1,color_2,color_3,login_id,conditions):
  
        where =''
        if company_id!='' and company_id!= 0 :
            where += f'where mm.company_id = {company_id}'
        
        if bu_id!='' and company_id!= 0:
            where += f'and mm.bu_id = {bu_id}'
        
        if plant_id!='' and company_id!= 0:
            where += f'and mm.plant_id = {plant_id}'
        
        if plant_department_id!='' and company_id!= 0:
            where += f'and mm.plant_department_id = {plant_department_id}'
        
        if equipment_group_id!='' and company_id!= 0:
            where += f'and mm.equipment_group_id = {equipment_group_id}'

        if meter_id == '':
            query = text(f'''
                        select 
                            DISTINCT mm.meter_id 
                        from 
                            ems_v1.master_meter mm
                        {where}          
            ''') 
            data=await cnx.execute(query)
            data=data.fetchall()

            meter_id = []  
            if len(data) > 0:
                for record in data:
                    meter_id.append(str(record["meter_id"]))  
            meter_id = ",".join(meter_id)  
       
        if meter_id !='':
            value = meter_id.split(",")
            if len(value) > 1:
                values = ",".join(value)  
                meter_id = f",{values},"  
            else:
                meter_id = f",{value[0]},"

        query =text(f'''UPDATE  ems_v1.master_alarm_target SET meter_id='{meter_id}',parameter_name='{parameter_name}',
                       alarm_name='{alarm_name}',color_1='{color_1}',color_2='{color_2}',color_3='{color_3}',
                       modified_on = now(),modified_by='{login_id}', alarm_duration = {alarm_duration},alarm_type = '{alarm_type}',company_id = {company_id},
                       conditions = {conditions}
                       where alarm_target_id = '{alarm_target_id}'   
                       ''')
        
        await cnx.execute(query)
        await cnx.commit()

    
async def update_alarmStatus(cnx, alarm_target_id, status):

    if status != '':
        query=f''' Update ems_v1.master_alarm_target Set status = '{status}' Where alarm_target_id='{alarm_target_id}' '''
    else: 
        query=f''' Update ems_v1.master_alarm_target Set status = 'delete' Where alarm_target_id='{alarm_target_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    
async def alarm_popup_status(cnx,company_id):
        data = ''
        query = text(f'''Update ems_v1.master_company set alarm_status = 0 where company_id = {company_id}''')
        await cnx.execute(query)
        await cnx.commit()
        
        return data
    