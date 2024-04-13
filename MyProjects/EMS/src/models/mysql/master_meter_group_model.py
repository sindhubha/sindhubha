from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image,id
from log_file import createFolder

async def meter_group_Lists(cnx, meter_group_id):

    where = ""
    if meter_group_id !='' and meter_group_id != 0:
        where = f" AND mmg.meter_group_id = {meter_group_id}"   
    # where += f" and mm.meter_id in ({','.join(str(x) for x in meter_id)})  
    query = text(f"""
        SELECT                
            mm.meter_code AS meter_code,
            mm.meter_name AS meter_name,
            (CASE 
            WHEN group_type='Zone' THEN (SELECT plant_name FROM ems_v1.master_plant WHERE plant_id=type_id)
            WHEN group_type='Area' THEN (SELECT plant_department_name FROM ems_v1.master_plant_wise_department WHERE plant_department_id=type_id)
            WHEN group_type='Location' THEN (SELECT equipment_group_name FROM ems_v1.master_equipment_group WHERE equipment_group_id=type_id)
            WHEN group_type='Function' THEN (SELECT function_name FROM master_function WHERE function_id=type_id)
            WHEN group_type='Function_1' THEN (SELECT function_name FROM master_function WHERE function_id=type_id)
            WHEN group_type='Function_2' THEN (SELECT function_name FROM master_function WHERE function_id=type_id)
            END) AS type_name,
            mmg.*,
            '' AS meter_dtl,
            IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
	        IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user
        FROM 
            ems_v1.master_meter_group mmg
            left join ems_v1.master_employee cu on cu.employee_id=mmg.created_by
	        left join ems_v1.master_employee mu on mu.employee_id=mmg.modified_by                
            INNER JOIN master_meter mm ON mmg.meter_id = mm.meter_id
        WHERE 
            mmg.status != 'delete'{where}
    """)
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()      
 
    result = []
    for row in data:
        meter_id_list = row["meter_id"].split(",")   # Split comma-separated meter IDs into a list
        meter_dtl = ""

        for meter_id in meter_id_list:                             
            sub_query = text(f"SELECT * FROM ems_v1.master_meter WHERE meter_id = {meter_id}")
            sub_data = await cnx.execute(sub_query)
            sub_data = sub_data.fetchall()

            for sub_row in sub_data:
                if meter_dtl != "":
                    meter_dtl += '\n' 
                meter_dtl += f'''{sub_row['meter_code']} - {sub_row['meter_name']} '''  
                print(meter_dtl)  

        new_row = dict(row)
        new_row["meter_dtl"] = meter_dtl
        result.append(new_row)
        
    return result
    
    
async def save_meter_group(cnx, meter_id,group_type,type_id,user_login_id):
        
    meter_id = await id(meter_id)
    query = text(f"""
            INSERT INTO ems_v1.master_meter_group (
            group_type, type_id, meter_id, created_on, created_by
            )
            VALUES (
                '{group_type}', '{type_id}', '{meter_id}',  now(), '{user_login_id}'
            )
        """) 
    print(query)
    # createFolder("Log/",f"{query}")
    await cnx.execute(query)
    await cnx.commit()
    
async def update_meter_group(cnx,meter_group_id,meter_id,group_type,type_id,user_login_id):

    query =text(f"""
        UPDATE ems_v1.master_meter_group
        SET group_type = '{group_type}', type_id = '{type_id}',
        meter_id = '{meter_id}',  modified_on = NOW(),
        modified_by = '{user_login_id}'
        WHERE meter_group_id = {meter_group_id} 
    """)
    
    await cnx.execute(query)
    await cnx.commit()
   
    
async def update_meter_groupStatus(cnx, meter_group_id, status):
    if status != '':
        query=f''' Update ems_v1.master_meter_group Set status = '{status}' Where meter_group_id='{meter_group_id}' '''
    else: 
        query=f''' Update ems_v1.master_meter_group Set status = 'delete' Where meter_group_id='{meter_group_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    
    

