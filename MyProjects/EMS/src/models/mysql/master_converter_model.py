from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.image import save_image
from log_file import createFolder

async def converter_Lists(cnx, campus_id,converter_make_id,converter_model_id,converter_id,is_limit_check):
    meter_count = ''
    count = ''
    where = ""

    if converter_make_id != "":
        where += f"and mcm.converter_make_id = '{converter_make_id}' "

    if converter_model_id != "":
        where += f"and mcm.converter_model_id = '{converter_model_id}' "

    if converter_id != "":
        where += f"and mcd.converter_id = '{converter_id}' "

    if campus_id != "":
        where += f"and c.campus_id = '{campus_id}' "

    query =text( f''' 
            SELECT 
            	mcd.*,
                mc.company_name,
                mc.company_code,
                c.campus_name,
                c.campus_code,
                mcm.converter_model_name,
                mcm.converter_make_id,
                mme.converter_make_name,
                mb.bu_code,
                mb.bu_name,
                mp.plant_code,
                mp.plant_name,
            	IFNULL(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
            	IFNULL(concat(mu.employee_code,'-',mu.employee_name),'') as modified_use
            FROM  
            	ems_v1.master_converter_detail mcd
                inner join master_campus c on c.campus_id = mcd.campus_id
                inner join master_company mc on mc.company_id = mcd.company_id
                inner join master_business_unit mb on mb.bu_id = mcd.bu_id
                inner join master_plant mp on mp.plant_id = mcd.plant_id
                inner join master_converter_model mcm on mcm.converter_model_id = mcd.converter_model_id
                inner join master_converter_make mme on mme.converter_make_id = mcm.converter_make_id
            	left join ems_v1.master_employee cu on cu.employee_id=mcd.created_by
            	left join ems_v1.master_employee mu on mu.employee_id=mcd.modified_by
            WHERE 
            	mcd.status !='delete' and  mcm.status != 'delete'
            	{where} 
            ''') 
    createFolder("Current_power_log/","current_power api query "+str(query))   
    data = await cnx.execute(query)
    data = data.fetchall()

    return data
    
    
async def getconverterdtl(cnx,  converter_name):
    
      
    query=f'''select * from ems_v1.master_converter_detail where 1=1 and status<>'delete' and converter_name= '{converter_name}' '''

    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def save_converter(cnx,campus_id,converter_model_id,converter_name,ip_address,port_no,meter_limit,user_login_id,mac,company_id,bu_id,plant_id,baud_rate,parity):

    query = text(f"""
            INSERT INTO ems_v1.master_converter_detail (
                 converter_name, ip_address, port_no, created_on, created_by,meter_limit,campus_id,converter_model_id,mac,company_id,bu_id,plant_id,baud_rate,parity
            )
            VALUES (
                '{converter_name}', '{ip_address}', {port_no},  NOW(), '{user_login_id}','{meter_limit}','{campus_id}','{converter_model_id}','{mac}','{company_id}','{bu_id}','{plant_id}'
                ,'{baud_rate}','{parity}'
            )
        """) 
    await cnx.execute(query)
    result = await cnx.execute("SELECT LAST_INSERT_ID()")
    insert_id = result.first()[0]
    await cnx.commit()

    return insert_id
    
    
async def update_converter(cnx,campus_id,converter_model_id,converter_id,converter_name,ip_address,port_no,meter_limit,user_login_id,mac,company_id,bu_id,plant_id,baud_rate,parity):
    
    query =text(f"""
            UPDATE ems_v1.master_converter_detail
            SET converter_name = '{converter_name}', ip_address = '{ip_address}',
            port_no = {port_no},  modified_on = NOW(), modified_by = '{user_login_id}',
            meter_limit = {meter_limit},campus_id = '{campus_id}', 
            converter_model_id = '{converter_model_id}',mac = '{mac}',company_id  = '{company_id}',
            bu_id = '{bu_id}',plant_id = '{plant_id}',baud_rate = '{baud_rate}',parity = '{parity}'
            WHERE converter_id = {converter_id}
        """)
        
    await cnx.execute(query)
    await cnx.commit()
    
    sql = text(f"update master_meter set ip_address = '{ip_address}', port = {port_no} , mac = '{mac}' where converter_id = {converter_id}")
    await cnx.execute(sql)
    await cnx.commit()
    
async def update_converterStatus(cnx, converter_id, status):
    if status != '':
        query=f''' Update ems_v1.master_converter_detail Set status = '{status}' Where converter_id='{converter_id}' '''
    else: 
        query=f''' Update ems_v1.master_converter_detail Set status = 'delete' Where converter_id='{converter_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    
    

