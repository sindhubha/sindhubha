from datetime import datetime,date
import socket
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from sqlalchemy.sql import text
from log_file import createFolder

async def loginformmodel(cnx, logemployee_name, logpassword):

    where =""
    where += f''' and me.employee_code='{logemployee_name}' and me.password_login=MD5('{logpassword}') '''
    
    query=text(f'''
            SELECT 
                me.*,
                mc.company_name as company_name,
                mc.company_code as company_code,
                mb.bu_name as bu_name,
                mb.bu_code as bu_code,
                md.plant_name as plant_name,
                md.plant_code as plant_code,
                ifnull(md.campus_id, 0) as campus_id,
                c.campus_name
                                
            FROM 
                ems_v1.master_employee me
                LEFT JOIN ems_v1.master_company mc on me.company_id=mc.company_id
                LEFT JOIN ems_v1.master_business_unit mb on me.bu_id=mb.bu_id
                LEFT JOIN ems_v1.master_plant md on me.plant_id=md.plant_id
                LEFT JOIN ems_v1.master_campus c on c.campus_id=md.campus_id
            WHERE 
                me.employee_code='{logemployee_name}' AND 
                me.password_login=md5('{logpassword}') AND me.is_login='yes' AND me.status = 'active' ''')
    data = await cnx.execute(query)        
    data = data.fetchall()      
        
    return data
   
    
async def changepassword(cnx,employee_id,old_password,new_password,retype_password):  
    sql = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id} and password_login = password_login=md5('{old_password}') ''')
    data = await cnx.execute(query)        
    data = data.fetchall()         
    if len(data) == 0:            
       return _getErrorResponseJson("incorrect user id or password")
    else:
        if new_password != retype_password:
            return _getErrorResponseJson("retype password is incorrect")
        query=text(f'''update ems_v1.master_employee set password_login=md5('{new_password}') where employee_id ='{employee_id}' ''')
        await cnx.execute(query)
        await cnx.commit()

    
    
