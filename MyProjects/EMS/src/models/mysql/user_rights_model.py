from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
async def employeelistuser(cnx,employee_id,is_login):

    where = ""
    if employee_id !='':
        where = text(f"and employee_id = '{employee_id}' ")
    query=text(f'''SELECT * FROM  ems_v1.master_employee WHERE status='active' and employee_type <> 'Admin' {where} ''')
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
       
async def menu_list(cnx,employee_id):

    query1=text(f''' 
                SELECT 
                    ms.*,
                    IFNULL (u.id,0) AS u_r_id,
                    IFNULL (u.add_op,'')AS add_opp,
                    IFNULL (u.edit_op,'')AS edit_opp,
                    IFNULL (u.delete_op,'')AS delete_opp
                FROM
                    ems_v1.menu_mas ms
                    LEFT JOIN 
                    (select * from ems_v1.user_rights where userid={employee_id}) As u
                    ON u.menu_id=ms.menu_id
                    WHERE ms.status='active' 
                    ORDER BY ms.slno
		  ''')
    print(query1)  
    data = await cnx.execute(query1)
    data = data.fetchall()      
    return data

async def save_userrights(cnx,employee_id, menu):

    del_query=text(f'''DELETE FROM ems_v1.user_rights WHERE userid='{employee_id}' ''')
    await cnx.execute(del_query)
    await cnx.commit()
    user_dict = json.loads(menu)

    for i in user_dict:
          menu_id=i['menu_id']
          add_op = i['add_op']
          edit_op = i['edit_op']
          delete_op=i['delete_op']
          query=text(f'''insert into ems_v1.user_rights(menu_id,add_op,edit_op,delete_op,userid)
                  values('{menu_id}','{add_op}','{edit_op}','{delete_op}','{employee_id}') ''') 
                 
          await cnx.execute(query)
          await cnx.commit() 
    return employee_id

async def save_menumas(cnx,menu):
    user_dict = json.loads(menu)
    for i in user_dict:
        menu_id=i['menu_id']
        menu_name_display = i['menu_name_display']
        query = text(f'''update ems_v1.menu_mas set menu_name_display= '{menu_name_display}' where menu_id = {menu_id}''')       
        await cnx.execute(query)
        await cnx.commit()
    
