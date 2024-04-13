from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.parse_date import parse_date
from datetime import date
async def sidebarlist(cnx,employee_id):
        employee_type = ''
        query=text(f''' select * from master_employee where employee_id={employee_id}''')
        data=await cnx.execute(query)
        data = data.fetchall()
        print(data)
        
        if len(data) > 0 :
            for record in data:
                employee_type=record['employee_type']

        if employee_type == 'Admin':
            query1=text(f'''
                        select * 
                        from menu_mas 
                        where status='active' 
                        order by slno
                        ''')
        else:
            query1=text(f''' SELECT 
                            ms.*,
                            IFNULL(u.id, 0) AS u_r_id,
                            IFNULL(u.add_op, '') AS add_opp,
                            IFNULL(u.edit_op, '') AS edit_opp,
                            IFNULL(u.delete_op, '') AS delete_opp
                        FROM
                            menu_mas ms,
                            user_rights u
                        WHERE
                            ms.status = 'active'
                            AND ms.menu_id = u.menu_id
                            AND u.userid = {employee_id}
                        ORDER BY ms.slno
                            
			  ''')
        data = await cnx.execute(query1)
        data = data.fetchall()
        
        return data
    