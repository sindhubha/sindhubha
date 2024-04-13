from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.parse_date import parse_date
from datetime import date

async def operation_dtl(cnx,employee_id,menu_id):

    where=""
    if menu_id !='':
        where+=f''' and menu_id='{menu_id}' '''

    query=text(f'''SELECT 
                    u.*,
                    e.employee_type
              FROM 
                    user_rights u
                    inner join master_employee e on e.employee_id=u.userid
              WHERE
                    u.userid={employee_id} {where}
                    
    ''')
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
   