from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder
import json

async def report_name_list(cnx):

    query= text(f'''select * from ems_v1.report where status = 'active' ''')
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def report_fields_list(cnx,report_id):

    query = text(f"select * from ems_v1.report_fields where report_id = {report_id}")
    data = await cnx.execute(query)
    data = data.fetchall()      
    return data
    
async def update_reportfields(cnx,obj):
  
    obj_data = json.loads(obj)
    for row in obj_data:
        field_name_display = row["field_name_display"]
        report_field_id = row["report_field_id"]
        slno = row["slno"]
        is_show = row["is_show"]
        is_table_show = row["is_table_show"]
        status = row["status"]
    #[{"field_name_display":"Company NAME","report_field_id":"2","slno":"2","is_show":"yes","is_table_show":"yes","status":"active"},{"field_name_display":"Company Code","report_field_id":"1","slno":"1","is_show":"yes","is_table_show":"yes","status":"active"}]      
        query =  text(f'''
                UPDATE 
                    ems_v1.report_fields 
                SET 
                    field_name_display = '{field_name_display}', 
                    slno = '{slno}', 
                    is_show = '{is_show}',
                    is_table_show = '{is_table_show}',
                    status = '{status}'
                WHERE  report_field_id = '{report_field_id}' ''')
        print(query)
        await cnx.execute(query)
        await cnx.commit()
            
    
