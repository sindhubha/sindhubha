from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.parse_date import parse_date
from datetime import date
from log_file import createFolder

async def savemanual_entry(cnx,obj,user_login_id,plant_id,request):

    data = ''
    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
    completed_db="ems_v1_completed."    
        
    client_host = request.client.host
    where = ''
    kwh_reset = ''
    
    user_dict =json.loads(obj)

    for i in user_dict:
        meter_id = i['meter_id']
        print(meter_id)
        mill_date = i['mill_date']
        mill_shift = i['mill_shift']
        kwh_reset = i['kwh_reset']
        kwh = i['kWh']
        calculated_kwh = i['calculated_kwh']

        if kwh_reset =='yes':
            
            if mill_shift != '' :
                where +=f"and mill_shift = '{mill_shift}'"

            mill_date = await parse_date(mill_date)
            print(mill_date)
            month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
            table_name=f"  {completed_db}power_{month_year}"     

            query = text(f'''
                UPDATE {table_name}
                SET kWh = 0 ,machine_kwh = 0 , master_kwh = 0
                WHERE meter_id = '{meter_id}' and mill_date = '{mill_date}' {where} ''')
            createFolder("Manual_Entry_Log/", "query " + str(query))
            await cnx.execute(query)
            await cnx.commit()

            sql = f'''insert into manual_entry_history(meter_id,mill_date,mill_shift,kwh,calculated_kwh,created_on,created_by,ip_address)
                    values('{meter_id}','{mill_date}','{mill_shift}','{kwh}','{calculated_kwh}',now(),'{user_login_id}','{client_host}')'''
            await cnx.execute(text(sql))
            await cnx.commit()

    sql = f'''insert into data_correction(mill_date,mill_shift,plant_id,is_manual_call)
              values('{mill_date}','{mill_shift}','{plant_id}','yes')'''
    createFolder("Manual_Entry_Log/", "data_correction.." + str(sql))
    await cnx.execute(text(sql))
    await cnx.commit()

    return data

async def manualdata_correction(cnx,mill_date,mill_shift,plant_id):
    mill_date = await parse_date(mill_date)
    sql = f'''insert into data_correction(mill_date,mill_shift,plant_id,is_manual_call)
              values('{mill_date}','{mill_shift}','{plant_id}','yes')'''
    createFolder("Manual_Entry_Log/", "data_correction.." + str(sql))
    await cnx.execute(text(sql))
    await cnx.commit()