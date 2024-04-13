from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from log_file import createFolder
import json

async def energy_calculationlist(cnx):

    query = text(f'''select * from ems_v1.master_energy_calculations''')
    data = await cnx.execute(query)
    data = data.fetchall()
    
    return data
    

async def save_energycalculation(cnx, obj):
    
    data = ''
    del_query=text(f'''DELETE FROM ems_v1.master_energy_calculations''')
    await cnx.execute(del_query)
    await cnx.commit()
    obj_data = json.loads(obj)
    if obj !="":
        for row in obj_data:
            s_no = row["s_no"]
            group_name = row["group_name"]
            function_name = row["function_name"]
            formula1 = row["formula1"]
            formula2 = row["formula2"]
            parameter = row["parameter"]
            roundoff_value = row["roundoff_value"]
            query = text(f'''INSERT INTO ems_v1.master_energy_calculations 
                            (s_no,group_name,function_name,formula1,formula2,parameter,roundoff_value)
                            values({s_no},'{group_name}','{function_name}','{formula1}','{formula2}','{parameter}','{roundoff_value}')''')
            await cnx.execute(query)
            await cnx.commit()
    return data
 
async def energy_calculationlist2(cnx):
    
    query = text(f'''select * from ems_v1.master_energy_calculations2''')
    data = await cnx.execute(query)
    data = data.fetchall()
    
    return data

async def save_energycalculation2(cnx, obj):
 
    data = ''
    del_query=text(f'''DELETE FROM ems_v1.master_energy_calculations''')
    await cnx.execute(del_query)
    await cnx.commit()
    obj_data = json.loads(obj)
    if obj !="":
        for row in obj_data:
            s_no = row["s_no"]
            group_name = row["group_name"]
            function_name = row["function_name"]
            formula1 = row["formula1"]
            formula2 = row["formula2"]
            parameter = row["parameter"]
            roundoff_value = row["roundoff_value"]
            query = text(f'''INSERT INTO ems_v1.master_energy_calculations2 
                            (s_no,group_name,function_name,formula1,formula2,parameter,roundoff_value)
                            values({s_no},'{group_name}','{function_name}','{formula1}','{formula2}','{parameter}','{roundoff_value}')''')
            await cnx.execute(query)
            await cnx.commit()

    return data
    