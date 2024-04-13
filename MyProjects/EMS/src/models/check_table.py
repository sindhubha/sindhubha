from sqlalchemy.sql import text
from log_file import createFolder

async def check_analysis_table(cnx,month_year):
    query = text(f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_analysis_{month_year}'""")
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def check_power_table(cnx,month_year):
    query = text(f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'""")
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def check_polling_data_tble(cnx,month_year):
    query = text(f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'polling_data_{month_year}'""")
    # createFolder("Current_power_log/","Polling Data table query "+str(query))
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def check_power_12_table(cnx,month_year):
    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12'"""
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def check_alarm_tble(cnx,month_year):
    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'alarm_{month_year}'"""
    print(query)
    data = await cnx.execute(query)
    data = data.fetchall()
    return data

async def check_user_count(cnx):
    query2 = "SELECT COUNT(1) as Total_Count , user  FROM information_schema.PROCESSLIST group by user"
    all_user = await cnx.execute(query2)
    all_user = all_user.fetchall()
    return all_user
