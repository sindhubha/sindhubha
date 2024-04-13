import requests
import time
import traceback
import os
import json
import pyodbc
import sys
from datetime import datetime, date, timedelta
from dateutil.parser import parse
from dateutil import parser

def createFolder(directory, data):
    date_time = datetime.now()
    curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2 = date_time.strftime("%d-%m-%Y")

    try:
        # Get the path of the current script
        base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

        # Create the directory inside the user's file directory
        directory = os.path.join(base_path, directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Remove log files older than 5 days
        five_days_ago = date_time - timedelta(days=5)
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                file_date_str = filename.split('.')[0]
                file_date = datetime.strptime(file_date_str, "%d-%m-%Y")
                if file_date < five_days_ago:
                    os.remove(file_path)

        # Create the log file inside the directory
        file_path = os.path.join(directory, f"{curtime2}.txt")
        with open(file_path, "a+") as f:
            f.write(f"{curtime1} {data}\r\n")
    except OSError as e:
        print(f"Error: Creating directory. {directory} - {e}")

mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
# month_year=f"""{mill_month[current_date.month]}{str(current_date.year)}"""   

def create_table(month_year):
    sql = f'''use ems_v1_completed;
        CREATE TABLE power_{month_year}_12( power_id int NOT NULL identity(1,1),
        company_id int NOT NULL DEFAULT 0,
        branch_id int NOT NULL DEFAULT 0,
        department_id int NOT NULL DEFAULT 0,
        shed_id int NOT NULL DEFAULT 0,
        machinetype_id int NOT NULL DEFAULT 0,
        machine_id int NOT NULL DEFAULT 0,
        design_id int NOT NULL DEFAULT 0,
        beam_id int NOT NULL DEFAULT 0,
        date_time datetime NOT NULL default getdate(),
        date_time1 datetime NOT NULL default getdate(),
        mill_date datetime NOT NULL,
        mill_shift int NOT NULL DEFAULT 1,
        vln_avg float NOT NULL DEFAULT 0,
        r_volt float NOT NULL DEFAULT 0,
        y_volt float NOT NULL DEFAULT 0,
        b_volt float NOT NULL DEFAULT 0,
        vll_avg float NOT NULL DEFAULT 0,
        ry_volt float NOT NULL DEFAULT 0,
        yb_volt float NOT NULL DEFAULT 0,
        br_volt float NOT NULL DEFAULT 0,
        t_current float NOT NULL DEFAULT 0,
        r_current float NOT NULL DEFAULT 0,
        y_current float NOT NULL DEFAULT 0,
        b_current float NOT NULL DEFAULT 0,
        t_watts float NOT NULL DEFAULT 0,
        r_watts float NOT NULL DEFAULT 0,
        y_watts float NOT NULL DEFAULT 0,
        b_watts float NOT NULL DEFAULT 0,
        t_var float NOT NULL DEFAULT 0,
        r_var float NOT NULL DEFAULT 0,
        y_var float NOT NULL DEFAULT 0,
        b_var float NOT NULL DEFAULT 0,
        t_voltampere float NOT NULL DEFAULT 0,
        r_voltampere float NOT NULL DEFAULT 0,
        y_voltampere float NOT NULL DEFAULT 0,
        b_voltampere float NOT NULL DEFAULT 0,
        avg_powerfactor float NOT NULL DEFAULT 0,
        r_powerfactor float NOT NULL DEFAULT 0,
        y_powerfactor float NOT NULL DEFAULT 0,
        b_powerfactor float NOT NULL DEFAULT 0,
        powerfactor float NOT NULL DEFAULT 0,
        kwh float NOT NULL DEFAULT 0,
        kvah float NOT NULL DEFAULT 0,
        kw float NOT NULL DEFAULT 0,
        kvar float NOT NULL DEFAULT 0,
        power_factor float NOT NULL DEFAULT 0,
        kva float NOT NULL DEFAULT 0,
        frequency float NOT NULL DEFAULT 0,
        machine_status int NOT NULL DEFAULT 0,
        status int NOT NULL DEFAULT 0,
        created_on datetime NOT NULL DEFAULT getdate(),
        created_by int NOT NULL DEFAULT 0,
        modified_on varchar(30) NOT NULL DEFAULT '',
        modified_by int NOT NULL DEFAULT 0,
        machine_kwh float NOT NULL DEFAULT 0,
        master_kwh float NOT NULL DEFAULT 0,
        start_kwh float NOT NULL DEFAULT 0,
        end_kwh float NOT NULL DEFAULT 0,
        reverse_machine_kwh float NOT NULL DEFAULT 0,
        reverse_master_kwh float NOT NULL DEFAULT 0,
        reverse_kwh float NOT NULL DEFAULT 0,
        meter_status int NOT NULL DEFAULT 0,
        PRIMARY KEY (power_id))
    '''
    # print(sql)
    cursor.execute(sql)
    
    cursor.commit()

def day_record():
    try:
        createFolder("Logs/",f"Function Called... ")
        current_time = datetime.now().strftime('%H:%M')
       
        start_kwh = 0
        end_kwh = 0
        reverse_start_kwh = 0
        reverse_end_kwh = 0
        on_load_time = 0
        
        query3 = ''
        data = ''
        current_date = '' 
        meter_name = ''   
        location_code = ''
        location_name = '' 
        function2_name = ''
        function1_name = ''
        function_code = ''
        meter_status = 0
        
        if current_time == '23:59':
            current_date = date.today()
            next_date = current_date + timedelta(days=1)
            # date_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            createFolder("Logs/","current_date_time "+str(current_date))
            month_year=f"""{mill_month[current_date.month]}{str(current_date.year)}"""   
            query1 = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}_12'"""
            query1 = cursor.execute(query1).fetchall()
            if len(query1)>0: 
                pass
            else:
                create_table(month_year)
            
            sql = f''' 
                    select 
                        mm.*,
                        mmt.machinetype_code,
                        mmt.machinetype_name,
                        mf.function_code,
                        isnull(mf.function_name,'') function_name,
                        mmf.function_code as function2_code,
                        isnull(mmf.function_name,'') as function2_name
                    from 
                        ems_v1.dbo.master_machine mm 
                        inner join ems_v1.dbo.master_machinetype mmt on mmt.machinetype_id = mm.machinetype_id
                        left join ems_v1.dbo.master_function mf on mf.function_id = mm.function_id 
                        left join ems_v1.dbo.master_function mmf on mmf.function_id = mm.function2_id
                    where mm.status = 'active' '''
            
            data = cursor.execute(sql).fetchall()
            for row in data:
                machine_id = row.machine_id
                meter_code = row.machine_code
                meter_name = row.machine_name
                location_code = row.machinetype_code
                location_name = row.machinetype_name
                function_code = row.function_code
                function_name = row.function_name
                function2_code = row.function2_code
                function2_name = row.function2_name
                
                start_kwh = 0
                reverse_start_kwh = 0
                end_kwh = 0
                reverse_end_kwh = 0
                meter_status = 0
                kwh = 0
                reverse_kwh = 0
                on_load_time = 0

                query1 = f'''
                    select 
                        top (1) cp.* 
                    from
                    (
                        select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,on_load_time,meter_status from ems_v1.dbo.current_power_analysis 
                        union all 
                        select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,on_load_time,meter_status from ems_v1_completed.dbo.power_analysis_{month_year} 
                        ) cp
                    where 
                        cp.machine_id = {machine_id} and 
                        cp.created_on >='{current_date} 00:00:00' and cp.created_on <'{next_date} 00:00:00'
                    order by cp.id'''
                # createFolder("Logs/","current_date_time "+str(query1))

                data1 = cursor.execute(query1).fetchall()
                if len(data1)>0:
                    for row in data1:
                        start_kwh = row.machine_kwh
                        reverse_start_kwh = row.reverse_machine_kwh

                query2 = f'''
                    select 
                        top (1) cp.* 
                    from
                    (
                        select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,on_load_time,meter_status from ems_v1.dbo.current_power_analysis 
                        union all 
                        select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,on_load_time,meter_status from ems_v1_completed.dbo.power_analysis_{month_year} 
                        ) cp
                    where 
                        cp.machine_id = {machine_id} and 
                        cp.created_on >='{current_date} 00:00:00' and cp.created_on <'{next_date} 00:00:00'
                    order by cp.id desc
                    '''
                data2 = cursor.execute(query2).fetchall()
                if len(data2)>0:
                    for rows in data2:
                        end_kwh = rows.machine_kwh
                        reverse_end_kwh = rows.reverse_machine_kwh
                        meter_status  = rows.meter_status
                kwh = end_kwh - start_kwh
                reverse_kwh = reverse_end_kwh - reverse_start_kwh
                query_r = f'''
                select 
                    sum(cp.diff_on_load_time)  as diff_on_load_time,
                    cp.machine_id
                from 
                    (
                        select id,machine_id,created_on,diff_on_load_time from ems_v1.dbo.current_power_analysis 
                        WHERE machine_id = {machine_id} AND created_on >= '{current_date} 00:00:00' AND created_on < '{next_date} 00:00:00'
                        union all 
                        select id,machine_id,created_on,diff_on_load_time from ems_v1_completed.dbo.power_analysis_012024
                        WHERE machine_id = {machine_id} AND created_on >= '{current_date} 00:00:00' AND created_on < '{next_date} 00:00:00'
                    ) cp
                where
                    cp.machine_id = {machine_id} and 
                    cp.created_on >='{current_date} 00:00:00' and cp.created_on <'{next_date} 00:00:00'
                group by cp.machine_id'''
                createFolder("Logs/","query for run_time"+str(query_r))
                cursor.execute(query_r)
                data3 = cursor.fetchall()
                on_load_time = 0
                if len(data3)>0:
                    for run_time in data3:
                        on_load_time = run_time.diff_on_load_time
                createFolder("Logs/","on_load_time"+str(on_load_time))
                if on_load_time != 0:
                    on_load_time = on_load_time/3600

                query3 = f'''insert into [ems_v1_completed].[dbo].[power_{month_year}_12] (machine_id, mill_date , kwh, master_kwh, machine_kwh, reverse_kwh, reverse_master_kwh, reverse_machine_kwh,meter_status)
                             values({machine_id}, '{current_date}', {kwh}, {start_kwh}, {end_kwh},{reverse_kwh},{reverse_start_kwh},{reverse_end_kwh},{meter_status})'''
                createFolder("Logs/","query "+str(query3))
                cursor.execute(query3)
                cursor.commit()
                createFolder("Logs/","power 12to12 data inserted!")
                try:
                    query_erp = f''' insert into  [ERP_TNPL].[dbo].[TNPL_U2_EMS_SQL_TO_ORACLE](TIME_STAMP,TAG_NO,FEEDER_NAME,KWH,RUN_HR,AREA,SUB_AREA1,SUB_AREA2,KWHINTIAL,KWHFINAL,METER_STATUS)
                                    values('{current_date}','{meter_code}','{meter_name}','{kwh}','{on_load_time}','{location_name}','{function_name}','{function2_name}','{start_kwh}','{end_kwh}',{meter_status})'''
                    createFolder("Log_erp/","ERP query "+str(query_erp))
                    cursor.execute(query_erp)
                    cursor.commit()
                except Exception as e:
                    createFolder(f"Logs/",f"Error Updating ERP Data  ->> {e}")

        return current_date
     
    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Logs/","Issue in returning data "+error_message)

def manual_record():
    try:
        start_kwh = 0
        end_kwh = 0
        reverse_start_kwh = 0
        reverse_end_kwh = 0
        meter_status = 0
        
        query = f''' select date_time from ems_v1.dbo.manual_12to12 group by date_time order by date_time'''
        results =cursor.execute(query).fetchall()
        date_time_array = [row[0] for row in results]
        print(date_time_array)
        
        date_objects = [datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S") for date_time in date_time_array]

        next_dates = [(date_time + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") for date_time in date_objects]

        print(next_dates)
        month_year_array = [date_time[5:7] + date_time[:4] for date_time in date_time_array]
        print(month_year_array) 
      
        sql = "SELECT * FROM ems_v1.dbo.master_machine WHERE status = 'active'"
        cursor.execute(sql)
        machines_data = cursor.fetchall()
        for date_time, month_year, next_date in zip(date_time_array, month_year_array,next_dates):
            query1 = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'ems_v1_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'power_{month_year}_12'"""
            query1 = cursor.execute(query1).fetchall()
            
            if len(query1)>0: 
                pass
            else:
                create_table(month_year)
            for row in machines_data:
                machine_id = row.machine_id
            
                query1 = f'''
                    select 
                        top (1) cp.* 
                    from 
                        (
                            select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh from ems_v1.dbo.current_power_analysis 
                            union all 
                            select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh from ems_v1_completed.dbo.power_analysis_{month_year}
                        ) cp
                    where
                        cp.machine_id = {machine_id} 
                        and cp.created_on >='{date_time}' and cp.created_on <'{next_date}'
                    order by cp.id
                '''
                print(query1)
                cursor.execute(query1)
                
                data1 = cursor.fetchall()
                for row in data1:
                    start_kwh = row.machine_kwh
                    reverse_start_kwh = row.reverse_machine_kwh
                query2 = f'''
                    select 
                        top (1) cp.* 
                    from 
                        (
                            select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,meter_status from ems_v1.dbo.current_power_analysis 
                            union all 
                            select id,machine_id,mill_date,created_on,machine_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,meter_status from ems_v1_completed.dbo.power_analysis_{month_year}
                        ) cp
                    where
                        cp.machine_id = {machine_id} 
                        and cp.created_on >='{date_time}' and cp.created_on <'{next_date}'
                    order by cp.id desc
                '''
                cursor.execute(query2)
                data2 = cursor.fetchall()
                for rows in data2:
                    end_kwh = rows.machine_kwh
                    reverse_end_kwh = rows.reverse_machine_kwh
                    meter_status = rows.meter_status
                kwh = end_kwh - start_kwh
                reverse_kwh = reverse_end_kwh - reverse_start_kwh

                query3 = f'''
                        INSERT INTO [ems_v1_completed].[dbo].[power_{month_year}_12] (machine_id, mill_date, kwh, master_kwh, machine_kwh,reverse_kwh, reverse_master_kwh, reverse_machine_kwh,meter_status)
                        VALUES ({machine_id}, '{date_time}', {kwh}, {start_kwh}, {end_kwh},{reverse_kwh},{reverse_start_kwh},{reverse_end_kwh},{meter_status})
                    '''
                cursor.execute(query3)
                createFolder("Logs/","insert query "+str(query3))
                cursor.commit()

            query = f''' delete from ems_v1.dbo.manual_12to12 where date_time = '{date_time}' '''
            createFolder("Logs/","delete query "+str(query))
            cursor.execute(query)
            cursor.commit()
            
    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Logs/","Issue in returning data "+error_message)

def manual_record_erp():
    try:
        start_kwh = 0
        end_kwh = 0
        reverse_start_kwh = 0
        reverse_end_kwh = 0
        on_load_time = 0
        meter_status = 0
        
        query3 = ''
        data = ''
        current_date = '' 
        meter_name = ''   
        location_code = ''
        location_name = '' 
        function2_name = ''
        function1_name = ''
        function_code = ''
        createFolder("Log_erp/",f"ERP Manual Record Function Called... ")
        query = f''' select date_time from [ERP_TNPL].[dbo].[manual_ERP_data] group by date_time order by date_time'''
        results =cursor.execute(query).fetchall()
        createFolder("Log_erp/",f"query-{query} ")
        if len(results)>0:
            date_time_array = [row[0] for row in results]
            print(date_time_array)
            
            date_objects = [datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S") for date_time in date_time_array]

            next_dates = [(date_time + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S") for date_time in date_objects]

            print(next_dates)
            month_year_array = [date_time[5:7] + date_time[:4] for date_time in date_time_array]
            print(month_year_array) 
        
            sql = f''' 
                        select 
                            mm.*,
                            mmt.machinetype_code,
                            mmt.machinetype_name,
                            mf.function_code,
                            isnull(mf.function_name,'') as function_name,
                            mmf.function_code as function2_code,
                            isnull(mmf.function_name,'') as function2_name
                        from 
                            ems_v1.dbo.master_machine mm 
                            inner join ems_v1.dbo.master_machinetype mmt on mmt.machinetype_id = mm.machinetype_id
                            left join ems_v1.dbo.master_function mf on mf.function_id = mm.function_id 
                            left join ems_v1.dbo.master_function mmf on mmf.function_id = mm.function2_id
                        where mm.status = 'active' '''
            cursor.execute(sql)
            machines_data = cursor.fetchall()
            for date_time, month_year, next_date in zip(date_time_array, month_year_array,next_dates):
                
                sql = f" delete from [ERP_TNPL].[dbo].[TNPL_U2_EMS_SQL_TO_ORACLE] where TIME_STAMP = '{date_time}'"
                createFolder("Log_erp/","ERP Data Deleted-- "+str(sql))
                cursor.execute(sql)
                cursor.commit()

                for row in machines_data:
                    machine_id = row.machine_id
                    meter_code = row.machine_code
                    meter_name = row.machine_name
                    location_code = row.machinetype_code
                    location_name = row.machinetype_name
                    function_code = row.function_code
                    function_name = row.function_name
                    function2_code = row.function2_code
                    function2_name = row.function2_name
                    start_kwh = 0
                    reverse_start_kwh = 0
                    end_kwh = 0
                    reverse_end_kwh = 0
                    meter_status = 0
                    kwh = 0
                    reverse_kwh = 0
                    on_load_time = 0
                
                    query1 = f'''
                        select 
                            cp.* from ems_v1_completed.dbo.power_{month_year}_12 cp
                        where
                            cp.machine_id = {machine_id} 
                            and cp.mill_date ='{date_time}' 
                        order by cp.power_id
                    '''
                    createFolder("Log_erp/",f"query1-{query1} ")
                    cursor.execute(query1)
                    
                    data1 = cursor.fetchall()
                    for row in data1:
                        kwh = row.kwh
                        start_kwh = row.master_kwh
                        end_kwh = row.machine_kwh
                        meter_status = row.meter_status
                    query3 = f'''
                            select 
                                sum(cp.diff_on_load_time)  as diff_on_load_time,
                                cp.machine_id
                            from 
                                (
                                    select id,machine_id,created_on,diff_on_load_time from ems_v1.dbo.current_power_analysis 
                                    where
                                machine_id = {machine_id} 
                                and created_on >='{date_time}' and created_on <'{next_date}'
                                    union all 
                                    select id,machine_id,created_on,diff_on_load_time from ems_v1_completed.dbo.power_analysis_{month_year}
                                    where
                                machine_id = {machine_id} 
                                and created_on >='{date_time}' and created_on <'{next_date}'
                                ) cp
                            where
                                cp.machine_id = {machine_id} 
                                and cp.created_on >='{date_time}' and cp.created_on <'{next_date}'
                            group by cp.machine_id'''
                    cursor.execute(query3)
                    data3 = cursor.fetchall()
                    on_load_time = 0
                    if len(data3)>0:
                        for run_time in data3:
                            on_load_time = run_time.diff_on_load_time

                    if on_load_time != 0:
                        on_load_time = on_load_time/3600
                    
                    query_erp = f''' insert into  [ERP_TNPL].[dbo].[TNPL_U2_EMS_SQL_TO_ORACLE](TIME_STAMP,TAG_NO,FEEDER_NAME,KWH,RUN_HR,AREA,SUB_AREA1,SUB_AREA2,KWHINTIAL,KWHFINAL,METER_STATUS)
                                    values('{date_time}','{meter_code}','{meter_name}','{kwh}','{on_load_time}','{location_name}','{function_name}','{function2_name}','{start_kwh}','{end_kwh}','{meter_status}')'''
                    createFolder("Log_erp/","ERP Data Inserted-- "+str(query_erp))
                    cursor.execute(query_erp)
                    cursor.commit()

                query = f''' delete from [ERP_TNPL].[dbo].[manual_ERP_data] where date_time = '{date_time}' '''
                createFolder("Log_erp/","delete query-- "+str(query))
                print(query)
                cursor.execute(query)
                cursor.commit()
            
    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Logs/","Issue in returning data "+error_message)
   
def daily_energy_record_12to12(current_date):
    try:
        createFolder("Log/",f"daily_energy_record_12to12 Function Called ... ")
        res = ''
        energy_in_units = ''
        loss_percentage = ''
        day = 0
        
        current_time = datetime.now().strftime('%H:%M')
        # current_date = date.today()
        previous_date = current_date 

        report_for = '12to12'
        loss_record = 'insert'
        mill_date = previous_date
        
        res = {"date": mill_date, "report_for": report_for, "loss_record": loss_record}
        createFolder("Log/",f"previous_date: {mill_date} ")

        url = "http://localhost:5001/custom_daily_report/"
        response = requests.post(url, data=res, verify=False)

        if response.status_code == 200:
            response_data = response.json()
            createFolder("Log/",f"API response {response_data}")
        else:
            createFolder("Log/",f"Request failed with status code {response.status_code}")
    
        for i in response_data:
            energy_in_units = i["func_name"]
            day = i["formula_day"]
            day = abs(day)
            sql = f''' insert into ems_v1.dbo.loss_record (date,report_for,energy_in_units,day,created_on)
                        values ('{mill_date}','{report_for}','{energy_in_units}','{day}',getdate())'''
            cursor.execute(sql)
            cursor.commit()

        sql  = f'select * from [ems_v1].[dbo].[master_energy_calculations]'
        data = cursor.execute(sql).fetchall()
        
        for row in data :
            loss_percentage = row.loss_percentage
            energy_in_units = row.function_name
            query = f" update ems_v1.dbo.loss_record set loss_percentage = '{loss_percentage}' where energy_in_units = '{energy_in_units}' and date = '{mill_date}' "
            cursor.execute(query)
            cursor.commit()
        createFolder("Log/",f"record insert sucessfully... ")

    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in current_production"+error_message)

def daily_energy_record_12to12_erp():
    try:
        createFolder("Log_erp/",f"daily_energy_record_12to12_erp Function Called ... ")
        send_to_erp = ''
      
        loss_sql = f"select * from ems_v1.dbo.loss_record where report_for = '12to12' and  send_to_erp = 'yes' and process_status = 'no' "
        loss_data = cursor.execute(loss_sql).fetchall()
        if len(loss_data)>0:
            for ld in loss_data:
                date = ld.date
                report_for = ld.report_for
                energy_in_units = ld.energy_in_units
                day = ld.day
                loss_percentage = ld.loss_percentage
                loss_value = ld.loss_value
                id = ld.id
                sql = f''' insert into ERP_TNPL.dbo.loss_record (date,report_for,energy_in_units,day,loss_value,loss_percentage)
                            values('{date}','{report_for}','{energy_in_units}','{day}','{loss_value}','{loss_percentage}') '''
                createFolder("Log/",f"Loss Record Update query - {sql}")
                cursor.execute(sql)
                cursor.commit()
                createFolder("Log_erp/",f"ERP Loss Record Updated Sucessfully")
                query = f"update ems_v1.dbo.loss_record set  process_status = 'yes' where id = {id}"
                createFolder("Log_erp/",f"ERP Loss Record Close = {query}")
                cursor.execute(query)
                cursor.commit()

        else:
            createFolder("Log/",f"no data available (send_to_erp - {send_to_erp})... ")

    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in current_production"+error_message)


  
# DESKTOP-92NNMNK
#192.168.95.10 --tnpl
while True:
    try:
        conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='192.168.95.10',PORT='',DATABASE='ems_v1_completed',UID='sa',PWD='admin@2023')
        cursor = conn.cursor()
        response = day_record()
        print("response",response)
        manual_record()
        if response is not None and response != '':
            daily_energy_record_12to12(response)
        daily_energy_record_12to12_erp()
            
        manual_record_erp()
        conn.close()
        time.sleep(60)  
    
    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Logs/","Issue in returning data "+error_message)
       
       