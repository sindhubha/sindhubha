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

def daily_energy_record_6to6():
    try:
        createFolder("Log/",f"daily_energy_record_6to6 Function Called... ")
        res = ''
        energy_in_units = ''
        loss_percentage = ''
        day = 0
        
        current_time = datetime.now().strftime('%H:%M')

        query = f" select * from ems_v1.dbo.master_shifts"
        data = cursor.execute(query).fetchall()

        if len(data)>0:
            for row in data:
                mill_date = row.mill_date
                shift1_start_time = row.shift1_start_time

        mill_date = mill_date[:10]
        mill_date = datetime.strptime(mill_date, "%Y-%m-%d")

        previous_mill_date = mill_date - timedelta(days=1)

        shift1_start_time = shift1_start_time[10:16] 
        shift1_start_time = shift1_start_time.strip() 
        shift1_start_time = datetime.strptime(shift1_start_time, "%H:%M")

        # Add 5 minutes to the datetime
        shift1_start_time += timedelta(minutes=5)
        shift1_start_time_str = shift1_start_time.strftime("%H:%M")

        if current_time == shift1_start_time_str:
            report_for = '6to6'
            loss_record = 'insert'
            mill_date = previous_mill_date
            res = {"date": mill_date, "report_for": report_for, "loss_record": loss_record}
            createFolder("Log/",f"previous_mill_date: {mill_date} ")

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
                try:
                    day = abs(day)
                except Exception as e :
                    error_type = type(e).__name__
                    error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
                    error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
                    error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
                    createFolder("Log/","Issue in current_production"+error_message)
                    day = 0
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

def manual_daily_energy_record():
    try:
        createFolder("Log/",f"daily_energy_record_6to6 Function Called... ")
        res = ''
        energy_in_units = ''
        loss_percentage = ''
        day = 0
        query = f" select * from ems_v1.dbo.manual_loss_record"
        results =  cursor.execute(query).fetchall()
        print(results)
        if len(results)>0:
            for row in results:
                date = row.date_time
                print(date)
                report_for = row.report_for
                mill_date = date[:11]
                print(mill_date)
                loss_record = 'insert'
                res = {"date": mill_date, "report_for": report_for, "loss_record": loss_record}
            
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
                    try:
                        day = abs(day)
                    except Exception as e :
                        error_type = type(e).__name__
                        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
                        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
                        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
                        createFolder("Log/","Issue in current_production"+error_message)
                        day = 0
                    query = f'''select * from ems_v1.dbo.loss_record where report_for = '{report_for}' and date = '{mill_date}' and energy_in_units = '{energy_in_units}' '''
                    createFolder("Log/",f"query...{query} ")
                    res = cursor.execute(query).fetchall()
                    if len(res)>0:
                        pass
                    else:
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
                query = f''' delete from ems_v1.dbo.manual_loss_record where date_time = '{mill_date}' and report_for = '{report_for}' '''
                createFolder("Log/","delete query "+str(query))
                cursor.execute(query)
                cursor.commit()

    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in current_production"+error_message)
  
while True:
    try:
        conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='192.168.95.10',PORT='',DATABASE='ems_v1_completed',UID='sa',PWD='admin@2023')
        cursor = conn.cursor()
        daily_energy_record_6to6()            
        manual_daily_energy_record()
        conn.close()
        time.sleep(60)  
    
    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Logs/","Issue in returning data "+error_message)
       
       