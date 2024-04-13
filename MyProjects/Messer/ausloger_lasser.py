
import requests
import time
import traceback
#establishing connection with mssql server using pyodbc
from datetime import datetime, date, timedelta
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
import pytz

# def createFolder(directory, data):
#     date_time = datetime.now()
#     curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
#     curtime2 = date_time.strftime("%d-%m-%Y")

#     try:
#         # Get the path of the current script
#         base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

#         # Create the directory inside the user's file directory
#         directory = os.path.join(base_path, directory)
#         if not os.path.exists(directory):
#             os.makedirs(directory)

#         # Create the log file inside the directory
#         file_path = os.path.join(directory, f"{curtime2}.txt")
#         with open(file_path, "a+") as f:
#             f.write(f"{curtime1} {data}\r\n")
#     except OSError as e:
#         print(f"Error: Creating directory. {directory} - {e}")

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
        
# https://10.30.170.51/machine-data/
def current_production():
    try:
        createFolder("Log/","function called")
        #fetching data from api for updating current_production
        response = requests.get("https://10.30.170.51/machine-data/api/v1/machines/",verify=False)
        data = response.json()
        createFolder("Log/","API response"+str(data))
        for record in data:
            #checking status
            statedic ={}
            statedic['Idle']=0
            statedic['Working']=1
            state = record['State']

            if record['CommissionNumber']== '211001520':
                id=774
            else :
                id=785

            query = f"update messer_dt.dbo.current_production set machine_status = {statedic[state]},date_time=getdate() where machine_id={id}"
            createFolder("Log/","query"+str(query))
            cursor.execute(query)
            conn.commit()

            if cursor.rowcount>0:
                createFolder("Log/","updated current_production successfully")

    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in current_production"+error_message)

def date_shift_check(timestamp):

    try:
        conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='P-MUM-DFS-SQL-1\SQLEXPRESS',PORT='1433',DATABASE='messer_dt',UID='sa',PWD='Me$$er@DF')
        cursor = conn.cursor()
    except Exception as e:
        createFolder('Log/',f"Error Connecting Database in Shift_check File : {e} ")

    try:

        global act_shift
        global act_date
        global yesterday
        global shift1
        global shift2
        global shift3
        global curtime1
        global curtime
        global end_time
        global start_time 

        shift1_start = date.today()
        shift1_end = date.today()
        shift2_start = date.today()
        shift2_end = date.today()
        shift3_start = date.today()
        shift3_end = date.today()
        
        if type(timestamp) is not datetime:
            curtime = datetime.fromisoformat(timestamp)

        curtime1 = curtime.strftime("%Y-%m-%d %H:%M:%S")
        current_date = curtime.strftime("%Y-%m-%d")
        # current_date = datetime.fromisoformat(current_date)
       
        # cursor.execute(f'''SELECT * from master_shifts where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' ''' )
        query = f'''SELECT
            DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), shift1_start_time) AS shift1_start_time,
            DATEADD(MINUTE, shift_time1, DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), shift1_start_time)) AS shift1_end_time,
            DATEADD(MINUTE, shift_time1, DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), shift1_start_time)) AS shift2_start_time,
            DATEADD(MINUTE, shift_time1 + shift_time2, DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), shift1_start_time)) AS shift2_end_time,
            DATEADD(MINUTE, shift_time1 + shift_time2, DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), shift1_start_time)) AS shift3_start_time,
            DATEADD(MINUTE, shift_time1 + shift_time2 + shift_time3, DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), shift1_start_time)) AS shift3_end_time,
            DATEADD(DAY, DATEDIFF(DAY, '1900-01-01', '{curtime1}'), '1900-01-01') AS mill_date,
            mill_date as c_mill_date,
            mill_shift
        FROM
            messer_dt.dbo.master_shifts'''

        cursor.execute(query)
        rows = cursor.fetchall()
  
        for row in rows:

            shift1_start = row[0]
            shift1_end = row[1]

            shift2_start = row[2]
            shift2_end = row[3]

            shift3_start = row[4]
            shift3_end = row[5]
          
            current_mill_date = row[7]
            current_mill_shift = row[8]
       
        
        curtime1_dt = datetime.strptime(curtime1, "%Y-%m-%d %H:%M:%S")

        act_date = date.today()
        act_shift= 0
        start_time = date.today()
        end_time = date.today()
     
        if curtime1 >= shift1_start and curtime1<= shift1_end:
            act_shift = 1
            if curtime1 [:10] == shift1_start[:10]:
                act_date = curtime1[:10]
                start_time = shift1_start
                end_time = shift1_end

            elif curtime1 [:10] <= shift1_start[:10]:
                curtime1_dt = datetime.strptime(curtime1, "%Y-%m-%d %H:%M:%S")
                shift1_start = datetime.strptime(shift1_start, "%Y-%m-%d %H:%M:%S")
                shift1_end = datetime.strptime(shift1_end, "%Y-%m-%d %H:%M:%S")
                previous_day = curtime1_dt - timedelta(days=1)
                act_date = previous_day.strftime('%Y-%m-%d %H:%M:%S')
                act_date = act_date[:10]
                day = shift1_start - timedelta(days=1)
                e_day = shift1_end - timedelta(days=1)
                start_time = day.strftime('%Y-%m-%d %H:%M:%S')
                end_time = e_day.strftime('%Y-%m-%d %H:%M:%S')
            
        elif (curtime1[11:] >= '00:00:00' and curtime1[11:]<= shift2_end[11:]) or (curtime1 >= shift1_start and curtime1<= shift2_end):
        
            if curtime1 >= shift1_start and curtime1<= shift2_end:
                act_shift = 2
                if curtime1 [:10] == shift1_start[:10]:
                    act_date = curtime1[:10]
                    start_time = shift2_start
                    end_time = shift2_end

                elif curtime1 [:10] <= shift2_start[:10]:
                    curtime1_dt = datetime.strptime(curtime1, "%Y-%m-%d %H:%M:%S")
                    shift2_start = datetime.strptime(shift2_start, "%Y-%m-%d %H:%M:%S")
                    shift2_end = datetime.strptime(shift2_end, "%Y-%m-%d %H:%M:%S")
                    previous_day = curtime1_dt - timedelta(days=1)
                    act_date = previous_day.strftime('%Y-%m-%d %H:%M:%S')
                    act_date = act_date[:10]
                    day = shift2_start - timedelta(days=1)
                    e_day = shift2_end - timedelta(days=1)
                    start_time = day.strftime('%Y-%m-%d %H:%M:%S')
                    end_time = e_day.strftime('%Y-%m-%d %H:%M:%S')

            elif curtime1[11:] >= '00:00:00' and curtime1[11:]<= shift2_end[11:]:
                act_shift = 2
                if curtime1 [:10] == shift1_start[:10]:
                    curtime1_dt = datetime.strptime(curtime1, "%Y-%m-%d %H:%M:%S")
                    shift2_start = datetime.strptime(shift2_start, "%Y-%m-%d %H:%M:%S")
                    shift2_end = datetime.strptime(shift2_end, "%Y-%m-%d %H:%M:%S")
                    previous_day = curtime1_dt - timedelta(days=1)
                    act_date = previous_day.strftime('%Y-%m-%d %H:%M:%S')
                    act_date = act_date[:10]
                    day = shift2_start - timedelta(days=1)
                    e_day = shift2_end - timedelta(days=1)
                    start_time = day.strftime('%Y-%m-%d %H:%M:%S')
                    end_time = e_day.strftime('%Y-%m-%d %H:%M:%S')

        elif shift3_start[:10] == shift3_end[:10]:
            if curtime1 [11:]>= shift3_start[11:] and curtime1[11:]<= shift3_end[11:]:
                act_shift = 3
                if curtime1 [:10] == shift3_start[:10]:
                    act_date = curtime1[:10]
                    start_time = shift3_start
                    end_time = shift3_end

                elif curtime1 [:10] <= shift3_start[:10]:
                    curtime1_dt = datetime.strptime(curtime1, "%Y-%m-%d %H:%M:%S")
                    shift3_start = datetime.strptime(shift3_start, "%Y-%m-%d %H:%M:%S")
                    shift3_end = datetime.strptime(shift3_end, "%Y-%m-%d %H:%M:%S")
                    previous_day = curtime1_dt - timedelta(days=1)
                    act_date = previous_day.strftime('%Y-%m-%d %H:%M:%S')
                    act_date = act_date[:10]
                    day = shift3_start - timedelta(days=1)
                    e_day = shift3_end - timedelta(days=1)
                    start_time = day.strftime('%Y-%m-%d %H:%M:%S')
                    end_time = e_day.strftime('%Y-%m-%d %H:%M:%S')
        
        else:
        
            if curtime1 >= shift3_start and curtime1<= shift3_end:
                act_shift = 3
                if curtime1 [:10] == shift3_start[:10]:
                    act_date = curtime1[:10]
                    start_time = shift3_start
                    end_time = shift3_end

                elif curtime1 [:10] <= shift3_start[:10]:
                    curtime1_dt = datetime.strptime(curtime1, "%Y-%m-%d %H:%M:%S")
                    shift3_start = datetime.strptime(shift3_start, "%Y-%m-%d %H:%M:%S")
                    shift3_end = datetime.strptime(shift3_end, "%Y-%m-%d %H:%M:%S")
                    previous_day = curtime1_dt - timedelta(days=1)
                    act_date = previous_day.strftime('%Y-%m-%d %H:%M:%S')
                    act_date = act_date[:10]
                    day = shift3_start - timedelta(days=1)
                    e_day = shift3_end - timedelta(days=1)
                    start_time = day.strftime('%Y-%m-%d %H:%M:%S')
                    end_time = e_day.strftime('%Y-%m-%d %H:%M:%S')
            
        createFolder('Log/',f"Timestamp from database : {timestamp} .")
        createFolder('Log/',f"Mill_date & Mill_shift of Given timestamp : {act_date} , {act_shift} ,{start_time}, {end_time} .")

        return act_date,act_shift,start_time,end_time,current_mill_date,current_mill_shift
    except Exception as e:
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in returning data "+error_message)

def manual_loss_entry():
    try:
        select_query = f" select * from messer_dt.dbo.manual_loss_entry"
        loss_record = cursor.execute(select_query).fetchall()
        if len(loss_record)>0:
            for rec in loss_record:
                url_date = rec.date_time
                manual_rec_id = rec.id
                createFolder("LogM/","Manual Loss function called")
        
                response = requests.get(f"https://10.30.170.51/machine-data/api/v1/idle-reasons/{url_date}/{url_date}/",verify=False)
                data = response.json()
                createFolder("LogM/","API response"+str(data))
                if response is None:
                    createFolder("LogM/",f"No data Available {url_date}")
                else: 
                    data11 = ''
                    data22 = ''
                    data33 = ''
                    utc_zone = pytz.utc
                    indian_zone = pytz.timezone('Asia/Kolkata') 

                for record in data:
                    starttime = record['Data']['TimestampStart']
                    start_i = datetime.fromisoformat(starttime.replace('Z', '+00:00'))  # Add UTC timezone info
                    start_i = start_i.replace(tzinfo=None)  # Make the datetime object naive
                    utc_time = utc_zone.localize(start_i)
                    start = utc_time.astimezone(indian_zone)
                    time_start = start.strftime("%Y-%m-%d %H:%M:%S")
                    createFolder("LogM/", f"utc_time_s - {start_i}, indian_time = {time_start},start = {start}")
                    
                    endtime = record['Data']['TimestampEnd']
                    end_i = datetime.fromisoformat(endtime.replace('Z', '+00:00'))  # Add UTC timezone info
                    end_i = end_i.replace(tzinfo=None)
                    utc_time_e = utc_zone.localize(end_i)
                    end = utc_time_e.astimezone(indian_zone)
                    time_end = end.strftime("%Y-%m-%d %H:%M:%S")

                    createFolder("LogM/",f"utc_time_e - {end_i}, indian_time = {time_end},end = {end}")

                    code = record['Data']['IdleReasonId'] #csc
                
                    loss_code = 0
                    company_id = 0
                    branch_id = 0
                    department_id = 0

                    if code  == 1:
                        code = code
                    else:
                        code = code + 100000
                
                    sql1 = f'''select * from messer_dt.dbo.master_loss_code where loss_code = '{code}' '''
                    data1 = cursor.execute(sql1).fetchall()
                    createFolder("LogM/","data1"+str(data1))
                    if len(data1)>0:
                        for row in data1:
                            loss_code = row.loss_code

                    loss_duration1 = 0
                    loss_duration2 = 0
                    loss_duration3 = 0

                    date2_shift_start = date.today()
                    date2_shift_end = date.today()
                    date1_shift_start = date.today()
                    date1_shift_end = date.today()

                    act_date_end = date.today()
                    act_shift_end = 0
                    act_c_shift_start_time1 = ''
                    act_c_shift_start_time2 = ''
                    act_c_shift_start_time3 = ''
                    current_data = ''
                    act_date_start = date.today()
                    act_shift_start = 0

                    data2 = date_shift_check(time_end)
            
                    if len(data2)>0:  
                        act_date_end = data2[0]
                        act_shift_end = data2[1]
                        date2_shift_start = data2[2]
                        date2_shift_end = data2[3]
                        current_mill_date = data2[4]
                        current_mill_shift = data2[5]

                    data3 = date_shift_check(time_start)
                
                    if len(data3)>0:   
                        act_date_start = data3[0] 
                        act_shift_start = data3[1]
                        date1_shift_start = data3[2]
                        date1_shift_end = data3[3]
                        current_mill_date = data3[4]
                        current_mill_shift = data3[5]
                    sel_query = f"select * from messer_dt.dbo.master_shifts"
                    shift_data = cursor.execute(sel_query).fetchall()
                    if len(shift_data)>0:
                        for shift in shift_data:
                            s_time1 = shift.shift_time1
                            s_time2 = shift.shift_time2
                            s_time3 = shift.shift_time3
                    else:
                        createFolder("LogM/","NO data in master shift table")

                    if act_date_start == act_date_end:
                        
                        if act_shift_start == act_shift_end:
                            loss_duration1 = (end-start).total_seconds()
                            start_time1 = time_start
                            end_time1 = time_end
                            act_date1 = act_date_start
                            act_shift1 = act_shift_start
                            act_c_shift_start_time1 = date1_shift_start

                        elif act_shift_start == 1 and act_shift_end == 3:
                            if time_start >= date1_shift_start and time_start<= date1_shift_end:
                                end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                                loss_duration1 = (end - start).total_seconds()
                                start_time1 = time_start
                                end_time1 = date1_shift_end
                                act_date1 = act_date_start
                                act_shift1 = act_shift_start
                                act_c_shift_start_time1 = date1_shift_start
                            
                            if date1_shift_end<=date2_shift_start:
                                end = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                                loss_duration2 = (end - start).total_seconds()
                                start_time2 = date1_shift_end
                                end_time2 = date2_shift_start
                                act_date2 = act_date_start
                                act_shift2 = 2
                                act_c_shift_start_time2 = date2_shift_start

                            if time_end >= date2_shift_start and time_end<= date2_shift_end:
                                end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                                loss_duration3 = (end - start).total_seconds()
                                start_time3 = date2_shift_start
                                end_time3 = time_end
                                act_date3 = act_date_end
                                act_shift3 = act_shift_end
                                act_c_shift_start_time3 = date2_shift_start

                        else:       
                    
                            if time_start >= date1_shift_start and time_start<= date1_shift_end:
                                end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                                loss_duration1 = (end - start).total_seconds()
                                start_time1 = time_start
                                end_time1 = date1_shift_end
                                act_date1 = act_date_start
                                act_shift1 = act_shift_start
                                act_c_shift_start_time1 = date1_shift_start

                            if time_end >= date2_shift_start and time_end<= date2_shift_end:
                                end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                                loss_duration2 = (end - start).total_seconds()
                                start_time2 = date2_shift_start
                                end_time2 = time_end
                                act_date2 = act_date_end
                                act_shift2 = act_shift_end
                                act_c_shift_start_time2 = date2_shift_start
        
                    else:
                        
                        if act_shift_end == 1:
                            if time_start >= date1_shift_start and time_start<= date1_shift_end:
                                end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                                loss_duration1 = (end - start).total_seconds()
                                start_time1 = time_start
                                end_time1 = date1_shift_end
                                act_date1 = act_date_start
                                act_shift1 = act_shift_start
                                act_c_shift_start_time1 = date1_shift_start

                            if time_end >= date2_shift_start and time_end<= date2_shift_end:
                                end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                                start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                                loss_duration2 = (end - start).total_seconds()
                                start_time2 = date2_shift_start
                                end_time2 = time_end
                                act_date2 = act_date_end
                                act_shift2 = act_shift_end   
                                act_c_shift_start_time2 = date2_shift_start
                        else:
                            createFolder("LogM/",f"Condition not match kindly check the loss timestamp")
                
                    if record['CommissionNumber']== '211001520':    
                        id=774
                    else :
                        id=785

                    sql2 = f'''select * from messer_dt.dbo.master_machine where machine_id = {id} '''
                    datas= cursor.execute(sql2).fetchall()
                    if len(datas)>0:
                        for r in datas:
                            company_id = r.company_id
                            department_id = r.department_id

                    loss_duration1 = int(loss_duration1)
                    loss_duration2 = int(loss_duration2)
                    loss_duration3 = int(loss_duration3)
                    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}                 
                    current_time = datetime.now()
                    current_mill_date = datetime.strptime(current_mill_date, '%Y-%m-%d %H:%M:%S')
                    types1 = type(current_mill_date)
                    createFolder("LogM/",f"types1-{types1}")
                
                    if loss_duration1 != 0:  #checking if the record already exists in db before inserting
                        createFolder("LogM/",f"Loss_duration1-{loss_duration1}")

                        sql1 = f"select * from messer_dt.dbo.current_loss where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time1}' and current_stop_duration={loss_duration1} "
                        data1 = cursor.execute(sql1).fetchall()
                    
                        if len(data1) >0:
                            pass
                        else: 
                            month_year=act_date1[5:7]+act_date1[:4]
                            table_name=f"[messer_dt_completed].[dbo].[loss_{month_year}]"
                            
                            query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'messer_dt_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'loss_{month_year}' """
                        
                            result_query = cursor.execute(query).fetchall()
                            if len(result_query)>0:
                                sql11 = f'''select * from {table_name} where  machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time1}' and current_stop_duration={loss_duration1}'''
                                data11=cursor.execute(sql11).fetchall()
                            if len(data11)!=0:
                                pass
                            else:
                                
                                query = f"insert into [messer_dt_completed].[dbo].[loss_{month_year}](company_id,branch_id,department_id,machine_id,current_stop_code,current_stop_begin_time,run_begin_time, current_stop_duration,mill_date,mill_shift,product_id,operator_id) values({company_id},{branch_id},{department_id},{id},'{loss_code}','{start_time1}','{end_time1}',{loss_duration1},'{act_date1}','{act_shift1}',7,2)"
                                createFolder("LogM/","query for loss_duration1 "+str(query))
                                cursor.execute(query)
                                conn.commit()
                                try:
                                    act_date1 = datetime.strptime(act_date1, '%Y-%m-%d')
                                    if current_mill_date == act_date1 and int(current_mill_shift) == int(act_shift1):
                                    
                                        cur_query = f"select sum(current_stop_duration) as current_stop_duration from messer_dt.dbo.current_loss where mill_date = '{act_date1}' and mill_shift = '{act_shift1}' and machine_id = '{id}'  group by machine_id"  
                                        createFolder("LogM/","sel query "+str(cur_query))
                                        current_data = cursor.execute(cur_query).fetchall()   
                                        c_end = current_time
                                        c_start = datetime.strptime(act_c_shift_start_time1, '%Y-%m-%d %H:%M:%S')
                                        actual_duration = (c_end - c_start).total_seconds()
                                        actual_duration = int(actual_duration)
                                        p_table_name = F"messer_dt.dbo.current_production"
                                        if len(current_data)>0:
                                            for data in current_data:
                                                current_stop_duration = data.current_stop_duration
                                        
                                    else:
                                        cur_query = f'''select 
                                                            sum(cp.current_stop_duration) as current_stop_duration
                                                        from 
                                                            (select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt.dbo.current_loss 
                                                            union all select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt_completed.dbo.loss_{month_year}) cp  
                                                        where 
                                                            cp.mill_date = '{act_date1}' and cp.mill_shift = '{act_shift1}' and cp.machine_id = '{id}'  group by cp.machine_id'''  
                                        createFolder("LogM/","sel query "+str(cur_query))
                                        current_data = cursor.execute(cur_query).fetchall()   
                                        if act_shift1 == 1 :
                                            actual_duration = s_time1*60
                                        elif act_shift1 == 2:
                                            actual_duration = s_time2*60
                                        elif act_shift1 == 3:
                                            actual_duration = s_time3*60
                                        p_table_name = f"[messer_dt_completed].[dbo].production_{month_year}"
                                        if len(current_data)>0:
                                            for data in current_data:
                                                current_stop_duration = data.current_stop_duration                                                
                                    
                                    run_duration = actual_duration - current_stop_duration 
                                    update_query = f" update {p_table_name} set run_time = '{run_duration}', loss_time_1 = '{current_stop_duration}' where mill_date = '{act_date1}' and mill_shift = '{act_shift1}' and machine_id = '{id}'"
                                    createFolder("LogM/","query for production update "+str(update_query))
                                    cursor.execute(update_query)
                                    conn.commit()
                                except Exception as e :
                                    createFolder("LogM/",f"error in updating production table-{e} ")

                    if loss_duration2 != 0:
                        createFolder("LogM/",f"Loss_duration2-{loss_duration2}")
                        sql2 = f"select * from messer_dt.dbo.current_loss where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time2}' and current_stop_duration={loss_duration2} "
                    
                        data2 = cursor.execute(sql2).fetchall()
                        
                        if len(data2) >0:
                            pass
                        else:
                            month_year=act_date2[5:7]+act_date2[:4]
                            table_name=f"[messer_dt_completed].[dbo].[loss_{month_year}]"
                            query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'messer_dt_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'loss_{month_year}' """
                            result_query = cursor.execute(query).fetchall()
                            if len(result_query)>0:
                                sql22 = f'''select * from {table_name} where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time2}' and current_stop_duration={loss_duration2}'''
                                data22=cursor.execute(sql22).fetchall()
                            if len(data22)!=0:
                                pass
                            else:
                            
                                query = f"insert into [messer_dt_completed].[dbo].[loss_{month_year}](company_id,branch_id,department_id,machine_id,current_stop_code,current_stop_begin_time,run_begin_time, current_stop_duration,mill_date,mill_shift,product_id,operator_id) values({company_id},{branch_id},{department_id},{id},'{loss_code}','{start_time2}','{end_time2}',{loss_duration2},'{act_date2}','{act_shift2}',7,2)"
                                createFolder("LogM/","query for loss_duration2 "+str(query))
                                cursor.execute(query)
                                conn.commit()
                                try:
                                    act_date2 = datetime.strptime(act_date2, '%Y-%m-%d')
                                    ac_tpe = type(act_date2)
                                    
                                    if current_mill_date == act_date2 and int(current_mill_shift) == int(act_shift2):
                                    
                                        cur_query = f"select sum(current_stop_duration) as current_stop_duration from messer_dt.dbo.current_loss where mill_date = '{act_date2}' and mill_shift = '{act_shift2}' and machine_id = '{id}'  group by machine_id"  
                                        createFolder("LogM/","sel query "+str(cur_query))
                                        current_data = cursor.execute(cur_query).fetchall()   
                                        c_end = current_time
                                        c_start = datetime.strptime(act_c_shift_start_time2, '%Y-%m-%d %H:%M:%S')
                                        actual_duration = (c_end - c_start).total_seconds()
                                        actual_duration = int(actual_duration)
                                        p_table_name = F"messer_dt.dbo.current_production"
                                        if len(current_data)>0:
                                            for data in current_data:
                                                current_stop_duration = data.current_stop_duration
                                    else:
                                        cur_query = f'''select 
                                                            sum(cp.current_stop_duration) as current_stop_duration
                                                        from 
                                                            (select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt.dbo.current_loss 
                                                            union all select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt_completed.dbo.loss_{month_year}) cp  
                                                        where 
                                                            cp.mill_date = '{act_date2}' and cp.mill_shift = '{act_shift2}' and cp.machine_id = '{id}'  group by cp.machine_id'''  
                                        createFolder("LogM/","sel query "+str(cur_query))
                                        current_data = cursor.execute(cur_query).fetchall()   
                                        if act_shift2 == 1 :
                                            actual_duration = s_time1*60
                                        elif act_shift2 == 2:
                                            actual_duration = s_time2*60
                                        elif act_shift2 == 3:
                                            actual_duration = s_time3*60
                                        p_table_name = f"[messer_dt_completed].[dbo].production_{month_year}"
                                        if len(current_data)>0:
                                            for data in current_data:
                                                current_stop_duration = data.current_stop_duration
                                                
                                    run_duration = actual_duration - current_stop_duration 
                                    update_query = f" update {p_table_name} set run_time = '{run_duration}', loss_time_1 = '{current_stop_duration}' where mill_date = '{act_date2}' and mill_shift = '{act_shift2}' and machine_id = '{id}'"
                                    createFolder("LogM/","query for production update "+str(update_query))
                                    cursor.execute(update_query)
                                    conn.commit()
                                except Exception as e :
                                    createFolder("LogM/",f"error in updating production table-{e} ")

                    if loss_duration3 != 0:  
                        createFolder("LogM/",f"Loss_duration3-{loss_duration3}")
                        sql3 = f"select * from messer_dt.dbo.current_loss where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time3}' and current_stop_duration={loss_duration3} "
                        data3 = cursor.execute(sql3).fetchall()
                    
                        if len(data3)> 0:
                            pass
                            
                        else:
                            month_year = act_date3[5:7]+act_date3[:4]
                            table_name=f"[messer_dt_completed].[dbo].[loss_{month_year}]"
                            query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'messer_dt_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'loss_{month_year}' """
                            
                            result_query = cursor.execute(query).fetchall()
                            if len(result_query)>0:
                                sql33 = f'''select * from {table_name} where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time3}' and current_stop_duration={loss_duration3} '''
                                data33=cursor.execute(sql33).fetchall()
                            if len(data33)!=0:
                                pass
                            else: 
                                
                                query = f"insert into [messer_dt_completed].[dbo].[loss_{month_year}](company_id,branch_id,department_id,machine_id,current_stop_code,current_stop_begin_time,run_begin_time, current_stop_duration,mill_date,mill_shift,product_id,operator_id) values({company_id},{branch_id},{department_id},{id},'{loss_code}','{start_time3}','{end_time3}',{loss_duration3},'{act_date3}','{act_shift3}',7,2)"
                                createFolder("LogM/","query for loss_duration3 "+str(query))
                                cursor.execute(query)
                                conn.commit()
                                try:
                                    act_date3 = datetime.strptime(act_date3, '%Y-%m-%d')
                                    if current_mill_date == act_date3 and int(current_mill_shift) == int(act_shift3):
                                    
                                        cur_query = f"select sum(current_stop_duration) as current_stop_duration from messer_dt.dbo.current_loss where mill_date = '{act_date3}' and mill_shift = '{act_shift3}' and machine_id = '{id}'  group by machine_id"  
                                        createFolder("LogM/","sel query "+str(cur_query))
                                        current_data = cursor.execute(cur_query).fetchall()   
                                        c_end = current_time
                                        c_start = datetime.strptime(act_c_shift_start_time3, '%Y-%m-%d %H:%M:%S')
                                        actual_duration = (c_end - c_start).total_seconds()
                                        actual_duration = int(actual_duration)
                                        p_table_name = F"messer_dt.dbo.current_production"
                                        if len(current_data)>0:
                                            for data in current_data:
                                                current_stop_duration = data.current_stop_duration
                                    else:
                                        cur_query = f'''select 
                                                            sum(cp.current_stop_duration) as current_stop_duration
                                                        from 
                                                            (select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt.dbo.current_loss 
                                                            union all select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt_completed.dbo.loss_{month_year}) cp  
                                                        where 
                                                            cp.mill_date = '{act_date3}' and cp.mill_shift = '{act_shift3}' and cp.machine_id = '{id}'  group by cp.machine_id'''  
                                        createFolder("LogM/","sel query "+str(cur_query))
                                        current_data = cursor.execute(cur_query).fetchall()   
                                        if act_shift3 == 1 :
                                            actual_duration = s_time1*60
                                        elif act_shift3 == 2:
                                            actual_duration = s_time2*60
                                        elif act_shift3 == 3:
                                            actual_duration = s_time3*60
                                        p_table_name = f"[messer_dt_completed].[dbo].production_{month_year}"
                                        if len(current_data)>0:
                                            for data in current_data:
                                                current_stop_duration = data.current_stop_duration
                                                
                                    run_duration = actual_duration - current_stop_duration 
                                    update_query = f" update {p_table_name} set run_time = '{run_duration}', loss_time_1 = '{current_stop_duration}' where mill_date = '{act_date3}' and mill_shift = '{act_shift3}' and machine_id = '{id}'"
                                    createFolder("LogM/","query for production update "+str(update_query))
                                    cursor.execute(update_query)
                                    conn.commit()
                                except Exception as e :
                                    createFolder("LogM/",f"error in updating production table-{e} ")
                
                del_query = f"delete from messer_dt.dbo.manual_loss_entry where id = {manual_rec_id}"
                cursor.execute(del_query)
                conn.commit()

    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("LogM/","Issue in returning data "+error_message)
   

def current_loss():
    try:
        createFolder("Log/","current_loss function called")
        response = requests.get("https://10.30.170.51/machine-data/api/v1/idle-reasons/",verify=False)
        data = response.json()
        createFolder("Log/","API response"+str(data))
        data11 = ''
        data22 = ''
        data33 = ''
        utc_zone = pytz.utc
        indian_zone = pytz.timezone('Asia/Kolkata') 

        for record in data:
            starttime = record['Data']['TimestampStart']
            start_i = datetime.fromisoformat(starttime.replace('Z', '+00:00'))  # Add UTC timezone info
            start_i = start_i.replace(tzinfo=None)  # Make the datetime object naive
            utc_time = utc_zone.localize(start_i)
            start = utc_time.astimezone(indian_zone)
            time_start = start.strftime("%Y-%m-%d %H:%M:%S")
            createFolder("Log/", f"utc_time_s - {start_i}, indian_time = {time_start},start = {start}")
            
            endtime = record['Data']['TimestampEnd']
            end_i = datetime.fromisoformat(endtime.replace('Z', '+00:00'))  # Add UTC timezone info
            end_i = end_i.replace(tzinfo=None)
            utc_time_e = utc_zone.localize(end_i)
            end = utc_time_e.astimezone(indian_zone)
            time_end = end.strftime("%Y-%m-%d %H:%M:%S")

            createFolder("Log/",f"utc_time_e - {end_i}, indian_time = {time_end},end = {end}")

            code = record['Data']['IdleReasonId'] #csc
        
            loss_code = 0
            company_id = 0
            branch_id = 0
            department_id = 0

            if code  == 1:
                code = code
            else:
                code = code + 100000
         
            sql1 = f'''select * from messer_dt.dbo.master_loss_code where loss_code = '{code}' '''
            data1 = cursor.execute(sql1).fetchall()
            createFolder("Log/","data1"+str(data1))
            if len(data1)>0:
                for row in data1:
                    loss_code = row.loss_code

            loss_duration1 = 0
            loss_duration2 = 0
            loss_duration3 = 0

            date2_shift_start = date.today()
            date2_shift_end = date.today()
            date1_shift_start = date.today()
            date1_shift_end = date.today()

            act_date_end = date.today()
            act_shift_end = 0
            act_c_shift_start_time1 = ''
            act_c_shift_start_time2 = ''
            act_c_shift_start_time3 = ''
            current_data = ''
            act_date_start = date.today()
            act_shift_start = 0

            data2 = date_shift_check(time_end)
       
            if len(data2)>0:  
                act_date_end = data2[0]
                act_shift_end = data2[1]
                date2_shift_start = data2[2]
                date2_shift_end = data2[3]
                current_mill_date = data2[4]
                current_mill_shift = data2[5]

            data3 = date_shift_check(time_start)
        
            if len(data3)>0:   
                act_date_start = data3[0] 
                act_shift_start = data3[1]
                date1_shift_start = data3[2]
                date1_shift_end = data3[3]
                current_mill_date = data3[4]
                current_mill_shift = data3[5]
            sel_query = f"select * from messer_dt.dbo.master_shifts"
            shift_data = cursor.execute(sel_query).fetchall()
            if len(shift_data)>0:
                for shift in shift_data:
                    s_time1 = shift.shift_time1
                    s_time2 = shift.shift_time2
                    s_time3 = shift.shift_time3
            else:
                createFolder("Log/","NO data in master shift able")

            if act_date_start == act_date_end:
                if act_shift_start == 1:
                    if act_shift_start == act_shift_end:
                        loss_duration1 = (end-start).total_seconds()
                        start_time1 = time_start
                        end_time1 = time_end
                        act_date1 = act_date_start
                        act_shift1 = act_shift_start
                        act_c_shift_start_time1 = date1_shift_start

                    elif act_shift_start == 1 and act_shift_end == 3:
                        if time_start >= date1_shift_start and time_start<= date1_shift_end:
                            end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                            loss_duration1 = (end - start).total_seconds()
                            start_time1 = time_start
                            end_time1 = date1_shift_end
                            act_date1 = act_date_start
                            act_shift1 = act_shift_start
                            act_c_shift_start_time1 = date1_shift_start
                        
                        if date1_shift_end<=date2_shift_start:
                            end = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                            loss_duration2 = (end - start).total_seconds()
                            start_time2 = date1_shift_end
                            end_time2 = date2_shift_start
                            act_date2 = act_date_start
                            act_shift2 = 2
                            act_c_shift_start_time2 = date2_shift_start

                        if time_end >= date2_shift_start and time_end<= date2_shift_end:
                            end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                            loss_duration3 = (end - start).total_seconds()
                            start_time3 = date2_shift_start
                            end_time3 = time_end
                            act_date3 = act_date_end
                            act_shift3 = act_shift_end
                            act_c_shift_start_time3 = date2_shift_start


                    else:       
                
                        if time_start >= date1_shift_start and time_start<= date1_shift_end:
                            end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                            loss_duration1 = (end - start).total_seconds()
                            start_time1 = time_start
                            end_time1 = date1_shift_end
                            act_date1 = act_date_start
                            act_shift1 = act_shift_start
                            act_c_shift_start_time1 = date1_shift_start

                        if time_end >= date2_shift_start and time_end<= date2_shift_end:
                            end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                            loss_duration2 = (end - start).total_seconds()
                            start_time2 = date2_shift_start
                            end_time2 = time_end
                            act_date2 = act_date_end
                            act_shift2 = act_shift_end
                            act_c_shift_start_time2 = date2_shift_start

                elif act_shift_start == 2:
                    if act_shift_start == act_shift_end:
                        loss_duration2 = (end-start).total_seconds()
                        start_time2 = time_start
                        end_time2 = time_end
                        act_date2 = act_date_start
                        act_shift2 = act_shift_start
                        act_c_shift_start_time2 = date2_shift_start

                    else:
                        if time_start>=date1_shift_start and time_start<= date1_shift_end:
                            end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                            loss_duration2 = (end - start).total_seconds()
                            start_time2 = time_start
                            end_time2 = date1_shift_end
                            act_date2 = act_date_start
                            act_shift2 = act_shift_start
                            act_c_shift_start_time2 = date1_shift_start

                        if time_end >= date2_shift_start and time_end<= date2_shift_end:
                            
                            end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                            start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                            loss_duration3 = (end - start).total_seconds()
                            start_time3 = date2_shift_start
                            end_time3 = time_end
                            act_date3 = act_date_end
                            act_shift3 = act_shift_end
                            act_c_shift_start_time3 = date2_shift_start

                elif act_shift_start == 3:
                    if act_shift_start == act_shift_end:
                        loss_duration3 = (end-start).total_seconds()
                        start_time3 = time_start
                        end_time3 = time_end
                        act_date3 = act_date_start
                        act_shift3 = act_shift_start
                        act_c_shift_start_time3 = date2_shift_start
            else:
                
                if act_shift_end == 1:
                    if time_start >= date1_shift_start and time_start<= date1_shift_end:
                        end = datetime.strptime(date1_shift_end, '%Y-%m-%d %H:%M:%S')
                        start = datetime.strptime(time_start, '%Y-%m-%d %H:%M:%S')
                        loss_duration1 = (end - start).total_seconds()
                        start_time1 = time_start
                        end_time1 = date1_shift_end
                        act_date1 = act_date_start
                        act_shift1 = act_shift_start
                        act_c_shift_start_time1 = date1_shift_start

                    if time_end >= date2_shift_start and time_end<= date2_shift_end:
                        end = datetime.strptime(time_end, '%Y-%m-%d %H:%M:%S')
                        start = datetime.strptime(date2_shift_start, '%Y-%m-%d %H:%M:%S')
                        loss_duration2 = (end - start).total_seconds()
                        start_time2 = date2_shift_start
                        end_time2 = time_end
                        act_date2 = act_date_end
                        act_shift2 = act_shift_end   
                        act_c_shift_start_time2 = date2_shift_start
                else:
                    createFolder("Log/",f"Condition not match kindly check the loss timestamp")
           
            if record['CommissionNumber']== '211001520':    
                id=774
            else :
                id=785

            sql2 = f'''select * from messer_dt.dbo.master_machine where machine_id = {id} '''
            datas= cursor.execute(sql2).fetchall()
            if len(datas)>0:
                for r in datas:
                    company_id = r.company_id
                    department_id = r.department_id

            loss_duration1 = int(loss_duration1)
            loss_duration2 = int(loss_duration2)
            loss_duration3 = int(loss_duration3)
            mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}                 
            current_time = datetime.now()
            current_mill_date = datetime.strptime(current_mill_date, '%Y-%m-%d %H:%M:%S')
            types1 = type(current_mill_date)
            createFolder("Log/",f"types1-{types1}")
        
            if loss_duration1 != 0:  #checking if the record already exists in db before inserting
                createFolder("Log/",f"Loss_duration1-{loss_duration1}")

                sql1 = f"select * from messer_dt.dbo.current_loss where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time1}' and current_stop_duration={loss_duration1} "
                data1 = cursor.execute(sql1).fetchall()
               
                if len(data1) >0:
                    pass
                else: 
                    month_year=act_date1[5:7]+act_date1[:4]
                    table_name=f"[messer_dt_completed].[dbo].[loss_{month_year}]"
                    
                    query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'messer_dt_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'loss_{month_year}' """
                   
                    result_query = cursor.execute(query).fetchall()
                    if len(result_query)>0:
                        sql11 = f'''select * from {table_name} where  machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time1}' and current_stop_duration={loss_duration1}'''
                        data11=cursor.execute(sql11).fetchall()
                    if len(data11)!=0:
                        pass
                    else:
                        
                        query = f"insert into messer_dt.dbo.current_loss(company_id,branch_id,department_id,machine_id,current_stop_code,current_stop_begin_time,run_begin_time, current_stop_duration,mill_date,mill_shift,product_id,operator_id) values({company_id},{branch_id},{department_id},{id},'{loss_code}','{start_time1}','{end_time1}',{loss_duration1},'{act_date1}','{act_shift1}',7,2)"
                        createFolder("Log/","query for loss_duration1 "+str(query))
                        cursor.execute(query)
                        conn.commit()
                        try:
                            act_date1 = datetime.strptime(act_date1, '%Y-%m-%d')
                            if current_mill_date == act_date1 and int(current_mill_shift) == int(act_shift1):
                            
                                cur_query = f"select sum(current_stop_duration) as current_stop_duration from messer_dt.dbo.current_loss where mill_date = '{act_date1}' and mill_shift = '{act_shift1}' and machine_id = '{id}'  group by machine_id"  
                                createFolder("Log/","query for production sel query "+str(cur_query))
                                current_data = cursor.execute(cur_query).fetchall()   
                                c_end = current_time
                                c_start = datetime.strptime(act_c_shift_start_time1, '%Y-%m-%d %H:%M:%S')
                                actual_duration = (c_end - c_start).total_seconds()
                                actual_duration = int(actual_duration)
                                p_table_name = F"messer_dt.dbo.current_production"
                                if len(current_data)>0:
                                    for data in current_data:
                                        current_stop_duration = data.current_stop_duration
                                
                            else:
                                cur_query = f'''select 
                                                    sum(cp.current_stop_duration) as current_stop_duration
                                                from 
                                                    (select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt.dbo.current_loss 
                                                    union all select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt_completed.dbo.loss_{month_year}) cp  
                                                where 
                                                    cp.mill_date = '{act_date1}' and cp.mill_shift = '{act_shift1}' and cp.machine_id = '{id}'   group by cp.machine_id'''  
                                createFolder("Log/","query for production sel query "+str(cur_query))
                                current_data = cursor.execute(cur_query).fetchall()   
                                if act_shift1 == 1 :
                                    actual_duration = s_time1*60
                                elif act_shift1 == 2:
                                    actual_duration = s_time2*60
                                elif act_shift1 == 3:
                                    actual_duration = s_time3*60
                                p_table_name = f"[messer_dt_completed].[dbo].production_{month_year}"
                                if len(current_data)>0:
                                    for data in current_data:
                                        current_stop_duration = data.current_stop_duration
                                        current_stop_duration = current_stop_duration 
                                
                           
                            
                            run_duration = actual_duration - current_stop_duration 
                            update_query = f" update {p_table_name} set run_time = '{run_duration}', loss_time_1 = '{current_stop_duration}' where mill_date = '{act_date1}' and mill_shift = '{act_shift1}' and machine_id = '{id}'"
                            createFolder("Log/","query for production update "+str(update_query))
                            cursor.execute(update_query)
                            conn.commit()
                        except Exception as e :
                            createFolder("Log/",f"error in updating production table-{e} ")

            if loss_duration2 != 0:
                createFolder("Log/",f"Loss_duration2-{loss_duration2}")
                sql2 = f"select * from messer_dt.dbo.current_loss where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time2}' and current_stop_duration={loss_duration2} "
              
                data2 = cursor.execute(sql2).fetchall()
                
                if len(data2) >0:
                    pass
                else:
                    month_year=act_date2[5:7]+act_date2[:4]
                    table_name=f"[messer_dt_completed].[dbo].[loss_{month_year}]"
                    query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'messer_dt_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'loss_{month_year}' """
                    result_query = cursor.execute(query).fetchall()
                    if len(result_query)>0:
                        sql22 = f'''select * from {table_name} where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time2}' and current_stop_duration={loss_duration2}'''
                        data22=cursor.execute(sql22).fetchall()
                    if len(data22)!=0:
                        pass
                    else:
                       
                        query = f"insert into messer_dt.dbo.current_loss(company_id,branch_id,department_id,machine_id,current_stop_code,current_stop_begin_time,run_begin_time, current_stop_duration,mill_date,mill_shift,product_id,operator_id) values({company_id},{branch_id},{department_id},{id},'{loss_code}','{start_time2}','{end_time2}',{loss_duration2},'{act_date2}','{act_shift2}',7,2)"
                        createFolder("Log/","query for loss_duration2 "+str(query))
                        cursor.execute(query)
                        conn.commit()
                        try:
                            act_date2 = datetime.strptime(act_date2, '%Y-%m-%d')
                            ac_tpe = type(act_date2)
                            createFolder("Log/",f"ac_tpe-{ac_tpe}")
                            if current_mill_date == act_date2 and int(current_mill_shift) == int(act_shift2):
                            
                                cur_query = f"select sum(current_stop_duration) as current_stop_duration from messer_dt.dbo.current_loss where mill_date = '{act_date2}' and mill_shift = '{act_shift2}' and machine_id = '{id}'  group by machine_id"  
                                createFolder("Log/","query for production sel query "+str(cur_query))
                                current_data = cursor.execute(cur_query).fetchall()   
                                c_end = current_time
                                c_start = datetime.strptime(act_c_shift_start_time2, '%Y-%m-%d %H:%M:%S')
                                actual_duration = (c_end - c_start).total_seconds()
                                actual_duration = int(actual_duration)
                                p_table_name = F"messer_dt.dbo.current_production"
                                if len(current_data)>0:
                                    for data in current_data:
                                        current_stop_duration = data.current_stop_duration
                            else:
                                cur_query = f'''select 
                                                    sum(cp.current_stop_duration) as current_stop_duration
                                                from 
                                                    (select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt.dbo.current_loss 
                                                    union all select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt_completed.dbo.loss_{month_year}) cp  
                                                where 
                                                    cp.mill_date = '{act_date2}' and cp.mill_shift = '{act_shift2}' and cp.machine_id = '{id}'  group by cp.machine_id'''  
                                createFolder("Log/","query for production sel query "+str(cur_query))
                                current_data = cursor.execute(cur_query).fetchall()   
                                if act_shift2 == 1 :
                                    actual_duration = s_time1*60
                                elif act_shift2 == 2:
                                    actual_duration = s_time2*60
                                elif act_shift2 == 3:
                                    actual_duration = s_time3*60
                                p_table_name = f"[messer_dt_completed].[dbo].production_{month_year}"
                                if len(current_data)>0:
                                    for data in current_data:
                                        current_stop_duration = data.current_stop_duration
                                        current_stop_duration = current_stop_duration 
                                
                            
                                
                            run_duration = actual_duration - current_stop_duration 
                            update_query = f" update {p_table_name} set run_time = '{run_duration}', loss_time_1 = '{current_stop_duration}' where mill_date = '{act_date2}' and mill_shift = '{act_shift2}' and machine_id = '{id}'"
                            createFolder("Log/","query for production update "+str(update_query))
                            cursor.execute(update_query)
                            conn.commit()
                        except Exception as e :
                            createFolder("Log/",f"error in updating production table-{e} ")

            if loss_duration3 != 0:  
                createFolder("Log/",f"Loss_duration3-{loss_duration3}")
                sql3 = f"select * from messer_dt.dbo.current_loss where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time3}' and current_stop_duration={loss_duration3} "
                data3 = cursor.execute(sql3).fetchall()
               
                if len(data3)> 0:
                    pass
                    
                else:
                    month_year = act_date3[5:7]+act_date3[:4]
                    table_name=f"[messer_dt_completed].[dbo].[loss_{month_year}]"
                    query = f"""SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_CATALOG = 'messer_dt_completed' AND TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'loss_{month_year}' """
                    
                    result_query = cursor.execute(query).fetchall()
                    if len(result_query)>0:
                        sql33 = f'''select * from {table_name} where machine_id={id} and current_stop_code='{loss_code}' and current_stop_begin_time='{start_time3}' and current_stop_duration={loss_duration3} '''
                        data33=cursor.execute(sql33).fetchall()
                    if len(data33)!=0:
                        pass
                    else: 
                        
                        query = f"insert into messer_dt.dbo.current_loss(company_id,branch_id,department_id,machine_id,current_stop_code,current_stop_begin_time,run_begin_time, current_stop_duration,mill_date,mill_shift,product_id,operator_id) values({company_id},{branch_id},{department_id},{id},'{loss_code}','{start_time3}','{end_time3}',{loss_duration3},'{act_date3}','{act_shift3}',7,2)"
                        createFolder("Log/","query for loss_duration3 "+str(query))
                        cursor.execute(query)
                        conn.commit()
                        try:
                            act_date3 = datetime.strptime(act_date3, '%Y-%m-%d')
                            if current_mill_date == act_date3 and int(current_mill_shift) == int(act_shift3):
                            
                                cur_query = f"select sum(current_stop_duration) as current_stop_duration from messer_dt.dbo.current_loss where mill_date = '{act_date3}' and mill_shift = '{act_shift3}' and machine_id = '{id}'  group by machine_id"  
                                createFolder("Log/","query for production sel query "+str(cur_query))
                                current_data = cursor.execute(cur_query).fetchall()   
                                c_end = current_time
                                c_start = datetime.strptime(act_c_shift_start_time3, '%Y-%m-%d %H:%M:%S')
                                actual_duration = (c_end - c_start).total_seconds()
                                actual_duration = int(actual_duration)
                                p_table_name = F"messer_dt.dbo.current_production"
                                if len(current_data)>0:
                                    for data in current_data:
                                        current_stop_duration = data.current_stop_duration
                            else:
                                cur_query = f'''select 
                                                    sum(cp.current_stop_duration) as current_stop_duration
                                                from 
                                                    (select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt.dbo.current_loss 
                                                    union all select current_stop_duration,machine_id,mill_date,mill_shift from messer_dt_completed.dbo.loss_{month_year}) cp  
                                                where 
                                                    cp.mill_date = '{act_date3}' and cp.mill_shift = '{act_shift3}' and cp.machine_id = '{id}'  group by cp.machine_id'''  
                                createFolder("Log/","query for production sel query "+str(cur_query))
                                current_data = cursor.execute(cur_query).fetchall()   
                                if act_shift3 == 1 :
                                    actual_duration = s_time1*60
                                elif act_shift3 == 2:
                                    actual_duration = s_time2*60
                                elif act_shift3 == 3:
                                    actual_duration = s_time3*60
                                p_table_name = f"[messer_dt_completed].[dbo].production_{month_year}"
                                if len(current_data)>0:
                                    for data in current_data:
                                        current_stop_duration = data.current_stop_duration
                                        current_stop_duration = current_stop_duration
                           
                                
                            run_duration = actual_duration - current_stop_duration 
                            update_query = f" update {p_table_name} set run_time = '{run_duration}', loss_time_1 = '{current_stop_duration}' where mill_date = '{act_date3}' and mill_shift = '{act_shift3}' and machine_id = '{id}'"
                            createFolder("Log/","query for production update "+str(update_query))
                            cursor.execute(update_query)
                            conn.commit()
                        except Exception as e :
                            createFolder("Log/",f"error in updating production table-{e} ")

    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in returning data "+error_message)
   
while True:
    try:
        # conn = pyodbc.connect(DRIVER='{SQL Server}',host = 'DESKTOP-6034N39', PORT = 1434,DATABASE='ems_v1',UID='sa',PWD='admin@2024')
        conn = pyodbc.connect(DRIVER='{SQL Server}',SERVER='P-MUM-DFS-SQL-1\SQLEXPRESS',PORT='1433',DATABASE='messer_dt_completed',UID='sa',PWD='Me$$er@DF')
        cursor = conn.cursor()
        current_production()
        current_loss()
        manual_loss_entry()
        conn.close()
        time.sleep(60)  # Sleep for 60 seconds (1 minute)
    
    except Exception as e :
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in returning data "+error_message)


