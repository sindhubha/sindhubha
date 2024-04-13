import sys
import os
import datetime
from  datetime import date,timedelta
import shutil
import time
from dateutil import parser
import pymysql
import pytz
import json
import shutil
import psutil



shift_end_flag=0
live = 'LogS/'
errorlog = 'ErrorLogS/'
date_time=datetime.datetime.now()

def parse_date(from_date):
    from_date = str(from_date)
    date_from = from_date.split("-")
    if len(date_from[0]) <= 2:
        if int(date_from[0]) > 12:
            from_date = parser.parse(from_date).strftime("%Y-%m-%d %H:%M:%S")
        else:
            from_date = parser.parse(from_date).strftime("%Y-%d-%m %H:%M:%S")
        from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
    else:
        from_date = parser.parse(from_date).strftime("%Y-%m-%d %H:%M:%S")
        from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d %H:%M:%S")
    return from_date 

def createFolder(directory,data): 
    date_time=datetime.datetime.now()
    curtime1=date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2=date_time.strftime("%d-%m-%Y")
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        with open(directory + curtime2 + ".txt", "a+") as f:
            f.write(curtime1 +" "+ str(data) +"\r\n")
        old_date = (datetime.datetime.now() + datetime.timedelta(days=-5)).date()
        file_list = os.listdir(directory[:-1])
        for file in file_list:
            try:
                file_date = datetime.datetime.strptime(file[:-4], '%d-%m-%Y').date()
            except:
                if os.path.isdir(directory+file):
                    shutil.rmtree(directory+file)
                else:
                    os.remove(directory+file)
            if file_date <= old_date:
                if os.path.isdir(directory+file):
                    shutil.rmtree(directory+file)
                else:
                    os.remove(directory+file)

    except OSError:
        print ('Error: Creating directory. ' +  directory)

mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}

def get_amps_dtls(db,cursor):
    try:
        start_time = date.today()
        poll_duration = 0
        meter_id = 0
        
        createFolder('LogS/'," function started ...!!!")
        query = f"select * from current_polling_data where is_amps = 'no'"
        cursor.execute(query)
        result = cursor.fetchall()

        if len(result)>0:
            for row in result:
                meter_id = row["meter_id"]
                id = row["id"]
                start_time = row["mc_state_changed_time"]
                poll_duration = row["poll_duration"]
                end_time = start_time + timedelta(seconds=poll_duration)
                

                sql = f'''
                    select 
                        cp.meter_id,
                        ROUND(AVG(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS avg_t_current,
                        ROUND(MIN(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS min_t_current,
                        ROUND(MAX(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS max_t_current
                                
                    from 
                        current_power_analysis cp 
                        inner join ems_v1.master_meter mm on mm.meter_id=cp.meter_id
                        inner join ems_v1.master_meter_factor mmf on  mmf.plant_id = mm.plant_id AND mmf.meter_id = mm.meter_id
                        inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mm.plant_id  
                        where cp.created_on >='{start_time}' and cp.created_on <= '{end_time}' and cp.meter_id = '{meter_id}'
                    group by cp.meter_id
                    '''  
                createFolder('LogS/',f" query - {sql} ...!!!")
                cursor.execute(sql)
                data = cursor.fetchall()
                
                avg_amps = 0
                min_amps = 0
                max_amps = 0
                
                if len(data)>0:
                    for i in data:
                        avg_amps = i["avg_t_current"]
                        min_amps = i["min_t_current"]
                        max_amps = i["max_t_current"]
                    
                update_query = f''' update ems_v1.current_polling_data
                                        set avg_amps = '{avg_amps}', min_amps = '{min_amps}', max_amps='{max_amps}', is_amps = 'yes' 
                                        where id = '{id}' '''
                createFolder(f"LogS/", f"Updated Amps {update_query}...!!!")   
                cursor.execute(update_query)
                db.commit()
        
    except Exception as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder('Log/',f" Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")

def manual_amps(db,cursor):
    try:
        query = f"select * from manual_update"
        cursor.execute(query)
        data = cursor.fetchall()
        plant_id = 0
        is_manual_call = 'no'
        if len(data)>0:
            for row in data:
                record_id = row["id"]
                plant_id = row["plant_id"]
                mill_date = row["mill_date"]
                mill_shift = row["mill_shift"]
                date_time = row["created_on"]
                is_manual_call = row["is_manual_call"]
                mill_date = parse_date(mill_date)
                month_year = f"{mill_month[mill_date.month]}{mill_date.year}"

                query = f"select * from ems_v1_completed.polling_data_{month_year} where mill_date = '{mill_date}'  and mill_shift = '{mill_shift}'  and is_amps = 'no'"
                cursor.execute(query)
                result = cursor.fetchall()
                poll_duration = 0
                start_time = date.today()
                meter_id = 0

                if len(result)>0:
                    for row in result:
                        meter_id = row["meter_id"]
                        id = row["id"]
                        start_time = row["mc_state_changed_time"]
                        poll_duration = row["poll_duration"]
                        end_time = start_time + timedelta(seconds=poll_duration)

                        sql = f'''
                            select 
                                cp.meter_id,
                                ROUND(AVG(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS avg_t_current,
                                ROUND(MIN(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS min_t_current,
                                ROUND(MAX(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS max_t_current
                                
                            from 
                                ems_v1_completed.power_analysis_{month_year} cp
                                inner join ems_v1.master_meter mm on mm.meter_id=cp.meter_id
                                inner join ems_v1.master_meter_factor mmf on  mmf.plant_id = mm.plant_id AND mmf.meter_id = mm.meter_id
                                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mm.plant_id  
                                where  cp.meter_id = '{meter_id}'  and  cp.created_on >='{start_time}' and cp.created_on <= '{end_time}'  
                            group by cp.meter_id
                            '''  
                        cursor.execute(sql)
                        data = cursor.fetchall()

                        avg_amps = 0
                        min_amps = 0
                        max_amps = 0
                        
                        if len(data)>0:
                            for i in data:
                                avg_amps = i["avg_t_current"]
                                min_amps = i["min_t_current"]
                                max_amps = i["max_t_current"]
                            
                        update_query = f''' update ems_v1_completed.polling_data_{month_year}
                                        set avg_amps = '{avg_amps}', min_amps = '{min_amps}', max_amps='{max_amps}', is_amps = 'yes' 
                                        where id = '{id}' '''
                        createFolder(f"LogM/", f"Updated Amps {update_query}...!!!")   
                        cursor.execute(update_query)
                        db.commit()
                        
                else:
                    createFolder(f"LogM/", f"no data available...!!!")
                if is_manual_call != 'yes':
                    try:
                        # Creating database if not exists
                        cursor.execute("CREATE DATABASE IF NOT EXISTS gateway_completed")
                        db.commit()
                        createFolder(f"LogM/", "Creating database if not exists gateway_completed")
                        
                        # Creating table if not exists
                        sql = f'''
                                CREATE TABLE IF NOT EXISTS gateway_completed.log_{month_year} (
                                `id` BIGINT(11) NOT NULL AUTO_INCREMENT,
                                `imei_no` VARCHAR(50) NOT NULL DEFAULT '',
                                `mac` VARCHAR(50) NOT NULL DEFAULT '',
                                `signal_strength` INT(5) NOT NULL DEFAULT 0,
                                `received_packet` TEXT NOT NULL,
                                `date_time` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',
                                `msg_type` VARCHAR(20) NOT NULL DEFAULT '',
                                `production_quantity` INT(11) NOT NULL,
                                `created_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP(),
                                `status` VARCHAR(11) NOT NULL DEFAULT 'no',
                                PRIMARY KEY (`id`),
                                KEY `imei_omly` (`imei_no`),
                                KEY `date_time_only` (`date_time`),
                                KEY `imei_no_msg_type` (`imei_no`,`msg_type`),
                                KEY `imei_no_date_time` (`imei_no`,`date_time`),
                                KEY `signal_strength_only` (`signal_strength`),
                                KEY `mac` (`mac`),
                                KEY `status` (`status`)
                                ) ENGINE=INNODB DEFAULT CHARSET=latin1
                                '''
                        createFolder(f"LogM/", f"Creating table if not exists : {sql}")
                        cursor.execute(sql)
                        db.commit()
                        
                        # Inserting data from current table to completed
                        sql = f'''
                            INSERT INTO gateway_completed.log_{month_year}(imei_no,mac,signal_strength,received_packet,date_time,msg_type,production_quantity,created_on,STATUS)
                            SELECT imei_no,mac,signal_strength,received_packet,date_time,msg_type,production_quantity,created_on,STATUS
                            FROM gateway.log WHERE mac IN (SELECT mac FROM ems_v1.master_meter WHERE  plant_id = '{plant_id}') and created_on <= '{date_time}'  AND STATUS = 'yes' 
                            '''
                        createFolder(f"LogM/", f"Inserting data into completed gateway : {sql}")
                        cursor.execute(sql)
                        db.commit()

                        query = f'''
                            delete from gateway.log WHERE mac IN (SELECT mac FROM ems_v1.master_meter WHERE  plant_id = '{plant_id}') and created_on <= '{date_time}' AND STATUS = 'yes' 
                            '''
                        createFolder(f"LogM/", f"deleted gateway data: {query}")
                        cursor.execute(query)
                        db.commit()
                        
                        createFolder(f"LogM/", f"Completed Moving all data from current data !")

                    except Exception as e:
                        exc_type, exc_obj, exc_tb = sys.exc_info()
                        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                        createFolder(f"Log/", f"Error In Moving Gateway data -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")
            

                query = f" delete from ems_v1.manual_update where id = {record_id}"
                cursor.execute(query)
                db.commit()
                createFolder(f"LogM/", f"closing manual_update record - {record_id}")      
        else:
            createFolder(f"LogM/", f"no data...!!!")   


    except Exception as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder('Log/',f" Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")

def checkIfProcessRunning(processName):
    process_count = 0
    for proc in psutil.process_iter():
        try:
            if processName.lower() in proc.name().lower():
                process_count = process_count + 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return process_count

if getattr(sys, 'frozen', False):
    File_Name = os.path.basename(sys.executable)
elif __file__:
    File_Name = os.path.basename(__file__)

process_count = checkIfProcessRunning(File_Name)


if process_count > 2:
    createFolder('Log/',live,f"Process {File_Name} Already Running !! -- Exiting - .")
    sys.exit()
else:

    while True:

        try:
                
            createFolder('LogS/', f"Function Called !! ")
            db = pymysql.connect(host="localhost", user="AIC_PY_AMPS",passwd="352d08195eea2bfb88e8b603e11c27c7", db="ems_v1" , port= 3308)
            cursor = db.cursor(pymysql.cursors.DictCursor)
            get_amps_dtls(db,cursor)
            manual_amps(db,cursor)
            if db:
                db.close()
            createFolder('LogS/', f"Execution Completed !! ")
            time.sleep(30)

        except Exception as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder('Log/',f" Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")