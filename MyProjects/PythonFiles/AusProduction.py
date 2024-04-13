import pymysql
from datetime import date
import datetime
import os
import shutil
import time
import sys
import psutil
import requests

errorlog = 'ErrorLog'
live = 'Live'
process = 'Process'

def createFolder(directory,file_name,data):
    
    date_time=datetime.datetime.now()
    curtime1=date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2=date_time.strftime("%d-%m-%Y")
    directory = directory + str(curtime2) + '/'
    
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        f= open(directory+str(file_name)+".txt","a+")      
        f.write(curtime1 +" "+ str(data) +"\r\n")
        f.close()

        # deleting old log files
        old_date = (datetime.datetime.now() + datetime.timedelta(days=-5)).date()
        file_list = os.listdir('Log')

        for file in file_list:
            try:
                file_date = datetime.datetime.strptime(file, '%d-%m-%Y').date()
            except:
                shutil.rmtree('Log/'+file)
            if file_date <= old_date:
                shutil.rmtree('Log/'+file)

    except OSError:
        print ('Error: Creating directory. ' +  directory)

def Production(db,cursor):
    try:
        createFolder('Log/',live," Production function started ...!!!")

        query = f'''SELECT ed.*,mm.plant_id,mp.host_ip,mp.fapi_port , mp.is_fapi_call FROM equipment_wise_production_data ed 
                    INNER JOIN master_meter mm on mm.meter_id = ed.meter_id
                    INNER JOIN master_plant mp on mp.plant_id = mm.plant_id
                    INNER JOIN master_equipment me ON me.equipment_id = mm.equipment_id and me.equipment_id = ed.equipment_id                
                    '''
        cursor.execute(query)
        result = cursor.fetchall()
        # createFolder('Log/',live,f" Shift Data - {result}")
        mill_date = date.today()
        mill_shift = 0
        
        if len(result)>0:
            for row in result:
                
                id = row['id']
                mill_date = row['mill_date']
                mill_shift = row['mill_shift']
                meter_id = row['meter_id']
                equipment_id = row['equipment_id']
                host_ip = row["host_ip"]
                fapi_port = row["fapi_port"]
                is_fapi_call = row["is_fapi_call"]
                created_on = row["created_on"]

                start_time = created_on + datetime.timedelta(minutes=30)
                current_time = datetime.datetime.now()
                if current_time > start_time:

                    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
                    table_name = ""

                    month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
                    table_name=f"  ems_v1_completed.power_{month_year}"
                    createFolder('Log/',live,f" Table Name - {table_name} ")

                    api_params = {
                        "period_id":"sel_shift",
                        "group_by":"machine",
                        "report_type":"detail",
                        "equipment_id": equipment_id,
                        "from_date": str(mill_date),
                        "shift_id": str(mill_shift)
                    }

                    createFolder('Log/',live,f" API inputs - {api_params} ")

                    api_url = f"http://{host_ip}:{fapi_port}/production_dashboard_report_detail_API/"
                    
                    api_url = f"{api_url}"
                    createFolder('Log/',live,f" API url - {api_url} ")
                    
                    response = requests.post(api_url, data=api_params)
                    # createFolder('Log/',live,f" API raw data - {response.text} ")
                    api_response = response.json()

                    if api_response is not None:

                        if 'data' in api_response:
                            data = api_response['data']

                            if not data:  
                                createFolder('Log/', live, "No data entries found in the data list")
                            else:
                                for row in data:
                                    actual_ton = row['actual_ton'] 
                                    createFolder('Log/',live,f" actual_ton - {actual_ton} ")
        
                                query = f"update {table_name} set actual_ton = '{actual_ton}' where meter_id = '{meter_id}' and mill_date='{mill_date}' and mill_shift = '{mill_shift}' "
                                createFolder('Log/',live,f" update actual ton completed  query - {query} ")
                                cursor.execute(query)
                                db.commit()
                                
                                
                        else:
                            createFolder('Log/', errorlog, "'data' key not found in the API response")

                    else:
                        createFolder('Log/', errorlog, "API response is None")

                    query = f"delete from equipment_wise_production_data where id = '{id}' "
                    cursor.execute(query)
                    db.commit()
                    createFolder('Log/',live,f" delete equipment_wise_production_data id - {id} ")
                else:
                    createFolder('Log/',live,f" Wait for run .. ") 
        else:
           createFolder('Log/',live,f" No Data .. ") 

    except Exception as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder('Log/',errorlog,f" Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")

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
                
            createFolder('Logs/',process, f"Calling Power Data  !! ")
            db = pymysql.connect(host="localhost", user="AIC_PY_PD",passwd="32f66dcebebc97d74410a7c3e94233f6", db="ems_v1" , port= 3308)
            cursor = db.cursor(pymysql.cursors.DictCursor)
            Production(db,cursor)
            if db:
                db.close()
            createFolder('Logs/',process, f"Power Data execution completed !! ")
            time.sleep(30)

        except Exception as e:

            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder('Log/',errorlog,f" Error!: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno}.")