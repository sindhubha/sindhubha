import pymysql
from datetime import datetime
import datetime
import os
import time
import sys
import psutil
import shutil

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
# isFile = os.path.abspath(os.path.join(os.path.dirname(__file__),"port.txt"))

# print(isFile)

# if isFile:
#     f = open(isFile,"r")
#     port = f.read()
# if port:
#     port_num = int(port)

# else :
#     print(f"The given port is : {port} " )

def function_call():

    try:
        db = pymysql.connect(host="localhost", user="AIC_PY_LA",passwd="1575e2339b8e4d4c3f4c05560f3dfb30", db="ems_v1" , port= 3308)
        cursor = db.cursor(pymysql.cursors.DictCursor)

        createFolder('Load_Analysis/',"Database Connected Successfully !")
        
        # sql ='''insert into current_power_analysis(power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,date_time,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kwh,kvah,kw,kvar,power_factor,kva,actual_demand,demand_dtm,frequency,machine_kwh,master_kwh,created_on,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,off_time,idle_time,on_load_time,kwh_msb,kwh_lsb,mc_state_changed_time,current_poll_duration,shift_off_time,shift_idle_time,shift_on_load_time,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh)
        #         select power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,date_time,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kwh,kvah,kw,kvar,power_factor,kva,actual_demand,demand_dtm,frequency,machine_kwh,master_kwh,now(),reverse_machine_kwh,reverse_master_kwh,reverse_kwh,off_time,idle_time,on_load_time,kwh_msb,kwh_lsb,mc_state_changed_time,current_poll_duration,shift_off_time,shift_idle_time,shift_on_load_time,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh 
        #         from current_power where date_time >= DATE_ADD(NOW(),INTERVAL -5 MINUTE) '''
        sql ='''insert into current_power_analysis(power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,date_time,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kwh,kvah,kw,kvar,power_factor,kva,actual_demand,demand_dtm,frequency,machine_kwh,master_kwh,created_on,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,off_time,idle_time,on_load_time,kwh_msb,kwh_lsb,mc_state_changed_time,current_poll_duration,shift_off_time,shift_idle_time,shift_on_load_time,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh,diff_equipment_off_kwh,diff_equipment_idle_kwh,diff_equipment_on_load_kwh,r_volt_thd,y_volt_thd,b_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,meter_status,runhour_msb,runhour_lsb,runhour,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd)
                select cp.power_id,cp.company_id,cp.bu_id,cp.plant_id,cp.plant_department_id,cp.equipment_group_id,cp.meter_id,cp.date_time,cp.mill_date,cp.mill_shift,cp.vln_avg,cp.r_volt,cp.y_volt,cp.b_volt,cp.vll_avg,cp.ry_volt,cp.yb_volt,cp.br_volt,cp.t_current,cp.r_current,cp.y_current,cp.b_current,cp.t_watts,cp.r_watts,cp.y_watts,cp.b_watts,cp.t_var,cp.r_var,cp.y_var,cp.b_var,cp.t_voltampere,cp.r_voltampere,cp.y_voltampere,cp.b_voltampere,cp.avg_powerfactor,cp.r_powerfactor,cp.y_powerfactor,cp.b_powerfactor,cp.powerfactor,cp.kwh,cp.kvah,cp.kw,cp.kvar,cp.power_factor,cp.kva,cp.actual_demand,cp.demand_dtm,cp.frequency,cp.machine_kwh,cp.master_kwh,now(),cp.reverse_machine_kwh,cp.reverse_master_kwh,cp.reverse_kwh,cp.off_time,cp.idle_time,cp.on_load_time,cp.kwh_msb,cp.kwh_lsb,cp.mc_state_changed_time,cp.current_poll_duration,cp.shift_off_time,cp.shift_idle_time,cp.shift_on_load_time,cp.current_poll_consumption,cp.last_poll_consumption,cp.off_kwh,cp.idle_kwh,cp.on_load_kwh,cp.shift_off_kwh,cp.shift_idle_kwh,cp.shift_on_load_kwh,cp.equipment_kwh,cp.total_kwh,cp.meter_status_code,cp.current_equipment_poll_consumption,cp.last_poll_equipment_kwh,
                cp.equipment_off_kwh,cp.equipment_idle_kwh,cp.equipment_on_load_kwh,
                cp.equipment_shift_off_kwh,cp.equipment_shift_idle_kwh,cp.equipment_shift_on_load_kwh ,
                COALESCE(cp.equipment_off_kwh - prev.prev_equipment_off_kwh, cp.equipment_off_kwh) AS diff_equipment_off_kwh,
                COALESCE(cp.equipment_idle_kwh - prev.prev_equipment_idle_kwh, cp.equipment_idle_kwh) AS diff_equipment_idle_kwh,
                COALESCE(cp.equipment_on_load_kwh - prev.prev_equipment_on_load_kwh, cp.equipment_on_load_kwh) AS diff_equipment_on_load_kwh,
                cp.r_volt_thd,cp.y_volt_thd,cp.b_volt_thd,cp.avg_volt_thd,cp.r_current_thd,cp.y_current_thd,cp.b_current_thd,cp.avg_current_thd,cp.meter_status,
                cp.runhour_msb,cp.runhour_lsb,cp.runhour,cp.ry_volt_thd,cp.yb_volt_thd,cp.br_volt_thd,cp.vll_avg_thd,cp.vln_avg_thd
                from current_power cp LEFT JOIN (
                        SELECT
                            ROW_NUMBER() OVER (PARTITION BY meter_id ORDER BY created_on DESC) AS row_num,
                            equipment_off_kwh AS prev_equipment_off_kwh,
                            equipment_idle_kwh AS prev_equipment_idle_kwh,
                            equipment_on_load_kwh AS prev_equipment_on_load_kwh,
                            meter_id
                        FROM
                            current_power_analysis
                    
                    ) AS PREV
                    ON
                        cp.meter_id = prev.meter_id AND prev.row_num = 1
                where cp.date_time >= DATE_ADD(NOW(),INTERVAL -5 MINUTE) '''
        cursor.execute(sql)
        db.commit()
        createFolder('Load_Analysis/',"Data Insertion success '-' .")

    except Exception as e:
        createFolder('Load_Analysis/',"Error Connecting Database : "+str(e))

# program begins here
createFolder('Load_Analysis/', "Application Startted ")

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
    createFolder('Process_log/', f"Process {File_Name} is Already Running  !! ")
    sys.exit()

else:
    while True:
        function_call()
        time.sleep(30)