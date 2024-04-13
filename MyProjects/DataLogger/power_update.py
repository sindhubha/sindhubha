import sys
import os
import datetime
from  datetime import date,timedelta
import shutil
import time
import pymysql
import pytz
import json
import psutil



shift_end_flag=0
Logfile_name = 'LogS/'

def createFolder(directory, data):
    date_time = datetime.datetime.now()
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
        five_days_ago = date_time - timedelta(days=3)
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                file_date_str = filename.split('.')[0]
                file_date = datetime.datetime.strptime(file_date_str, "%d-%m-%Y")
                if file_date < five_days_ago:
                    os.remove(file_path)

        # Create the log file inside the directory
        file_path = os.path.join(directory, f"{curtime2}.txt")
        with open(file_path, "a+") as f:
            f.write(f"{curtime1} {data}\r\n")
    except OSError as e:
        print(f"Error: Creating directory. {directory} - {e}")


def read_configuration_file(file_path):
    config = {}
    with open(file_path, 'r') as file:
        for line in file:
            key, value = line.strip().split(' : ')
            config[key] = int(value)  # Assuming all values in the file are integers
    return config

file_path = 'exe-config.ini'
isFile = os.path.isfile(file_path)
if isFile:
    configuration = read_configuration_file(file_path)
    parameter_value = configuration['parameter']
    createFolder(f"Logs/Parameter/",f"The given parameter is : {parameter_value} !")
else :
    print(f"The given parameter is : {isFile} " )
    createFolder(f"Logs/Parameter/",f"Error in parameter  : {isFile} " )
    sys.exit()
        
# Function to fetch configuration from .ini(configuration settings) and make it a dictionary .

time_zone = pytz.timezone("Asia/Kolkata")

def update_polling_data(t_current,machine_state_condition2,machine_state_condition4,power_table_mc_status,meter_id,mac,cursor,db):

        createFolder(f"gateway_log/{mac}-Log/",f"In Polling Data Updation Function {power_table_mc_status}!")

        # PreProcessing Machine Status by using Total Current
        meter_status = 0

        if t_current <= machine_state_condition2:
            str = "off"
            meter_status = 0
        elif t_current > machine_state_condition2 and t_current <= machine_state_condition4:
            str = "idle"
            meter_status = 1
        elif t_current > machine_state_condition4:
            str = "on load"
            meter_status = 2

        # off_time , shift_off_time , idle_time , shift_idle_time , on_load_time , shift_on_load_time

        query_str = ''
        if power_table_mc_status != meter_status :

            createFolder(f"gateway_log/{mac}-Log/","Machine state changed ! .")

            if power_table_mc_status == 0:
                query_str += ",shift_off_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_off_time , off_time = shift_off_time"
                query_str += ",shift_off_kwh = (machine_kwh - last_poll_consumption) + shift_off_kwh , off_kwh = shift_off_kwh"
                query_str += ",equipment_shift_off_kwh = (equipment_kwh - last_poll_equipment_kwh) + equipment_shift_off_kwh , equipment_off_kwh = equipment_shift_off_kwh"
            elif power_table_mc_status == 1:
                query_str += ",shift_idle_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_idle_time , idle_time = shift_idle_time"
                query_str += ",shift_idle_kwh = (machine_kwh - last_poll_consumption) + shift_idle_kwh , idle_kwh = shift_idle_kwh"
                query_str += ",equipment_shift_idle_kwh = (equipment_kwh - last_poll_equipment_kwh) + equipment_shift_idle_kwh , equipment_idle_kwh = equipment_shift_idle_kwh"
            elif power_table_mc_status == 2:
                query_str += ",shift_on_load_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_on_load_time , on_load_time = shift_on_load_time"
                query_str += ",shift_on_load_kwh = (machine_kwh - last_poll_consumption) + shift_on_load_kwh , on_load_kwh = shift_on_load_kwh"
                query_str += ",equipment_shift_on_load_kwh = (equipment_kwh - last_poll_equipment_kwh) + equipment_shift_on_load_kwh , equipment_on_load_kwh = equipment_shift_on_load_kwh"

            # TO take difference of the current polling data .
            sql = f'''UPDATE
                        current_power
                    SET
                        current_poll_consumption = machine_kwh - last_poll_consumption ,
                        current_equipment_poll_consumption = equipment_kwh - last_poll_equipment_kwh ,
                        current_poll_duration = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0)
                        {query_str}
                    WHERE
                        meter_id = '{meter_id}' '''
            createFolder(f"gateway_log/{mac}-Log/",f"TO take difference of the current polling data -->> {sql}")
            cursor.execute(sql)
            db.commit()
            
            # Insert query Previous Machines status with start datetime and duration
            sql = f'''INSERT INTO current_polling_data(meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , poll_duration , poll_consumption , equipment_consumption)
                        SELECT meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , current_poll_duration , current_poll_consumption , current_equipment_poll_consumption FROM current_power 
                        WHERE meter_id = '{meter_id}' '''
            createFolder(f"gateway_log/{mac}-Log/",f"Inserting last polling data -->> {sql}")
            cursor.execute(sql)
            db.commit()

            # Update query Current machine status and now time as start time and reset the duration
            sql = f'''UPDATE
                        current_power
                    SET
                        meter_status = '{meter_status}' ,
                        mc_state_changed_time = NOW() ,
                        current_poll_duration = 0 ,
                        current_poll_consumption = 0 ,
                        current_equipment_poll_consumption = 0 ,
                        last_poll_consumption = machine_kwh ,
                        last_poll_equipment_kwh = equipment_kwh
                    WHERE
                        meter_id = '{meter_id}' '''
            createFolder(f"gateway_log/{mac}-Log/",f"Updating machine state changed time and resetting poll duration -->> {sql}")
            cursor.execute(sql)
            db.commit()

        else:
            createFolder(f"gateway_log/{mac}-Log/","Machine status not changed . Duration updated for current Machine state !")

            if power_table_mc_status == 0:
                query_str += ",off_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_off_time"
                query_str += ",off_kwh = machine_kwh - last_poll_consumption + shift_off_kwh"
                query_str += ",equipment_off_kwh = equipment_kwh - last_poll_equipment_kwh + equipment_shift_off_kwh"
            elif power_table_mc_status == 1:
                query_str += ",idle_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_idle_time"
                query_str += ",idle_kwh = machine_kwh - last_poll_consumption + shift_idle_kwh"
                query_str += ",equipment_idle_kwh = equipment_kwh - last_poll_equipment_kwh + equipment_shift_idle_kwh"
            elif power_table_mc_status == 2:
                query_str += ",on_load_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_on_load_time"
                query_str += ",on_load_kwh = machine_kwh - last_poll_consumption + shift_on_load_kwh"
                query_str += ",equipment_on_load_kwh = equipment_kwh - last_poll_equipment_kwh + equipment_shift_on_load_kwh"

            # Update query Duration by current machine state 
            sql = f'''UPDATE
                        current_power
                    SET 
                        current_poll_consumption = machine_kwh - last_poll_consumption ,
                        current_equipment_poll_consumption = equipment_kwh - last_poll_equipment_kwh ,
                        current_poll_duration = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0)
                        {query_str}
                    WHERE
                        meter_id = '{meter_id}' '''
            createFolder(f"gateway_log/{mac}-Log/",f"Updating Current Polling Duration -->> {sql} ")
            cursor.execute(sql)
            db.commit()

def power_data(db,cursor):
    try:
        
        query = f"select * from master_converter_detail where status = 'active' and parameter = '{parameter_value}'"
        cursor.execute(query)
        converter_datas = cursor.fetchall()

        sql = f'''SELECT m.meter_id,m.mac,m.address,c.master_kwh,m.is_poll_meter,m.equipment_id ,m.plant_id, m.max_demand , c.kva, c.actual_demand, c.machine_kwh,mdl.model_make_id FROM master_meter m INNER JOIN 
                                current_power c ON c.meter_id=m.meter_id 
                                INNER JOIN master_converter_detail mcd ON mcd.converter_id = m.converter_id AND mcd.parameter = '{parameter_value}'
                                INNER JOIN master_model mdl on mdl.model_id = m.model_name'''
        createFolder(f"main_process/",f"selquery:{sql}")
        cursor.execute(sql)
        machine_details = cursor.fetchall()
        
        for c_row in converter_datas:
            try:
                c_mac = c_row["mac"]
                createFolder('Logs/GatewayProcess/', f"Process Start ({c_mac}) !! ")
                query = f"select * from gateway.log where status = 'no'  and mac = '{c_mac}' order by created_on,mac"
                cursor.execute(query)
                datas = cursor.fetchall()

                for row in datas:
                    try:
                        id = row["id"]
                        mac = row["mac"]
                        received_packet = row['received_packet'].replace("'", "\"")
                        received_packet_data = json.loads(received_packet)
                        # print("received_packet_data",received_packet_data)
                        data = received_packet_data['data']
                        msg_type = data['msg']
                        
                        if msg_type == 'log':
                            modbus_data = data['modbus']

                            for data in modbus_data:
                                try:
                                    is_error = int(data['stat'])                
                                    slave_id = data['sid']
                                        
                                    meter_id = ''
                                    prev_machine_kwh = 0
                                    for machine in machine_details:
                                        if machine["mac"] == mac and int(machine["address"]) == int(slave_id):
                                            # print(meter_id)

                                            data.pop('sid')
                                            data.pop('stat')
                                            data.pop('rcnt')
                                            # data.pop('gid')
                                            # data.pop('indx')
                                            
                                            mac = machine["mac"]
                                            slave_id = machine["address"]
                                            meter_id = machine["meter_id"]
                                            plant_id = machine["plant_id"]
                                            master_kwh = machine["master_kwh"]
                                            is_poll_meter = machine['is_poll_meter']
                                            equipment_id = machine['equipment_id']
                                            max_demand = float(machine["max_demand"])
                                            last_demand = float(machine["kva"])
                                            last_actual_demand = float(machine["actual_demand"])
                                            prev_machine_kwh = float(machine["machine_kwh"])
                                            model_make_id = int(machine["model_make_id"])

                                            break

                                    if  meter_id != '':   
                                    
                                        query = (f" update ems_v1.current_power set meter_status_code = '{is_error}' where meter_id = {meter_id}")
                                        createFolder(f"gateway_log/{mac}-Log/",f"Updated Meter Status Code - query:{query}")
                                        cursor.execute(query)
                                        db.commit()
                                        t_current = 0
                                        kva = 0
                                        if is_error == 0 :
                                            try:
                                                sql = "UPDATE current_power SET date_time = now()"
                                                print(f"Type of 'data': {type(data)}")
                                                for key,value in data.items():

                                                    if key != 'none' and key != 'gid':
                                                        if key == "kva":
                                                            kva = float(value)
                                                            sql +=f", kva = {value}"
                                                            # if kva >last_demand and kva > last_actual_demand and max_demand != 0:
                                                            if kva >last_demand and kva > last_actual_demand and max_demand != 0:
                                                                sql +=f",actual_demand = {kva} , demand_dtm = now()"
                                                        
                                                        elif key == 'avg_pf':
                                                            if model_make_id == 1:
                                                                if value < 1000 and value != 0:
                                                                    sql += f",avg_powerfactor= {value}"
                                                                else:
                                                                    createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or above key:{key} Value:{value}")
                                                            elif value != 1 and value != 0:
                                                                sql += f",avg_powerfactor= {value}"
                                                            else:
                                                                createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 key:{key} Value:{value}")
                                                        
                                                        elif key == 'kwh':
                                                            meter_kwh = value
                                                            if master_kwh > meter_kwh:
                                                                update_query = f"update current_power set master_kwh = '{meter_kwh}' where meter_id = '{meter_id}'"
                                                                cursor.execute(update_query)
                                                                db.commit()
                                                                sql += f",machine_kwh= {meter_kwh}"
                                                                sql += f",kwh= 0"
                                                                createFolder(f"gateway_log/{mac}-Log/",f"- Master_kwh reset done  for this  meter_id:{meter_id}")

                                                            else:
                                                                actual_kwh = int(meter_kwh) - int(master_kwh)
                                                                
                                                                if meter_kwh != 0 and meter_kwh > 0:
                                                                    sql += f",machine_kwh= {meter_kwh}"
                                                                    sql += f",kwh= {actual_kwh}"
                                                                else:
                                                                    createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 0 or lessthen 0 key:{key} Value:{meter_kwh}")
                                                        
                                                        elif key == 'kwh_msb':
                                                            kwh_msb = value
                                                            sql += f",kwh_msb= {value}"

                                                        elif key == 'kwh_lsb':
                                                            meter_kwh = (kwh_msb * 65536) + value
                                                            if master_kwh > meter_kwh:
                                                                update_query = f"update current_power set master_kwh = '{meter_kwh}' where meter_id = '{meter_id}'"
                                                                cursor.execute(update_query)
                                                                db.commit()
                                                                sql += f",machine_kwh= {meter_kwh}"
                                                                sql += f",kwh= 0"
                                                                createFolder(f"gateway_log/{mac}-Log/",f"- Master_kwh reset done  for this  meter_id:{meter_id}")

                                                            else:
                                                                actual_kwh = int(meter_kwh) - int(master_kwh)
                                                                if meter_kwh != 0 and meter_kwh > 0:                                                     
                                                                    sql += f",machine_kwh= {meter_kwh}"
                                                                    sql += f",kwh= {actual_kwh}"
                                                                else:
                                                                    createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 0 or lessthen 0 key:{key} Value:{meter_kwh}")
                                                                sql += f",kwh_lsb= {value}"

                                                        elif key == 'runhour_msb':
                                                            runhour_msb = value
                                                            sql += f",runhour_msb= {value}"
                                
                                                        elif key == 'runhour_lsb':
                                                            runhour = (runhour_msb * 65536) + value     
                                                            sql += f",runhour= {runhour}"
                                                            sql += f",runhour_lsb= {value}"

                                                        elif key == 'r_powerfactor':
                                                            if model_make_id == 1:
                                                                if value < 1000 and value != 0:
                                                                    sql += f",r_powerfactor= {value}"
                                                                else:
                                                                    createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or above key:{key} Value:{value}")
                                                            elif value != 1 and value != 0:
                                                                sql += f",r_powerfactor= {value}"
                                                            else:
                                                                createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or 0 key:{key} Value:{value}")

                                                        elif key == 'y_powerfactor':
                                                            if model_make_id == 1:
                                                                if value < 1000 and value != 0:
                                                                    sql += f",y_powerfactor= {value}"
                                                                else:
                                                                    createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or above key:{key} Value:{value}")
                                                            elif value != 1 and value != 0:
                                                                sql += f",y_powerfactor= {value}"
                                                            else:
                                                                createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or 0 key:{key} Value:{value}")

                                                        elif key == 'b_powerfactor':
                                                            if model_make_id == 1:
                                                                if value < 1000 and value != 0:
                                                                    sql += f",b_powerfactor= {value}"
                                                                else:
                                                                    createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or above key:{key} Value:{value}")
                                                            elif value != 1 and value != 0:
                                                                sql += f",b_powerfactor= {value}"
                                                            else:
                                                                createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 1 or 0 key:{key} Value:{value}")
                                                        
                                                        elif value > 0:
                                                            sql += f",{key}= {value}"
                                                            
                                                        else:
                                                            createFolder(f"gateway_log/{mac}-Log/",f"- Getting Value 0 or lessthen 0 key:{key} Value:{value}")
                                                        
                                                        if data.get('t_current') == None:
                                                    
                                                            value = int(data.get('r_current', 0) + data.get('y_current', 0) + data.get('b_current', 0)) / 3
                                                            t_current = value
                                                
                                                            sql += f",t_current= {value}"
                                                        else:
                                                            t_current = data.get('t_current',0)

                                                        if data.get('r_volt_thd') != None and data.get('y_volt_thd') != None and data.get('b_volt_thd') != None:
                                                            value = int(data.get('r_volt_thd', 0) + data.get('y_volt_thd', 0) + data.get('b_volt_thd', 0)) / 3
                                                            sql +=f",avg_volt_thd = {value}"

                                                        if data.get('r_current_thd') != None and data.get('y_current_thd') != None and data.get('b_current_thd') != None:
                                                            value = int(data.get('r_current_thd', 0) + data.get('y_current_thd', 0) + data.get('b_current_thd', 0)) / 3
                                                            sql +=f",avg_current_thd = {value}"
                                                try:
                                                    if prev_machine_kwh != 0:
                                                        if prev_machine_kwh > meter_kwh:
                                                            msg_insert_query = f'''insert into master_sms(meter_id,previous_machine_kwh,current_machine_kwh,parameter_type,created_on) values('{meter_id}','{prev_machine_kwh}','{meter_kwh}','CkWh',now())'''
                                                            createFolder(f"alert/",f"message insert query - {msg_insert_query}")
                                                            cursor.execute(msg_insert_query)
                                                            db.commit()     
                                                except Exception as e:
                                                    createFolder(f"Logs/GatewayErrorLog/",f"Error Checking message ->> {e}")
                                                    
                                                sql += f" WHERE meter_id = '{meter_id}'"
                                                createFolder(f"gateway_log/{mac}-Log/",f"Current Power Update - {sql}")
                                                cursor.execute(sql)
                                                db.commit() 
                                                if master_kwh == 0 and meter_kwh > 0:
                                                    query = f" update  current_power set master_kwh = machine_kwh where meter_id = '{meter_id}'"
                                                    createFolder(f"gateway_log/{mac}-Log/",f"Current Power Update - {sql}")
                                                    cursor.execute(query)
                                                    db.commit()                                                     
                                            except Exception as e:
                                                    createFolder(f"Logs/GatewayErrorLog/",f"Error Updating Current Power ->> {e}")
                                            
                                                  # Checking whether it is polling meter . if yes , Caculations will be applied to find equipment wise kwh .
                                            if is_poll_meter == 'yes':
                                                try:
                                                  
                                                    sql_cal = "UPDATE current_power SET "
                                                    if equipment_id == 0 :
                                                        equipment_id = meter_id
                                                        meter_communication = 'common'
                                                    else:
                                                        meter_communication = 'equipment'
                                                    calc_sql = f'''SELECT 
                                                                    mec.formula2, cp.meter_id ,  
                                                                    IFNULL(CASE WHEN mmf.kwh = '*' THEN (cp.machine_kwh - cp.master_kwh) * mmf.kwh_value WHEN 
                                                                mmf.kwh = '/' THEN (cp.machine_kwh - cp.master_kwh) / mmf.kwh_value ELSE cp.kwh END ,0)
                                                                    AS kwh,
                                                                    cp.plant_id
                                                            FROM
                                                                master_equipment_calculations mec INNER JOIN
                                                                master_equipment_meter mem ON mec.equipment_id = mem.equipment_id 
                                                                INNER JOIN current_power cp ON mem.meter_id = cp.meter_id
                                                                
                                                                INNER JOIN master_meter_factor mmf ON mmf.meter_id = cp.meter_id AND mmf.plant_id = cp.plant_id
                                                            
                                                            WHERE
                                                                mec.equipment_id = {equipment_id} and mec.meter_communication = '{meter_communication}' and mem.meter_communication = '{meter_communication}' and mec.status = 'active' '''
                                                    formula_details = cursor.execute(calc_sql)
                                                    formula_details = cursor.fetchall()
                                                    createFolder(f"gateway_log/{mac}-Log/",f"formula_details - {formula_details}")
                                                    
                                                    if len(formula_details) > 0:
                                                        dict = {}
                                                        total_kwh = 0
                                                        formula = ''
                                                        for detail in formula_details:
                                                            # createFolder(f"gateway_log/{mac}-Log/",f"detail - {detail}")
                                                            formula = detail['formula2']
                                                            dict[detail['meter_id']] = detail['kwh']
                                                            total_kwh += detail['kwh']
                                                        createFolder(f"gateway_log/{mac}-Log/",f"data value dict - {dict}")
                                                        
                                                        if formula != '':
                                                            try:
                                                                createFolder(f"gateway_log/{mac}-Log/",f"Formula - {formula}")
                                                                equipment_kwh = eval(formula)
                                                                sql_cal += f" equipment_kwh= {equipment_kwh} ,total_kwh = {total_kwh}"
                                                                data = f"equipment_kwh= {equipment_kwh} ,total_kwh = {total_kwh}"
                                                                createFolder(f"gateway_log/{mac}-Log/",f"calculated data - {data}")
                                                            except Exception as e:
                                                                createFolder(f"Logs/FormulaError/",f"Error Evaluating formula -->> {e}")
                                                        else:
                                                            createFolder(f"gateway_log/{mac}-Log/",f"NO Formula - {formula_details}")
                                                    else:
                                                        
                                                        calc_sql = f'''
                                                        select 
                                                            mm.meter_id ,
                                                            mm.equipment_id,
                                                            mm.plant_id, 
                                                            IFNULL(CASE WHEN mmf.kwh = '*' THEN (cp.machine_kwh - cp.master_kwh) * mmf.kwh_value WHEN 
                                                                mmf.kwh = '/' THEN (cp.machine_kwh - cp.master_kwh) / mmf.kwh_value ELSE cp.kwh END ,0)
                                                                    AS kwh
                                                        from current_power cp
                                                            inner join master_meter mm on mm.meter_id = cp.meter_id 
                                                            inner join master_meter_factor mmf on mmf.meter_id = mm.meter_id and mmf.plant_id = mm.plant_id
                                                            where mm.meter_id = {meter_id} and mm.status = 'active' '''
                                                        formula_details = cursor.execute(calc_sql)
                                                        formula_details = cursor.fetchall()
                                                        # print(calc_sql)
                                                
                                                        if len(formula_details)>0:     
                                                            total_kwh = 0
                                                            for detail in formula_details:
                                                                equipment_kwh = detail["kwh"]
                                                                total_kwh += detail['kwh']
                                                                sql_cal += f" equipment_kwh= {equipment_kwh} ,total_kwh = {total_kwh}"
                                                                data = f"equipment_kwh= {equipment_kwh} ,total_kwh = {total_kwh}"
                                                                createFolder(f"gateway_log/{mac}-Log/",f"calculated data - {data}")
                                                
                                                            createFolder(f"gateway_log/{mac}-Log/",f"No Calculation data found for Equipment - {equipment_id} !")
                                                    sql_cal += f" WHERE meter_id = '{meter_id}'"
                                                    createFolder(f"gateway_log/{mac}-Log/",f"Equipment_kwh  - query:{sql_cal}")
                                                    cursor.execute(sql_cal)
                                                    db.commit()
                                                    createFolder(f"gateway_log/{mac}-Log/",f"Equipment_kwh Updated Sucessfully !")

                                                except Exception as e:
                                                    createFolder(f"Logs/GatewayErrorLog/",f"Error Updating Equipment kwh ->> {e}")

                                            try:
                                                t_current = 0
                                                sql_t = f''' 
                                                        select 
                                                            mm.meter_id, 
                                                            cp.meter_status,mm.meter_state_condition2,mm.meter_state_condition4,
                                                            case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end as t_current
                                                            from current_power cp 
                                                            inner join master_meter mm on mm.meter_id = cp.meter_id
                                                            inner join master_meter_factor mmf on mmf.meter_id = mm.meter_id and mmf.plant_id = mm.plant_id
                                                        where mm.meter_id = {meter_id} '''
                                                cursor.execute(sql_t)
                                                data_t = cursor.fetchall()
                                                for row_t in data_t:
                                                    t_current = row_t["t_current"]
                                                    meter_state_condition2 = row_t["meter_state_condition2"]
                                                    meter_state_condition4 = row_t["meter_state_condition4"]
                                                    meter_status = row_t["meter_status"]
                                                    
                                                createFolder(f"gateway_log/{mac}-Log/",f"Calling Polling Data Updation t_current --{t_current} condition2 - {meter_state_condition2} condition4 - {meter_state_condition4} !")
                                                update_polling_data(t_current,meter_state_condition2,meter_state_condition4,meter_status,meter_id,mac,cursor,db)
                                                createFolder(f"gateway_log/{mac}-Log/",f"Polling Data Updation Completed !")
                                            except Exception as e:
                                                createFolder(f"Logs/GatewayErrorLog/",f"Error Updating Polling data ->> {e}")

                                            try:
                                                
                                                cursor.execute(f"SELECT alarm_target_id,parameter_name,color_1,color_2,color_3,color_4,color_5,color_6,previous_values,meter_id FROM master_alarm_target WHERE status = 'active' AND meter_id like '%,{meter_id},%' ")
                                                createFolder(Logfile_name,f"SELECT alarm_target_id,parameter_name,color_1,color_2,color_3,color_4,color_5,color_6,previous_values,meter_id FROM master_alarm_target WHERE status = 'active' AND meter_id like '%,{meter_id},%' ")
                                                al_data = cursor.fetchall()

                                                if len(al_data) > 0:
                                                    # createFolder(Logfile_name, f"{len(al_data)} Alarms found for Machine {meter_id}")

                                                    for i in al_data:
                                                        c1= i["color_1"]
                                                        c2 = i["color_2"]
                                                        c3 = i["color_3"]
                                                        alarm_target_id = i['alarm_target_id']
                                                        parameter_name = i["parameter_name"]
                                                        value = i["parameter_name"]
                                                        value = value.lower()
                                                        if value=='kwh':
                                                            value=locals()['actual_kwh']                                        
                                                        elif value =='machine_kwh':
                                                            value=locals()['meter_kwh']
                                                        elif value =='kw':
                                                            value=data['t_watts']
                                                        else:
                                                            value = data[value]  #the current reading from plc for the parameter 
                                                                            
                                                        # createFolder(Logfile_name,f"Validating alarm {i}-{al_data[i][1]}")
                                                        
                                                        if value<c1:
                                                            zone='low'
                                                            trigger_alarm='True'
                                                            alert_str= parameter_name + ' too low! Observed reading : '+str(value)
                                                
                                                        elif value>=c1 and value<=c2:
                                                            zone='normal'
                                                            trigger_alarm='False'

                                                        elif value>=c2 and value<=c3:
                                                            zone='Warning'
                                                            trigger_alarm='True'
                                                            alert_str= parameter_name + ' Value reach high soon! Observed reading : '+str(value)

                                                        elif value>=c3 :
                                                            zone='high'
                                                            trigger_alarm='True'
                                                            alert_str= parameter_name + ' too high! Observed reading : '+str(value)

                                                        # createFolder(Logfile_name,f"Validated alarm {i}-{al_data[i][1]} | Zone : {zone} , TriggerAlarm : {trigger_alarm}")

                                                        # from each alarm, master alarm target prev values, machine id list for indexing prev vals and updating the machine's zone 
                                                        
                                                        prev = i["previous_values"][1:-1].split(',')
                                                        mids = i["meter_id"][1:-1].split(',')
                                                        
                                                        ind=mids.index(str(meter_id))
                                                        try:
                                                            prev_val=prev[ind]
                                                        except Exception as e:
                                                            prev_val = ""
                                                        loop_count = (ind+1) - len(prev)
                                                        
                                                        if int(loop_count) > 0:
                                                            for i in range(loop_count):
                                                                prev.append('')
                                                        
                                                        if prev_val!=zone:
                                                            prev_str=','
                                                            prev[ind]=zone
                                                            for j in range(len(prev)):
                                                                prev_str += prev[j] +','
                                                        
                                                            cursor.execute(f"update master_alarm_target set previous_values='{prev_str}' where alarm_target_id={alarm_target_id}")
                                                            db.commit()
                                                            
                                                            if prev_val == 'high' and zone=='low' or prev_val == 'low' and zone=='high' or prev_val == 'high' and zone == 'Warning' or prev_val == 'Warning' and zone == 'high' or prev_val == 'low' and zone == 'Warning' or prev_val == 'Warning' and zone == 'low':
                                                                trigger_alarm='Fluctuation'
                                                                
                                                                alert_str= 'Machine '+str(meter_id) +'  '+ parameter_name + ' fluctuating from ' + prev_val +'to '+ zone + ' current reading :  '+str(value)
                                                                cursor.execute(f"update present_alarm set start_time=NOW() , description='{alert_str}' where meter_id='{meter_id}' and parameter_name='{parameter_name}'")
                                                                db.commit()
                                                    
                                                            if trigger_alarm=='True':
                                                                createFolder(f"gateway_log/{mac}-Log/",f"trigger alarm is true")                           
                                                                
                                                                cursor.execute(f"select mill_date,mill_shift from master_shifts where plant_id = '{plant_id}'" )
                                                                recs = cursor.fetchall()
                                                                if len(recs)>0:
                                                                    for row in recs:
                                                                        mill_date = row["mill_date"]
                                                                        mill_shift=row["mill_shift"]

                                                                    cursor.execute(f"insert into present_alarm(description,meter_id,start_time,parameter_name,alarm_target_id,mill_date,mill_shift) values('{alert_str}',{meter_id},NOW(),'{parameter_name}','{alarm_target_id}','{mill_date}','{mill_shift}')")
                                                                    db.commit()
                                                                    
                                                                # createFolder(Logfile_name,f"Inserted new Alarm For Machine:{meter_id} {i}-{al_data[i][1]} ")
                                                        else:
                                                            
                                                            createFolder(f"gateway_log/{mac}-Log/",f"No Changes Seen in Alarm {i}-{al_data[i][1]} ")
                                                
                                                        if prev_val in('high','low','Warning') and trigger_alarm == 'False':
                                                            cursor.execute(f"update present_alarm set stop_time=NOW() where meter_id='{meter_id}' and parameter_name='{parameter_name}' and stop_time is NULL")
                                                            db.commit()
                                                            # createFolder(Logfile_name,f"Alarm Ended For Machine:{meter_id} {i}-{al_data[i][1]} ")
                                                
                                                        db.commit()
                                                else:
                                                    
                                                    createFolder(f"gateway_log/{mac}-Log/",f"Alarms Not Created for Machine {meter_id}")

                                                cursor.execute(f'''update present_alarm set stop_time = now()
                                                            where meter_id = {meter_id} and stop_time is NULL 
                                                            and alarm_target_id in 
                                                            (select alarm_target_id 
                                                            FROM master_alarm_target WHERE status <> 'active' AND meter_id like '%,{meter_id},%' )''')
                                                db.commit()

                                            except Exception as e:
                                                createFolder(f"Logs/GatewayErrorLog/",f"Error in alarm Block Error for meter - {meter_id} :{e}")
                                    else:
                                        createFolder(f"Logs/GatewayErrorLog/",f"No data Match for this slave_id:{slave_id} and mac :{mac}") 
                                        continue
                                                       
                                except Exception as e:
                                        createFolder(f"Logs/GatewayErrorLog/",f"Error in Power Update Function :{e}")                
                        else:
                           createFolder(f"gateway_log/{mac}-Log/", f"msg_type - {msg_type}") 

                        sql = f" update gateway.log set status = 'yes' where id = {id}"
                        createFolder(f"gateway_log/{mac}-Log/", f"closing gateway log record - {id}")
                        cursor.execute(sql)
                        db.commit()
                    except Exception as e:
                        createFolder(f"Logs/GatewayErrorLog/",f"Error in Modbus data :{e}")
                createFolder('Logs/GatewayProcess/', f"Process End ({c_mac}) !! ")
            except Exception as e:
                createFolder(f"Logs/GatewayErrorLog/",f"Error in Power UpdateFunction :{e}")

    except Exception as e:
        createFolder(f"Logs/GatewayErrorLog/",f"Error in Power Update Function :{e}")

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

        try:
            createFolder('main_process/', f"Calling Power Data  !! ")
            db = pymysql.connect(host="localhost", user="AIC_PY_EXE",passwd="02da3b733bb289f26031348dcf045b69", db="ems_v1" , port= 3308)
            cursor = db.cursor(pymysql.cursors.DictCursor)
            power_data(db,cursor)
            if db:
                db.close()
            createFolder('main_process/', f"Power Data execution completed !! ")
            time.sleep(5)
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder(f"{Logfile_name}main_loop_error/", f"Error In While Loop -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")