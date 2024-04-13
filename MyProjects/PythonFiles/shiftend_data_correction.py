import sys
import os
import datetime
from  datetime import date,timedelta
import shutil
import time
from dateutil import parser
import pymysql


shift_end_flag=0
Logfile_name = 'LogS/'
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
def data_correction(cursor,db):
    try:
        createFolder(f"main_process/", f"Data Correction Function Called!!")
        plant_id= 0
        equipment_kwh = 0
        mill_date= date.today()
        mill_shift = 0
        is_manual_call = ''
        current_time = datetime.datetime.now().strftime('%H:%M')
        current_date = date.today()
        shift_end_time = '1900-01-01 08:00:00'
        # shift_end_time = ''
        if current_time == '00:00' and current_date.day == 1:
            sql = f"update current_power set actual_demand = 0"
            cursor.execute(sql)
            db.commit()
            print(current_date.day)
            createFolder(f"main_process/", f"Reseting Actual Demand!!")
        
        query = f"select d.*,s.shift1_end_time,s.shift2_end_time,s.shift3_end_time,s.shift1_start_time,s.shift2_start_time,s.shift3_start_time from data_correction d inner join master_shifts s on s.plant_id = d.plant_id"
        cursor.execute(query)
        data = cursor.fetchall()
        if len(data)>0:
            for row in data:
                try:
                    createFolder(f"main_process/", f"Data Correction Function Start!!")
                    record_id = row["id"]
                    plant_id = row["plant_id"]
                    mill_date = row["mill_date"]
                    mill_shift = row["mill_shift"]
                    created_on = row["created_on"]
                    is_manual_call = row["is_manual_call"]

                    if mill_shift == 1:
                        shift_end_time = row["shift1_end_time"]
                    elif mill_shift == 2:
                        shift_end_time = row["shift2_end_time"]
                    elif mill_shift == 3:
                        shift_end_time = row["shift3_end_time"]

                    mill_date = parse_date(mill_date)
                    month_year = f"{mill_month[mill_date.month]}{mill_date.year}"
                    
                    sql = f"SELECT equipment_id , meter_id FROM master_meter where  plant_id = '{plant_id}' and is_poll_meter = 'yes' "
                    cursor.execute(sql)
                    equipments = cursor.fetchall()
                    if len(equipments) > 0:
                        for equipment in equipments:
                            try:
                                equipment_id = equipment['equipment_id']
                                meter_id = equipment['meter_id']
                                if equipment_id == 0 :
                                    equipment_id = meter_id
                                    meter_communication = 'common'
                                else:
                                    meter_communication = 'equipment'
                                sql = f'''
                                    SELECT 
                                            mec.formula2, cp.meter_id ,  
                                            IFNULL(CASE WHEN mmf.kwh = '*' THEN (cp.machine_kwh - cp.master_kwh) * mmf.kwh_value WHEN 
                                        mmf.kwh = '/' THEN (cp.machine_kwh - cp.master_kwh) / mmf.kwh_value ELSE cp.kwh END ,0)
                                            AS kwh,
                                            cp.plant_id
                                    FROM
                                        master_equipment_calculations mec INNER JOIN
                                        master_equipment_meter mem ON mec.equipment_id = mem.equipment_id 
                                        INNER JOIN
                                        ems_v1_completed.power_{month_year} cp ON mem.meter_id = cp.meter_id
                                        
                                        INNER JOIN master_meter_factor mmf ON mmf.meter_id = cp.meter_id AND mmf.plant_id = cp.plant_id
                                    
                                    WHERE
                                        mec.equipment_id = {equipment_id} and mec.meter_communication = '{meter_communication}' and mem.meter_communication = '{meter_communication}'  and mec.status = 'active' 
                                        and cp.mill_date = '{mill_date}' and cp.mill_shift = '{mill_shift}' '''
                                # createFolder(f"{Logfile_name}/",f" Equipment - {sql} !")
                                    
                                cursor.execute(sql)
                                
                                formula_details = cursor.fetchall()
                                if len(formula_details) > 0:
                                    dict = {}
                                    total_kwh = 0
                                    formula = ''
                                    for detail in formula_details:
                                        formula = detail['formula2']
                                        dict[detail['meter_id']] = detail['kwh']
                                        total_kwh += detail['kwh']
                                    if formula != '':
                                        try:
                                            equipment_kwh = eval(formula)
                                            sql = f"update ems_v1_completed.power_{month_year} set equipment_kwh= {equipment_kwh} ,total_kwh = {total_kwh} where meter_id = {meter_id} and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' "
                                            createFolder(f"{Logfile_name}/",f"Updating equipment_kwh to {meter_id} - {sql}")
                                            cursor.execute(sql)
                                            db.commit()
                                        except Exception as e:
                                            createFolder(f"{Logfile_name}/",f"Error Evaluating formula -->> {e}")
                                    else:
                                        createFolder(f"{Logfile_name}/",f"Formula is empty - {formula_details}")
                                else:
                                    createFolder(f"{Logfile_name}/",f"No Calculation data found for Equipment - {equipment_id} !")
                                    
                                    sql = f'''select 
                                                mm.meter_id ,
                                                mm.equipment_id,
                                                mm.plant_id, 
                                                IFNULL(CASE WHEN mmf.kwh = '*' THEN (cp.machine_kwh - cp.master_kwh) * mmf.kwh_value WHEN 
                                                    mmf.kwh = '/' THEN (cp.machine_kwh - cp.master_kwh) / mmf.kwh_value ELSE cp.kwh END ,0)
                                                        AS kwh
                                            from ems_v1_completed.power_{month_year} cp
                                                inner join master_meter mm on mm.meter_id = cp.meter_id 
                                                inner join master_meter_factor mmf on mmf.meter_id = mm.meter_id and mmf.plant_id = mm.plant_id
                                                where mm.meter_id = {meter_id} and mm.status = 'active'and cp.mill_date = '{mill_date}' and cp.mill_shift = '{mill_shift}' '''
                                    # createFolder(f"{Logfile_name}/",f"No Calculation data found  - {sql} !")
                                    cursor.execute(sql)
                                    formula_details = cursor.fetchall()
                                    
                                    if len(formula_details) > 0:
                                        total_kwh = 0
                                        for detail in formula_details:
                                            equipment_kwh = detail["kwh"]
                                            total_kwh += detail['kwh']
                                        sql_u = f"update ems_v1_completed.power_{month_year} set equipment_kwh= {equipment_kwh} ,total_kwh = {total_kwh} where meter_id = {meter_id} and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' "
                                        createFolder(f"{Logfile_name}/",f"Updating equipment_kwh to {meter_id} - {sql_u}")
                                        cursor.execute(sql_u)
                                        db.commit()
                                    else:
                                        createFolder(f"{Logfile_name}/",f"no data availanle {meter_id} - {sql}")
                                
                                try:
                                    update_r = f'''update manual_entry_history set revised_calculated_kwh = '{equipment_kwh}' where meter_id = '{meter_id}'and mill_date = '{mill_date}'and mill_shift = '{mill_shift}' '''
                                    cursor.execute(update_r)
                                    db.commit()

                                except Exception as e:
                                    exc_type, exc_obj, exc_tb = sys.exc_info()
                                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                    createFolder(f"{Logfile_name}/", f"For Loop - Error Updating reset calculated  Kwh -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")            
 

                            except Exception as e:
                                exc_type, exc_obj, exc_tb = sys.exc_info()
                                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                                createFolder(f"{Logfile_name}/", f"For Loop - Error Updating Equipment Wise Kwh -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")            
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    createFolder(f"{Logfile_name}/", f"Main Loop - Error Updating Equipment Wise Kwh -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")            
                try:

                    meter_status = 0
                    sql = f'''SELECT m.meter_id,c.meter_status,m.meter_state_condition2,m.meter_state_condition4,m.is_poll_meter,m.equipment_id ,m.plant_id ,c.t_current FROM master_meter m INNER JOIN 
                                ems_v1_completed.power_{month_year} c ON c.meter_id=m.meter_id 
                                where m.plant_id = {plant_id} and c.mill_date = '{mill_date}' and c.mill_shift = '{mill_shift}' '''
                    createFolder(f"{Logfile_name}/",f"Machine details {sql} ")
                    cursor.execute(sql)
                    data_p = cursor.fetchall()

                    if len(data_p)>0:
                        for row_p in data_p:
                            power_table_mc_status = row_p["meter_status"]
                            machine_state_condition2 = float(row_p["meter_state_condition2"])
                            machine_state_condition4 = float(row_p["meter_state_condition4"])
                            t_current = float(row_p["t_current"])
                            meter_id = row_p["meter_id"]
                            shift_date = date.today()

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

                                createFolder(f"{Logfile_name}/","Machine state changed ! .")

                                if power_table_mc_status == 0:
                                    if is_manual_call != 'yes':
                                        # query_str += ",shift_off_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,NOW()),0) + shift_off_time , off_time = shift_off_time"
                                        query_str += f",shift_off_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0) + shift_off_time , off_time = shift_off_time"
                                    query_str += ",shift_off_kwh = (machine_kwh - last_poll_consumption) + shift_off_kwh , off_kwh = shift_off_kwh"
                                    query_str += ",equipment_shift_off_kwh = (equipment_kwh - last_poll_equipment_kwh) + equipment_shift_off_kwh , equipment_off_kwh = equipment_shift_off_kwh"
                                elif power_table_mc_status == 1:
                                    if is_manual_call != 'yes':
                                        query_str += f",shift_idle_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0) + shift_idle_time , idle_time = shift_idle_time"
                                    query_str += ",shift_idle_kwh = (machine_kwh - last_poll_consumption) + shift_idle_kwh , idle_kwh = shift_idle_kwh"
                                    query_str += ",equipment_shift_idle_kwh = (equipment_kwh - last_poll_equipment_kwh) + equipment_shift_idle_kwh , equipment_idle_kwh = equipment_shift_idle_kwh"
                                elif power_table_mc_status == 2:
                                    if is_manual_call != 'yes':
                                        query_str += f",shift_on_load_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0) + shift_on_load_time , on_load_time = shift_on_load_time"
                                    query_str += ",shift_on_load_kwh = (machine_kwh - last_poll_consumption) + shift_on_load_kwh , on_load_kwh = shift_on_load_kwh"
                                    query_str += ",equipment_shift_on_load_kwh = (equipment_kwh - last_poll_equipment_kwh) + equipment_shift_on_load_kwh , equipment_on_load_kwh = equipment_shift_on_load_kwh"
                                
                                if is_manual_call == 'yes':
                                    # TO take difference of the current polling data .
                                    sql = f'''UPDATE
                                                ems_v1_completed.power_{month_year}
                                            SET
                                                current_poll_consumption = machine_kwh - last_poll_consumption ,
                                                current_equipment_poll_consumption = equipment_kwh - last_poll_equipment_kwh 
                                                {query_str}
                                            WHERE
                                                meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                    createFolder(f"{Logfile_name}/",f"TO take difference of the current polling data -->> {sql}")
                                    cursor.execute(sql)
                                    db.commit()
                                    
                                else: 
                                    # TO take difference of the current polling data .
                                    sql = f'''UPDATE
                                                ems_v1_completed.power_{month_year}
                                            SET
                                                current_poll_consumption = machine_kwh - last_poll_consumption ,
                                                current_equipment_poll_consumption = equipment_kwh - last_poll_equipment_kwh ,
                                                current_poll_duration = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0)  
                                                {query_str}
                                            WHERE
                                                meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                    createFolder(f"{Logfile_name}/",f"TO take difference of the current polling data -->> {sql}")
                                    cursor.execute(sql)
                                    db.commit()
                                
                                    # Insert query Previous Machines status with start datetime and duration
                                    sql = f'''INSERT INTO ems_v1_completed.polling_data_{month_year}(meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , poll_duration , poll_consumption , equipment_consumption)
                                                SELECT meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , current_poll_duration , current_poll_consumption , current_equipment_poll_consumption FROM ems_v1_completed.power_{month_year} 
                                                WHERE meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                    createFolder(f"{Logfile_name}/",f"Inserting last polling data -->> {sql}")
                                    cursor.execute(sql)
                                    db.commit()

                                # Update query Current machine status and now time as start time and reset the duration
                                sql = f'''UPDATE
                                            ems_v1_completed.power_{month_year}
                                        SET
                                            meter_status = '{meter_status}' ,
                                            mc_state_changed_time = IFNULL(STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s'),0),
                                            current_poll_duration = 0 ,
                                            current_poll_consumption = 0 ,
                                            current_equipment_poll_consumption = 0 ,
                                            last_poll_consumption = machine_kwh ,
                                            last_poll_equipment_kwh = equipment_kwh
                                        WHERE
                                            meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                createFolder(f"{Logfile_name}/",f"Updating machine state changed time and resetting poll duration -->> {sql}")
                                cursor.execute(sql)
                                db.commit()

                            else:
                                createFolder(f"{Logfile_name}/","Machine status not changed . Duration updated for current Machine state !")

                                if power_table_mc_status == 0:
                                    if is_manual_call != 'yes':
                                        query_str += f",off_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0) + shift_off_time"
                                    query_str += ",off_kwh = machine_kwh - last_poll_consumption + shift_off_kwh"
                                    query_str += ",equipment_off_kwh = equipment_kwh - last_poll_equipment_kwh + equipment_shift_off_kwh"
                                elif power_table_mc_status == 1:
                                    if is_manual_call != 'yes':
                                        query_str += f",idle_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0) + shift_idle_time"
                                    query_str += ",idle_kwh = machine_kwh - last_poll_consumption + shift_idle_kwh"
                                    query_str += ",equipment_idle_kwh = equipment_kwh - last_poll_equipment_kwh + equipment_shift_idle_kwh"
                                elif power_table_mc_status == 2:
                                    if is_manual_call != 'yes':
                                        query_str += f",on_load_time = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0) + shift_on_load_time"
                                    query_str += ",on_load_kwh = machine_kwh - last_poll_consumption + shift_on_load_kwh"
                                    query_str += ",equipment_on_load_kwh = equipment_kwh - last_poll_equipment_kwh + equipment_shift_on_load_kwh"
                               
                                if is_manual_call == 'yes':
                                   
                                    sql = f'''UPDATE
                                                ems_v1_completed.power_{month_year}
                                            SET 
                                                current_poll_consumption = machine_kwh - last_poll_consumption ,
                                                current_equipment_poll_consumption = equipment_kwh - last_poll_equipment_kwh 
                                                
                                                {query_str}
                                            WHERE
                                                meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                    createFolder(f"{Logfile_name}/",f"Updating Current Polling Duration -->> {sql} ")
                                    cursor.execute(sql)
                                    db.commit()
                                else:
                                    sql = f'''UPDATE
                                                ems_v1_completed.power_{month_year}
                                            SET 
                                                current_poll_consumption = machine_kwh - last_poll_consumption ,
                                                current_equipment_poll_consumption = equipment_kwh - last_poll_equipment_kwh ,
                                                current_poll_duration = IFNULL(TIMESTAMPDIFF(SECOND,mc_state_changed_time,STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s')),0)  
                                                {query_str}
                                            WHERE
                                                meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                    createFolder(f"{Logfile_name}/",f"Updating Current Polling Duration -->> {sql} ")
                                    cursor.execute(sql)
                                    db.commit()
                                
                                    sql = f'''INSERT INTO ems_v1_completed.polling_data_{month_year}(meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , poll_duration , poll_consumption , equipment_consumption)
                                                SELECT meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , current_poll_duration , current_poll_consumption , current_equipment_poll_consumption FROM ems_v1_completed.power_{month_year} 
                                                WHERE meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                    createFolder(f"{Logfile_name}/",f"Inserting last polling data -->> {sql}")
                                    cursor.execute(sql)
                                    db.commit()

                                # Update query Current machine status and now time as start time and reset the duration
                                sql = f'''UPDATE
                                            ems_v1_completed.power_{month_year}
                                        SET
                                            meter_status = '{meter_status}' ,
                                            mc_state_changed_time = IFNULL(STR_TO_DATE(CONCAT(DATE('{shift_date}'), ' ', TIME('{shift_end_time}')), '%Y-%m-%d %H:%i:%s'),0),
                                            current_poll_duration = 0 ,
                                            current_poll_consumption = 0 ,
                                            current_equipment_poll_consumption = 0 ,
                                            last_poll_consumption = machine_kwh ,
                                            last_poll_equipment_kwh = equipment_kwh
                                        WHERE
                                            meter_id = '{meter_id}' and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                createFolder(f"{Logfile_name}/",f"Updating machine state changed time and resetting poll duration -->> {sql}")
                                cursor.execute(sql)
                                db.commit()
                                
                                sql_up = f'''UPDATE
                                            ems_v1_completed.power_{month_year}
                                        SET
                                            equipment_kwh = 0 
                                        WHERE
                                            meter_id in (select meter_id from master_meter where is_poll_meter = 'no') and mill_date = '{mill_date}' and mill_shift = '{mill_shift}' '''
                                createFolder(f"{Logfile_name}/",f"Updating Calculated kwh -->> {sql_up}")
                                cursor.execute(sql_up)
                                db.commit()
                                            
                    else:
                        createFolder(f"{Logfile_name}/", f" no data available..")            

                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    createFolder(f"{Logfile_name}/", f" Error polling update -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")            
                                
                try:
                    query_u = f''' insert into ems_v1.manual_update(plant_id,mill_date,mill_shift,created_on)
                                    values('{plant_id}','{mill_date}','{mill_shift}','{created_on}')'''
                    createFolder(f"{Logfile_name}/", f"manual update record - {query_u}")
                    cursor.execute(query_u)
                    db.commit()
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    createFolder(f"{Logfile_name}/", f" Error manual update record -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")            
                                        
                query = f" delete from ems_v1.data_correction where id = {record_id}"
                cursor.execute(query)
                db.commit()
                createFolder(f"{Logfile_name}/", f"closing data_correction record - {record_id}")
                createFolder(f"main_process/", f"Data Correction Function END - {plant_id}!!")
        else:
            print("No machines found !!")
            createFolder(f"{Logfile_name}/", f"No Record found !!")


    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder(f"{Logfile_name}/", f"Main Loop - Error Data Correction Function -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")            

while True:

    try:
        db = pymysql.connect(host="localhost", user="AIC_PY_DC",passwd="953a6275dca2bc4ff2d493c1ae8695b9", db="ems_v1" , port= 3308)
        # db = pymysql.connect(host="localhost", user="root",passwd="", db="ems_v1" , port= 3306)
        cursor = db.cursor(pymysql.cursors.DictCursor)
        data_correction(cursor,db)
        createFolder(f"main_process/", f"Data Correction execution completed!!")
        time.sleep(30)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder(f"{Logfile_name}main_loop_error/", f"Error In While Loop -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")