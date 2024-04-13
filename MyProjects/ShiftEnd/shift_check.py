import sys
import os
import datetime
from  datetime import date,timedelta
import shutil
import time
import pymysql

shift_end_flag=0
Logfile_name = 'LogS/'
date_time=datetime.datetime.now()

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

def shift_check(company_id,bu_id,plant_id,cursor):

    try:
        createFolder(f'LogS/{plant_id}/',"Shift Check")
        global act_shift
        global act_date
        global yesterday
        global shift1
        global shift2
        global shift3
        global curtime1
        global curtime
        global company_date
        global company_shift
        global shift_start
        
        def find_shift(company_id,bu_id,plant_id):

            # curtime = datetime.datetime.fromisoformat(timestamp)
            date_object = datetime.datetime.now()
            curtime1 = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            # current_date = datetime.datetime.fromisoformat(current_date)
            
            cursor.execute(f'''SELECT
                                    CONCAT(DATE_FORMAT(s.mill_date,'%Y-%m-%d'),' ',DATE_FORMAT(s.shift1_start_time,'%H:%i;%s')) AS shift1_start_time,
                                    DATE_ADD(CONCAT(DATE_FORMAT(s.mill_date,'%Y-%m-%d'),' ',DATE_FORMAT(s.shift1_start_time,'%H:%i;%s')),INTERVAL s.shift1_time MINUTE) AS shift1_end_time,
                                    
                                    DATE_ADD(CONCAT(DATE_FORMAT(s.mill_date,'%Y-%m-%d'),' ',DATE_FORMAT(s.shift1_start_time,'%H:%i;%s')),INTERVAL s.shift1_time MINUTE) AS shift2_start_time,
                                    DATE_ADD(DATE_ADD(CONCAT(DATE_FORMAT(s.mill_date,'%Y-%m-%d'),' ',DATE_FORMAT(s.shift1_start_time,'%H:%i;%s')),INTERVAL s.shift1_time MINUTE),INTERVAL s.shift2_time MINUTE) AS shift2_end_time,
                                    
                                    DATE_ADD(DATE_ADD(CONCAT(DATE_FORMAT(s.mill_date,'%Y-%m-%d'),' ',DATE_FORMAT(s.shift1_start_time,'%H:%i;%s')),INTERVAL s.shift1_time MINUTE),INTERVAL s.shift2_time MINUTE) AS shift3_start_time,
                                    DATE_ADD(DATE_ADD(DATE_ADD(CONCAT(DATE_FORMAT(s.mill_date,'%Y-%m-%d'),' ',DATE_FORMAT(s.shift1_start_time,'%H:%i;%s')),INTERVAL s.shift1_time MINUTE),INTERVAL s.shift2_time MINUTE),INTERVAL s.shift3_time MINUTE) AS shift3_end_time,

                                    s.mill_date,
                                    s.mill_shift
                                FROM
                                    master_shifts_exe s where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' and created_for = 'datalogger' ''' )
            rows = cursor.fetchall()

            for row in rows:

                shift1_start = row['shift1_start_time']
                shift1_end = row['shift1_end_time']

                shift2_start = row['shift2_start_time']
                shift2_end = row['shift2_end_time']

                shift3_start = row['shift3_start_time']
                shift3_end = row['shift3_end_time']

                company_date = row['mill_date']
                company_shift = row['mill_shift']

            if shift1_start[:10] == shift1_end[:10]:
                # if same date means . Compare it with hour and minutes 
                if curtime1[11:] >= shift1_start[11:] and curtime1[11:] < shift1_end[11:]:
                    act_shift = 1
                    act_date = date.today()
                    shift_start = shift1_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
            elif shift1_end[11:] == '00:00:00':

                if curtime1[11:] >= shift1_start[11:] and curtime1[11:] < '23:59:59':
                    act_shift = 1
                    act_date = date.today()
                    shift_start = shift1_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
            elif curtime1 > shift1_start and curtime1 <= shift1_end:

                temp_dt = shift1_start[:10] + " 23:59:59"

                if curtime1 > temp_dt:
                    act_shift = 1 
                    yesterday = (date_object - timedelta(days=1))
                    act_date = yesterday.strftime("%Y-%m-%d")
                    shift_start = shift1_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                else:
                    act_shift = 1
                    act_date = current_date
                    shift_start = shift1_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift

            if shift2_start[:10] == shift2_end[:10]:
                # if same date means . Compare it with hour and minutes 
                if shift2_start[11:] == '00:00:00' and curtime1 >= shift2_start:
                    act_shift = 2
                    yesterday = (date_object - timedelta(days=1))
                    act_date = yesterday.strftime("%Y-%m-%d")
                    shift_start = shift3_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift

                elif curtime1[11:] >= shift2_start[11:] and curtime1[11:] < shift2_end[11:]:
                    act_shift = 2
                    act_date = date.today()
                    shift_start = shift2_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
            elif shift2_end[11:] == '00:00:00':

                if curtime1[11:] >= shift2_start[11:] and curtime1[11:] < '23:59:59':
                    act_shift = 2
                    act_date = date.today()
                    shift_start = shift2_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
            elif curtime1 > shift2_start and curtime1 <= shift2_end:
                temp_dt = shift2_start[:10] + " 23:59:59"

                if curtime1 > temp_dt:
                    act_shift = 2 
                    yesterday = (date_object - timedelta(days=1))
                    act_date = yesterday.strftime("%Y-%m-%d")
                    shift_start = shift2_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                else:
                    act_shift = 2
                    act_date = current_date
                    shift_start = shift2_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift

            if shift3_start[:10] == shift3_end[:10]:
                # if same date means . Compare it with hour and minutes 
                if shift3_start[11:] == '00:00:00' and curtime1 >= shift3_start :
                    act_shift = 3 
                    yesterday = (date_object - timedelta(days=1))
                    act_date = yesterday.strftime("%Y-%m-%d")
                    shift_start = shift3_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
                elif curtime1[11:] >= shift3_start[11:] and curtime1[11:] < shift3_end[11:]:
                    act_shift = 3
                    if shift1_start[:10] == shift3_start[:10]:
                        act_date = date.today()
                    else:
                        yesterday = (date_object - timedelta(days=1))
                        act_date = yesterday.strftime("%Y-%m-%d")
                    shift_start = shift3_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
            elif shift3_end[11:] == '00:00:00':

                if curtime1[11:] >= shift3_start[11:] and curtime1[11:] < '23:59:59':
                    act_shift = 3
                    act_date = date.today()
                    shift_start = shift3_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
            elif curtime1 > shift3_start and curtime1 <= shift3_end:
                temp_dt = shift3_start[:10] + " 23:59:59"

                if curtime1 > temp_dt:
                    act_shift = 3 
                    yesterday = (date_object - timedelta(days=1))
                    act_date = yesterday.strftime("%Y-%m-%d")
                    shift_start = shift3_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift
                
                else:
                    act_shift = 3
                    act_date = current_date
                    shift_start = shift3_start[11:]
                    return act_date , act_shift , shift_start , company_date , company_shift

        data = find_shift(company_id,bu_id,plant_id)
        if data:
            act_date = data[0]
            act_shift = data[1]
            shift_start = data[2]
            company_date = data[3]
            company_shift = data[4]

            createFolder(f'LogS/{plant_id}/', f"if {company_shift} != {act_shift} or {company_date} != {act_date} 00:00:00")
            if str(company_shift) != str(act_shift) or str(company_date) != f"{act_date} 00:00:00":
                global shift_end_flag
                shift_end_flag = 1
                createFolder(f'LogS/{plant_id}/',f"Shift end flag = 1 || active_date = {act_date} , active_shift = {act_shift}")
        else:
            createFolder(f'LogS/{plant_id}/',"No shift conditions are matching ")

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder(f'LogS/{plant_id}/', f"Error in shift_check ---->>> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")

def shift_end(company_id,bu_id,plant_id,db,cursor):

    try:
        mill_date = date.today()
        mill_shift = 0
        is_fapi_call = ''

        global act_date, act_shift,shift_start
        
        createFolder(f"{Logfile_name}{plant_id}/", f"Shift End Called {shift_start}!")

        cursor.execute(f"SELECT * from ems_v1.master_shifts_exe where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' and created_for = 'datalogger'")
        rows=cursor.fetchall()
        for row in rows:
            mill_date= row['mill_date']
            mill_shift= row['mill_shift']
            prev_mill_date = mill_date
            prev_mill_shift = mill_shift
            shift_date_n = date.today()
        
        global shift_end_flag
        shift_end_flag=0

        act_month_year=mill_date.strftime("%m%Y")
        power_table = f"power_{act_month_year}"
        power_analysis_table = f"power_analysis_{act_month_year}"
        alarm_table = f"alarm_{act_month_year}"
        poll_table = f"polling_data_{act_month_year}"
        createFolder(f"{Logfile_name}{plant_id}/", "Fetched Records From masterShifts ")

        sql=f'''CREATE TABLE IF NOT EXISTS ems_v1_completed.{power_table}(
        `power_id` int(11) NOT NULL AUTO_INCREMENT,
        `company_id` int(11) NOT NULL DEFAULT 0,
        `bu_id` int(11) NOT NULL DEFAULT 0,
        `plant_id` int(11) NOT NULL DEFAULT 0,
        `plant_department_id` int(11) NOT NULL DEFAULT 0,
        `equipment_group_id` int(11) NOT NULL DEFAULT 0,
        `meter_id` int(11) NOT NULL DEFAULT 0,
        `design_id` int(11) NOT NULL DEFAULT 0,
        `beam_id` int(11) NOT NULL DEFAULT 0,
        `date_time` datetime NOT NULL,
        `date_time1` datetime NOT NULL,
        `mill_date` datetime NOT NULL,
        `mill_shift` varchar(1) NOT NULL,
        `vln_avg` double NOT NULL DEFAULT 0,
        `r_volt` double NOT NULL DEFAULT 0,
        `y_volt` double NOT NULL DEFAULT 0,
        `b_volt` double NOT NULL DEFAULT 0,
        `vll_avg` double NOT NULL DEFAULT 0,
        `ry_volt` double NOT NULL DEFAULT 0,
        `yb_volt` double NOT NULL DEFAULT 0,
        `br_volt` double NOT NULL DEFAULT 0,
        `t_current` double NOT NULL DEFAULT 0,
        `r_current` double NOT NULL DEFAULT 0,
        `y_current` double NOT NULL DEFAULT 0,
        `b_current` double NOT NULL DEFAULT 0,
        `t_watts` double NOT NULL DEFAULT 0,
        `r_watts` double NOT NULL DEFAULT 0,
        `y_watts` double NOT NULL DEFAULT 0,
        `b_watts` double NOT NULL DEFAULT 0,
        `t_var` double NOT NULL DEFAULT 0,
        `r_var` double NOT NULL DEFAULT 0,
        `y_var` double NOT NULL DEFAULT 0,
        `b_var` double NOT NULL DEFAULT 0,
        `t_voltampere` double NOT NULL DEFAULT 0,
        `r_voltampere` double NOT NULL DEFAULT 0,
        `y_voltampere` double NOT NULL DEFAULT 0,
        `b_voltampere` double NOT NULL DEFAULT 0,
        `avg_powerfactor` double NOT NULL DEFAULT 0,
        `r_powerfactor` double NOT NULL DEFAULT 0,
        `y_powerfactor` double NOT NULL DEFAULT 0,
        `b_powerfactor` double NOT NULL DEFAULT 0,
        `powerfactor` double NOT NULL DEFAULT 0,
        `kWh` double NOT NULL DEFAULT 0,
        `kvah` double NOT NULL DEFAULT 0,
        `kw` double NOT NULL DEFAULT 0,
        `kvar` double NOT NULL DEFAULT 0,
        `power_factor` double NOT NULL DEFAULT 0,
        `kva` double NOT NULL DEFAULT 0,
        `frequency` double NOT NULL DEFAULT 0,
        `machine_status` int(11) NOT NULL DEFAULT 0,
        `meter_status` int(11) NOT NULL DEFAULT 0,
        `status` int(11) NOT NULL DEFAULT 0,
        `created_on` timestamp NOT NULL DEFAULT current_timestamp(),
        `created_by` int(11) NOT NULL DEFAULT 0,
        `modified_on` varchar(30) NOT NULL,
        `modified_by` int(11) NOT NULL DEFAULT 0,
        `machine_kWh` double NOT NULL DEFAULT 0,
        `master_kwh` double NOT NULL DEFAULT 0,
        `start_kWh` double NOT NULL DEFAULT 0,
        `end_kWh` double NOT NULL DEFAULT 0,
        `imei` varchar(255) DEFAULT NULL,
        `sid` int(11) DEFAULT NULL,
        `reverse_machine_kwh` float NOT NULL DEFAULT 0,
        `reverse_master_kwh` float NOT NULL DEFAULT 0,
        `reverse_kwh` float NOT NULL DEFAULT 0,
        `off_time` int(11) NOT NULL DEFAULT 0,
        `idle_time` int(11) NOT NULL DEFAULT 0,
        `on_load_time` int(11) NOT NULL DEFAULT 0,
        `kwh_msb` int(11) NOT NULL DEFAULT 0,
        `kwh_lsb` int(11) NOT NULL DEFAULT 0,
        `mc_state_changed_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
        `current_poll_duration` int(11) NOT NULL DEFAULT 0,
        `shift_off_time` int(11) NOT NULL DEFAULT 0,
        `shift_idle_time` int(11) NOT NULL DEFAULT 0,
        `shift_on_load_time` int(11) NOT NULL DEFAULT 0,
        `current_poll_consumption` double NOT NULL DEFAULT 0,
        `last_poll_consumption` double NOT NULL DEFAULT 0,
        `off_kwh` double NOT NULL DEFAULT 0,
        `idle_kwh` double NOT NULL DEFAULT 0,
        `on_load_kwh` double NOT NULL DEFAULT 0,
        `shift_off_kwh` double NOT NULL DEFAULT 0,
        `shift_idle_kwh` double NOT NULL DEFAULT 0,
        `shift_on_load_kwh` double NOT NULL DEFAULT 0,
        `demand` double(18,3) NOT NULL DEFAULT 0.000,
        `meter_status_code` int(11) NOT NULL DEFAULT 0,
        `equipment_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `total_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `demand_dtm` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
        `actual_demand` double(18,3) NOT NULL DEFAULT 0.000,
        `current_equipment_poll_consumption` double(18,3) NOT NULL DEFAULT 0.000,
        `last_poll_equipment_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_shift_off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_shift_idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_shift_on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `actual_ton` double(18,3) NOT NULL DEFAULT 0.000,
        `r_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `y_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `b_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `avg_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `r_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `y_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `b_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `avg_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `runhour_msb` double(18,3) NOT NULL DEFAULT 0.000,
        `runhour_lsb` double(18,3) NOT NULL DEFAULT 0.000,
        `runhour` double(18,3) NOT NULL DEFAULT 0.000,
        `ry_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `yb_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `br_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `vll_avg_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `vln_avg_thd` double(18,3) NOT NULL DEFAULT 0.000,
        PRIMARY KEY (`power_id`),
        KEY `power_API` (`meter_id`,`mill_date`,`mill_shift`,`status`,`meter_status_code`),
        KEY `idx_comp_dtl` (`meter_id`,`mill_date`,`status`)
        )
        '''
        cursor.execute(sql)
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", f"Created power_{act_month_year} table ")
        
        sql=f""" CREATE TABLE  IF NOT EXISTS ems_v1_completed.{power_analysis_table}(
        `id` int(11) NOT NULL AUTO_INCREMENT,
        `power_id` int(11) NOT NULL DEFAULT 0,
        `company_id` int(11) NOT NULL DEFAULT 0,
        `bu_id` int(11) NOT NULL DEFAULT 0,
        `plant_id` int(11) NOT NULL DEFAULT 0,
        `plant_department_id` int(11) NOT NULL DEFAULT 0,
        `equipment_group_id` int(11) NOT NULL DEFAULT 0,
        `meter_id` int(11) NOT NULL DEFAULT 0,
        `date_time` datetime NOT NULL,
        `mill_date` datetime NOT NULL,
        `mill_shift` varchar(1) NOT NULL,
        `vln_avg` double NOT NULL DEFAULT 0,
        `r_volt` double NOT NULL DEFAULT 0,
        `y_volt` double NOT NULL DEFAULT 0,
        `b_volt` double NOT NULL DEFAULT 0,
        `vll_avg` double NOT NULL DEFAULT 0,
        `ry_volt` double NOT NULL DEFAULT 0,
        `yb_volt` double NOT NULL DEFAULT 0,
        `br_volt` double NOT NULL DEFAULT 0,
        `t_current` double NOT NULL DEFAULT 0,
        `r_current` double NOT NULL DEFAULT 0,
        `y_current` double NOT NULL DEFAULT 0,
        `b_current` double NOT NULL DEFAULT 0,
        `t_watts` double NOT NULL DEFAULT 0,
        `r_watts` double NOT NULL DEFAULT 0,
        `y_watts` double NOT NULL DEFAULT 0,
        `b_watts` double NOT NULL DEFAULT 0,
        `t_var` double NOT NULL DEFAULT 0,
        `r_var` double NOT NULL DEFAULT 0,
        `y_var` double NOT NULL DEFAULT 0,
        `b_var` double NOT NULL DEFAULT 0,
        `t_voltampere` double NOT NULL DEFAULT 0,
        `r_voltampere` double NOT NULL DEFAULT 0,
        `y_voltampere` double NOT NULL DEFAULT 0,
        `b_voltampere` double NOT NULL DEFAULT 0,
        `avg_powerfactor` double NOT NULL DEFAULT 0,
        `r_powerfactor` double NOT NULL DEFAULT 0,
        `y_powerfactor` double NOT NULL DEFAULT 0,
        `b_powerfactor` double NOT NULL DEFAULT 0,
        `powerfactor` double NOT NULL DEFAULT 0,
        `kwh_actual` double NOT NULL DEFAULT 0,
        `kwh` double NOT NULL DEFAULT 0,
        `kvah` double NOT NULL DEFAULT 0,
        `kw` double NOT NULL DEFAULT 0,
        `kvar` double NOT NULL DEFAULT 0,
        `power_factor` double NOT NULL DEFAULT 0,
        `actual_demand` double(18,3) NOT NULL DEFAULT 0.000,
        `demand_dtm` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
        `kva` double NOT NULL DEFAULT 0,
        `frequency` double NOT NULL DEFAULT 0,
        `machine_kwh` double DEFAULT NULL,
        `master_kwh` double DEFAULT NULL,
        `created_on` timestamp NOT NULL DEFAULT current_timestamp(),
        `reverse_machine_kwh` float NOT NULL DEFAULT 0,
        `reverse_master_kwh` float NOT NULL DEFAULT 0,
        `reverse_kwh` float NOT NULL DEFAULT 0,
        `off_time` int(11) NOT NULL DEFAULT 0,
        `idle_time` int(11) NOT NULL DEFAULT 0,
        `on_load_time` int(11) NOT NULL DEFAULT 0,
        `kwh_msb` int(11) NOT NULL DEFAULT 0,
        `kwh_lsb` int(11) NOT NULL DEFAULT 0,
        `mc_state_changed_time` datetime NOT NULL DEFAULT '1900-01-01 00:00:00',
        `current_poll_duration` int(11) NOT NULL DEFAULT 0,
        `shift_off_time` int(11) NOT NULL DEFAULT 0,
        `shift_idle_time` int(11) NOT NULL DEFAULT 0,
        `shift_on_load_time` int(11) NOT NULL DEFAULT 0,
        `current_poll_consumption` double(18,3) NOT NULL DEFAULT 0.000,
        `last_poll_consumption` double(18,3) NOT NULL DEFAULT 0.000,
        `off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `shift_off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `shift_idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `shift_on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `total_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `meter_status_code` int(11) NOT NULL DEFAULT 0,
        `current_equipment_poll_consumption` double(18,3) NOT NULL DEFAULT 0.000,
        `last_poll_equipment_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_shift_off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_shift_idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `equipment_shift_on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `diff_equipment_off_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `diff_equipment_idle_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `diff_equipment_on_load_kwh` double(18,3) NOT NULL DEFAULT 0.000,
        `r_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `y_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `b_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `avg_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `r_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `y_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `b_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `avg_current_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `runhour_msb` double(18,3) NOT NULL DEFAULT 0.000,
        `runhour_lsb` double(18,3) NOT NULL DEFAULT 0.000,
        `runhour` double(18,3) NOT NULL DEFAULT 0.000,
        `ry_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `yb_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `br_volt_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `vll_avg_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `vln_avg_thd` double(18,3) NOT NULL DEFAULT 0.000,
        `meter_status` int(11) NOT NULL DEFAULT 0,
        PRIMARY KEY (`id`),
        KEY `machine` (`meter_id`,`mill_date`,`mill_shift`,`created_on`,`avg_powerfactor`),
        KEY `idx_power_analysis_created_on_meter_id` (`created_on`,`meter_id`),
        KEY `idx_fk_cp_meter_id` (`meter_id`,`mill_date`,`mill_shift`,`created_on`)
        ) 
        """
        cursor.execute(sql)
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", f"Created power_analysis_{act_month_year} table")
        
        sql=f'''CREATE TABLE IF NOT EXISTS ems_v1_completed.{alarm_table}(
                    `id` int(11) NOT NULL AUTO_INCREMENT,
                    `alarm_target_id` int(11) DEFAULT NULL,
                    `meter_id` int(11) DEFAULT NULL,
                    `description` varchar(300) DEFAULT NULL,
                    `parameter_name` varchar(200) DEFAULT NULL,
                    `start_time` datetime DEFAULT NULL,
                    `stop_time` datetime DEFAULT NULL,
                    `duration` varchar(200) DEFAULT NULL,
                    `mill_date` datetime DEFAULT NULL,
                    `mill_shift` int(11) DEFAULT NULL,
                    PRIMARY KEY (`id`)
                    ) ENGINE=InnoDB DEFAULT CHARSET=latin1;
                '''
        cursor.execute(sql)
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", f"Created alarm_{act_month_year} table ")

        sql=f'''CREATE TABLE IF NOT EXISTS ems_v1_completed.{poll_table}(
                `id` int(11) NOT NULL AUTO_INCREMENT,
                `meter_id` int(11) NOT NULL DEFAULT 0,
                `mill_date` datetime DEFAULT NULL,
                `mill_shift` int(11) NOT NULL DEFAULT 0,
                `meter_status` int(11) NOT NULL DEFAULT 0,
                `mc_state_changed_time` datetime DEFAULT NULL,
                `poll_duration` int(11) NOT NULL DEFAULT 0,
                `poll_consumption` double NOT NULL DEFAULT '0',
                `equipment_consumption` double NOT NULL DEFAULT '0',
                `min_amps` double NOT NULL DEFAULT '0',
                `max_amps` double NOT NULL DEFAULT '0',
                `avg_amps` double NOT NULL DEFAULT '0',
                `is_amps` varchar(11) NOT NULL DEFAULT 'no',
                PRIMARY KEY (`id`),
                KEY `idx_mmss` (`meter_id`,`mill_date`,`mill_shift`,`meter_status`),
                KEY `idx_mms` (`mill_date`,`mill_shift`,`meter_status`)
                ) ENGINE=InnoDB DEFAULT CHARSET=latin1'''
        cursor.execute(sql)
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", f"Created {poll_table} table ")
        
        cursor.execute (f"""insert into ems_v1_completed.{power_analysis_table}(company_id ,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,date_time,mill_date,mill_shift,vln_avg,
        r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,
        b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,
        b_powerfactor,powerfactor,kwh_actual,kwh,kvah,kw,kvar,power_factor,kva,actual_demand,demand_dtm,frequency,created_on,machine_kwh,master_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,mc_state_changed_time , off_time , shift_off_time , idle_time , shift_idle_time , on_load_time , shift_on_load_time , current_poll_duration,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh,diff_equipment_off_kwh,diff_equipment_idle_kwh,diff_equipment_on_load_kwh,r_volt_thd,y_volt_thd,b_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,meter_status,runhour_msb,runhour_lsb,runhour,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd) select company_id ,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,date_time,mill_date,mill_shift,vln_avg,
        r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,
        b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,
        b_powerfactor,powerfactor,kwh_actual,kwh,kvah,kw,kvar,power_factor,kva,actual_demand,demand_dtm,frequency,created_on,ifnull(machine_kwh,0), ifnull(master_kwh,0),ifnull(reverse_machine_kwh,0), ifnull(reverse_master_kwh,0) , ifnull(reverse_kwh,0),mc_state_changed_time , off_time , shift_off_time , idle_time , shift_idle_time , on_load_time , shift_on_load_time , current_poll_duration,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh,diff_equipment_off_kwh,diff_equipment_idle_kwh,diff_equipment_on_load_kwh,r_volt_thd,y_volt_thd,b_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,meter_status,runhour_msb,runhour_lsb,runhour,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd from ems_v1.current_power_analysis 
        where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}'""")
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", "power analysis table insert Done !")

        cursor.execute (f"""insert into ems_v1_completed.{power_table}(company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,
            beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,
            br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,
            t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,
            y_powerfactor,b_powerfactor,powerfactor,kwh,kvah,kw,kvar,power_factor,kva,frequency,status,
            created_on,created_by,modified_on,modified_by,machine_kwh,master_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,mc_state_changed_time , off_time , shift_off_time , idle_time , shift_idle_time , on_load_time , shift_on_load_time , current_poll_duration,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,actual_demand,demand_dtm,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh,r_volt_thd,y_volt_thd,b_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,meter_status,runhour_msb,runhour_lsb,runhour,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd) select 
            company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,
            beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,
            br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,
            t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,
            y_powerfactor,b_powerfactor,powerfactor,kwh,kvah,kw,kvar,power_factor,kva,frequency,status,
            created_on,created_by,modified_on,modified_by,machine_kwh,master_kwh,reverse_machine_kwh,reverse_master_kwh,reverse_kwh,
            mc_state_changed_time , off_time , shift_off_time , idle_time , shift_idle_time , on_load_time , shift_on_load_time , current_poll_duration,current_poll_consumption,last_poll_consumption,off_kwh,idle_kwh,on_load_kwh,shift_off_kwh,shift_idle_kwh,shift_on_load_kwh,equipment_kwh,total_kwh,meter_status_code,actual_demand,demand_dtm,current_equipment_poll_consumption,last_poll_equipment_kwh,equipment_off_kwh,equipment_idle_kwh,equipment_on_load_kwh,equipment_shift_off_kwh,equipment_shift_idle_kwh,equipment_shift_on_load_kwh,r_volt_thd,y_volt_thd,b_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,meter_status,runhour_msb,runhour_lsb,runhour,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd  from ems_v1.current_power
            where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' """)
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", "power table insert Done !")
        try:
            
            sql = f'''
                    update ems_v1.current_power set meter_status = 0 , current_equipment_poll_consumption = 0 , last_poll_equipment_kwh = 0 , equipment_off_kwh = 0 , equipment_idle_kwh = 0 , equipment_on_load_kwh = 0 , equipment_shift_off_kwh = 0 , equipment_shift_idle_kwh = 0 , equipment_shift_on_load_kwh = 0 , 
                    equipment_kwh = 0 , total_kwh = 0 , off_kwh = 0 , idle_kwh = 0 , on_load_kwh = 0 , shift_off_kwh = 0 , shift_idle_kwh = 0 , shift_on_load_kwh = 0 , current_poll_consumption = 0 , off_time = 0 , shift_off_time = 0 , idle_time = 0 , shift_idle_time = 0 , on_load_time = 0 , shift_on_load_time = 0 , mc_state_changed_time = IFNULL(STR_TO_DATE(CONCAT(DATE('{shift_date_n}'), ' ', TIME('{shift_start}')), '%Y-%m-%d %H:%i:%s'),0) , current_poll_duration = 0 , current_poll_consumption = 0 , vln_avg = 0 , r_volt = 0 , y_volt = 0 , b_volt = 0 , vll_avg = 0 , ry_volt = 0 , yb_volt = 0 , br_volt = 0 , t_current = 0 , 
                    r_current = 0 , y_current = 0 , b_current = 0 , t_watts = 0 , r_watts = 0 , y_watts = 0 , b_watts = 0 , t_var = 0 , r_var = 0 , y_var = 0 , b_var = 0 , 
                    t_voltampere = 0 , r_voltampere = 0 , y_voltampere = 0 , b_voltampere = 0 , avg_powerfactor = 0 , r_powerfactor = 0 , y_powerfactor = 0 , b_powerfactor = 0 , 
                    powerfactor = 0 , kwh = 0 , kvah = 0 , kw = 0 , kvar = 0 , power_factor = 0 , kva = 0 , frequency = 0 ,r_volt_thd = 0,y_volt_thd = 0,b_volt_thd = 0,avg_volt_thd = 0,r_current_thd = 0,y_current_thd = 0,b_current_thd = 0,avg_current_thd = 0,ry_volt_thd = 0,yb_volt_thd = 0,br_volt_thd = 0,vll_avg_thd = 0,vln_avg_thd = 0
                    where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' 
                    '''
            createFolder(f"{Logfile_name}{plant_id}/", f"All live Parameters reset Query - {sql}")
            cursor.execute(sql)
            db.commit()
            createFolder(f"{Logfile_name}{plant_id}/", "All live Parameters reset Done !")

        except Exception as e:
            createFolder(f"{Logfile_name}{plant_id}/", f"Error Reseting All live Parameters {e}")

        cursor.execute(f"UPDATE ems_v1.current_power SET kwh=0,last_poll_consumption=machine_kwh,master_kwh=machine_kwh,start_kwh=0,end_kwh=0,reverse_master_kwh=reverse_machine_kwh where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' ")
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", "Currrent Power kwh reset and master_kwh set Done !")

        cursor.execute (f"""insert into ems_v1_completed.{poll_table}(meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , poll_duration , poll_consumption , equipment_consumption,min_amps,max_amps,avg_amps,is_amps) 
                        select meter_id , mill_date , mill_shift , meter_status , mc_state_changed_time , poll_duration , poll_consumption , equipment_consumption,min_amps,max_amps,avg_amps,is_amps
                        from ems_v1.current_polling_data""")
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/" , "polling data table insert Done !")
    
        cursor.execute(f"UPDATE ems_v1.master_shifts_exe SET mill_date='{act_date}',mill_shift= {act_shift} WHERE company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' and created_for = 'datalogger' ")
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", "Mill Date and Mill Shift Updated in Master Shifts Exe !")
    
        cursor.execute(f"UPDATE ems_v1.master_shifts SET mill_date='{act_date}',mill_shift= {act_shift} WHERE company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' ")
        db.commit()
        createFolder(f"{Logfile_name}{plant_id}/", "Mill Date and Mill Shift Updated in Master Shifts !")
         
        # try:
            
        #     sql = f'''
        #             update ems_v1.current_power set meter_status = 0 , current_equipment_poll_consumption = 0 , last_poll_equipment_kwh = 0 , equipment_off_kwh = 0 , equipment_idle_kwh = 0 , equipment_on_load_kwh = 0 , equipment_shift_off_kwh = 0 , equipment_shift_idle_kwh = 0 , equipment_shift_on_load_kwh = 0 , 
        #             equipment_kwh = 0 , total_kwh = 0 , off_kwh = 0 , idle_kwh = 0 , on_load_kwh = 0 , shift_off_kwh = 0 , shift_idle_kwh = 0 , shift_on_load_kwh = 0 , current_poll_consumption = 0 , off_time = 0 , shift_off_time = 0 , idle_time = 0 , shift_idle_time = 0 , on_load_time = 0 , shift_on_load_time = 0 , mc_state_changed_time = IFNULL(STR_TO_DATE(CONCAT(DATE('{shift_date_n}'), ' ', TIME('{shift_start}')), '%Y-%m-%d %H:%i:%s'),0) , current_poll_duration = 0 , current_poll_consumption = 0 , vln_avg = 0 , r_volt = 0 , y_volt = 0 , b_volt = 0 , vll_avg = 0 , ry_volt = 0 , yb_volt = 0 , br_volt = 0 , t_current = 0 , 
        #             r_current = 0 , y_current = 0 , b_current = 0 , t_watts = 0 , r_watts = 0 , y_watts = 0 , b_watts = 0 , t_var = 0 , r_var = 0 , y_var = 0 , b_var = 0 , 
        #             t_voltampere = 0 , r_voltampere = 0 , y_voltampere = 0 , b_voltampere = 0 , avg_powerfactor = 0 , r_powerfactor = 0 , y_powerfactor = 0 , b_powerfactor = 0 , 
        #             powerfactor = 0 , kwh = 0 , kvah = 0 , kw = 0 , kvar = 0 , power_factor = 0 , kva = 0 , frequency = 0 ,r_volt_thd = 0,y_volt_thd = 0,b_volt_thd = 0,avg_volt_thd = 0,r_current_thd = 0,y_current_thd = 0,b_current_thd = 0,avg_current_thd = 0,ry_volt_thd = 0,yb_volt_thd = 0,br_volt_thd = 0,vll_avg_thd = 0,vln_avg_thd = 0
        #             where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' 
        #             '''
        #     createFolder(f"{Logfile_name}{plant_id}/", f"All live Parameters reset Query - {sql}")
        #     cursor.execute(sql)
        #     db.commit()
        #     createFolder(f"{Logfile_name}{plant_id}/", "All live Parameters reset Done !")

        # except Exception as e:
        #     createFolder(f"{Logfile_name}{plant_id}/", f"Error Reseting All live Parameters {e}")

        try:
            # cursor.execute(f"UPDATE ems_v1.current_power SET kwh=0,last_poll_consumption=machine_kwh,master_kwh=machine_kwh,start_kwh=0,end_kwh=0,reverse_master_kwh=reverse_machine_kwh where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' ")
            # db.commit()
            # createFolder(f"{Logfile_name}{plant_id}/", "Currrent Power kwh reset and master_kwh set Done !")
            shift_update = f"UPDATE ems_v1.current_power SET mill_date='{act_date}',mill_shift={act_shift} where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' "
            createFolder(f"{Logfile_name}{plant_id}/", f"Mill Date and Mill Shift Updated in Current power{shift_update} !")
            cursor.execute(shift_update)
            db.commit()
            
            cursor.execute (f"Delete from ems_v1.current_power_analysis where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' ")
            db.commit()
            createFolder(f"{Logfile_name}{plant_id}/", "Deleted From Current  power analysis !")

            cursor.execute ("""delete from ems_v1.current_polling_data""")
            db.commit()
            createFolder(f"{Logfile_name}{plant_id}/", "Deleted From current_polling_data !")

            # shift_update = f"UPDATE ems_v1.current_power SET mill_date='{act_date}',mill_shift={act_shift} where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' "
            # createFolder(f"{Logfile_name}{plant_id}/", f"Mill Date and Mill Shift Updated in Current power{shift_update} !")
            # cursor.execute(shift_update)
            # db.commit()
            
            cursor.execute(f'''insert into ems_v1_completed.{alarm_table}(alarm_target_id,meter_id,description,parameter_name,start_time,stop_time,duration,mill_date,mill_shift)
            select alarm_target_id,meter_id,description,parameter_name,start_time,stop_time,duration,mill_date,
            mill_shift from ems_v1.present_alarm where stop_time is not NULL and meter_id in (select meter_id from master_meter where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}') ''')
            db.commit()
            createFolder(f"{Logfile_name}{plant_id}/", "Inserting Alarm to Completed !")

            cursor.execute(f"delete from ems_v1.present_alarm where stop_time is not NULL and meter_id in (select meter_id from master_meter where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}') ")
            db.commit()
            createFolder(f"{Logfile_name}{plant_id}/", "Deleted from Alarm Table ")

        except Exception as e:
            createFolder(f"{Logfile_name}{plant_id}/", f"Error inseting  shift update {e}")
       
        data_c = f'''insert into ems_v1.data_correction(mill_date,mill_shift,plant_id)
                                    values ('{prev_mill_date}','{prev_mill_shift}','{plant_id}')'''
        createFolder(f"{Logfile_name}{plant_id}/", f"data correction insert query-{data_c}")
        cursor.execute(data_c)
        db.commit()
        
        try:
            cursor.execute(f"update ems_v1.database_backup set db_backup_flag = 1")
            db.commit()
        except Exception as e:
            createFolder(f"{Logfile_name}{plant_id}/", f"Error inseting database_backup {e}")
       
        cursor.execute(f"SELECT is_fapi_call from ems_v1.master_plant where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}'")
        plant=cursor.fetchall()
        for row in plant:
            is_fapi_call = row["is_fapi_call"]
    
        if is_fapi_call =='yes':
            try:
                cursor.execute(f"SELECT equipment_id,meter_id,meter,is_poll_meter from ems_v1.master_meter where company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}' and meter = 'equipment' and is_poll_meter = 'yes'")
                row_eqp=cursor.fetchall()
                if len(row_eqp)>0:
                    for row_e in row_eqp:
                        equipment_id= row_e['equipment_id']
                        meter_id= row_e['meter_id']

                        
                        actual_ton = f'''insert into ems_v1.equipment_wise_production_data(equipment_id,meter_id,mill_date,mill_shift)
                                            values ('{equipment_id}','{meter_id}','{prev_mill_date}','{prev_mill_shift}')'''
                        createFolder(f"{Logfile_name}{plant_id}/", f"Equipment wise production data insert query-{actual_ton}")
                        cursor.execute(actual_ton)
                        db.commit()

                        
                    createFolder(f"{Logfile_name}{plant_id}/", f"Equipment wise production data insert Done !!")
                else:
                    createFolder(f"{Logfile_name}{plant_id}/", f"No data available in this plant !!")
            except Exception as e:
                createFolder(f"{Logfile_name}{plant_id}/", f"Error inseting equipment wise production data {e}")
        else :
            createFolder(f"{Logfile_name}{plant_id}/", f"Fastapi Call - {is_fapi_call}")
            
        try:
            # Creating database if not exists
            cursor.execute("CREATE DATABASE IF NOT EXISTS gateway_completed")
            db.commit()
            createFolder(f"{Logfile_name}{plant_id}/", "Creating database if not exists gateway_completed")
            
            # Creating table if not exists
            sql = f'''
                    CREATE TABLE IF NOT EXISTS gateway_completed.log_{act_month_year} (
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
            createFolder(f"{Logfile_name}{plant_id}/", f"Creating table if not exists : {sql}")
            cursor.execute(sql)
            db.commit()
            
            # # Inserting data from current table to completed
            # sql = f'''
            #     INSERT INTO gateway_completed.log_{act_month_year}(id,imei_no,mac,signal_strength,received_packet,date_time,msg_type,production_quantity,created_on,STATUS)
            #     SELECT id,imei_no,mac,signal_strength,received_packet,date_time,msg_type,production_quantity,created_on,STATUS
            #     FROM gateway.log WHERE mac IN (SELECT mac FROM ems_v1.master_meter WHERE company_id = '{company_id}' and bu_id = '{bu_id}' and plant_id = '{plant_id}') AND STATUS = 'yes' 
            #     '''
            # createFolder(f"{Logfile_name}{plant_id}/", f"Inserting data into completed gateway : {sql}")
            # cursor.execute(sql)
            # db.commit()
            
            createFolder(f"{Logfile_name}{plant_id}/", f"Completed Moving all data from current data !")

        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder(f"{Logfile_name}{plant_id}/", f"Error In Moving Gateway data -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")
        
        createFolder(f"{Logfile_name}{plant_id}/", " Shift ended and exiting")

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder(f"{Logfile_name}{plant_id}/", f"Error In While Loop -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")

while True:

    try:

        # Database connectivity
        db = pymysql.connect(host="localhost", user="AIC_PY_SE",passwd="88f63b33874910309091e5aebcd06ead", db="ems_v1" , port= 3308)
        cursor = db.cursor(pymysql.cursors.DictCursor)

        sql = "SELECT company_id,bu_id,plant_id FROM master_shifts_exe WHERE status = 'active' and created_for = 'datalogger' "
        cursor.execute(sql)
        plant_details = cursor.fetchall()
        if len(plant_details) > 0 :

            for detail in plant_details:

                company_id = detail['company_id']
                bu_id = detail['bu_id']
                plant_id = detail['plant_id']

                shift_check(company_id,bu_id,plant_id,cursor)
                if shift_end_flag == 1:
                    createFolder(f"{Logfile_name}{plant_id}/", f"Shift Changed -->> Date :{act_date} , shift : {act_shift} .")
                    shift_end(company_id,bu_id,plant_id,db,cursor)
                else:
                    createFolder(f"{Logfile_name}{plant_id}/", "Shift end Flag not raised !")

        else:
            createFolder(f"{Logfile_name}main_loop_error/","No Plant details found !!")

        time.sleep(10)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        createFolder(f"{Logfile_name}main_loop_error/", f"Error In While Loop -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")