from fastapi import APIRouter,HTTPException
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.check_table import check_power_table,check_power_12_table,check_analysis_table
import os
from src.models.image import save_image,id
from src.models.parse_date import parse_date
from datetime import datetime,timedelta, date
import asyncio
import re
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.load_analysis_model import load_analysis_mdl,polling_analysis
        from src.models.mysql.report_model import get_equipment_cal_dtl
        from src.models.mysql.master_shift_model import shift_Lists
        from src.models.check_table import check_power_table,check_power_12_table,check_analysis_table,check_polling_data_tble,check_alarm_tble,check_user_count


    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.load_analysis_model import load_analysis_mdl
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")


router = APIRouter()
mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}

@router.post("/load_analysis/", tags=["Analysis"])
async def load_analysis(company_id : int = Form(""),
                        bu_id: int = Form(''),
                        plant_id: int = Form(''),
                        period_id: str = Form(''),
                        group_by: str = Form(''),
                        meter_id: str=Form(''), 
                        equipment_id: str=Form(''), 
                        from_date: str = Form(''),
                        to_date: str = Form(''),
                        shift_id :int = Form(''),
                        from_time: str=Form(''), 
                        to_time: str=Form(''),  
                        duration :int = Form(''),                      
                        meter_status :int = Form(''),                      
                        cnx: AsyncSession = Depends(get_db)):

    try: 
        if period_id == "" :
            return _getErrorResponseJson("Period Id Is Required...")
        
        if plant_id == '':
            return _getErrorResponseJson("Plant Id Is Required...")
        
        mill_date = date.today()
        mill_shift = 0
        no_of_shifts = 3
        
        data1 = await shift_Lists(cnx, '',plant_id, bu_id, company_id)
       
        if len(data1) > 0:
            for shift_record in data1:
                mill_date = shift_record["mill_date"]
                mill_shift = shift_record["mill_shift"]  
                no_of_shifts = shift_record["no_of_shifts"]

        if period_id == 'cur_shift' or period_id == '#cur_shift':
            from_date = mill_date
            shift_id = mill_shift
        
        elif period_id == 'sel_shift' or period_id == 'sel_date':

            if from_date == '':
                return _getErrorResponseJson("From Date Is Required") 
            if period_id == 'sel_shift':
                if shift_id == '':
                    return _getErrorResponseJson("Shift Id Is Required") 
                
            from_date =  await parse_date(from_date)   
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}""" 

            res = await check_analysis_table(cnx,month_year)
            if len(res) == 0:
                return _getErrorResponseJson("Analysis Table Not Available...") 
            
        elif period_id == "#previous_shift" or period_id == "#previous_day":  
                if period_id == "#previous_shift":               
                    if int(mill_shift) == 1:
                        shift_id = no_of_shifts
                        from_date = mill_date - timedelta(days=1)
                    else:
                        shift_id = int(mill_shift) - 1
                        from_date = mill_date 

                elif period_id == "#previous_day":             
                    from_date = mill_date - timedelta(days=1)
                
                month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                res = await check_analysis_table(cnx,month_year)
                if len(res) == 0:
                    return _getErrorResponseJson("Analysis Table Not Available...")   
                       
        elif period_id == "from_to" or period_id == "#from_to":            
            if from_date == '':
                return _getErrorResponseJson("From Date Is Required")
            if to_date == '':
                return _getErrorResponseJson("To Date Is Required")  
            from_date = await parse_date(from_date)
            to_date = await parse_date(to_date)

        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or period_id == "#sel_year" or period_id=="#from_to" :
                if period_id  == "#this_week":
                    dt = mill_date
                    from_date=dt-timedelta(dt.weekday()+1)
                    to_date = mill_date

                elif period_id == "#previous_week":
                    dt = mill_date
                    current_week_start = dt - timedelta(days=dt.weekday())  
                    from_date = current_week_start - timedelta(weeks=1)  
                    to_date = from_date + timedelta(days=5)

                elif period_id == "#this_month":
                    from_date = mill_date.replace(day=1)
                    to_date = mill_date

                elif period_id == "#previous_month":
                    from_date = mill_date.replace(day=1)                   
                    from_date = (from_date - timedelta(days=1)).replace(day=1)
                    to_date = from_date + timedelta(days=30)   

                elif period_id=="#this_year": 
            
                    from_date = mill_date.replace(day=1,month=1) 
                    to_date = mill_date  
                    

                elif period_id=="#previous_year": 
                    from_date = mill_date.replace(day=1, month=1, year=mill_date.year - 1)
                    to_date = from_date.replace(day=1, month=12) + timedelta(days=30)
                   
            
                if from_date != '' and to_date != '':

                    month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                    union_queries = []
                    joins = []

                    for month_year in month_year_range:
                        res = await check_analysis_table(cnx,month_year)
            
                        if len(res)>0:
                            table_name = f"ems_v1_completed.power_{month_year}"
                            union_queries.append(f"{table_name}")

                    if len(union_queries) == 0:
                        return _getErrorResponseJson("Analysis Table Not Available...")     
            
        if group_by == 'equipment' and equipment_id == "" :
            return _getErrorResponseJson("Equipment Id Is Required...")  
        
        data = await load_analysis_mdl(cnx,company_id,bu_id,plant_id,period_id,group_by,equipment_id,meter_id,from_date,to_date,shift_id,from_time,to_time,duration,meter_status)
        
        label = {}
        meter_data = {}
        org_data = []
        for d in data:
            meter_id = d['meter_id']
            meter_name = d['meter_name']
            if meter_id not in label:        
                label[meter_id] = meter_name
            if meter_id not in meter_data:
                meter_data[meter_id] = []

            # set meter_data for meter_id
            temp = {
                'date_time': d['date_time'],
                't_current': d['t_current'],
                'r_current': d['r_current'],
                'y_current': d['y_current'],
                'b_current': d['b_current'],
                'vll_avg': d['vll_avg'],
                'ry_volt': d['ry_volt'],
                'yb_volt': d['yb_volt'],
                'br_volt': d['br_volt'],
                'vln_avg': d['vln_avg'],
                'r_volt': d['r_volt'],
                'y_volt': d['y_volt'],
                'b_volt': d['b_volt'],
                't_watts': d['t_watts'],
                'kWh': d['kWh'],
                'kvah': d['kvah'],
                'kw': d['kw'],
                'kvar': d['kvar'],
                'power_factor': d['power_factor'],
                'r_watts': d['r_watts'],
                'kva': d['kva'],
                'y_watts': d['y_watts'],
                'b_watts': d['b_watts'],
                'avg_powerfactor': d['avg_powerfactor'],
                'r_powerfactor': d['r_powerfactor'],
                'y_powerfactor': d['y_powerfactor'],
                'b_powerfactor': d['b_powerfactor'],
                'powerfactor': d['powerfactor'],
                'frequency': d['frequency'],
                't_voltampere': d['t_voltampere'],
                'r_voltampere': d['r_voltampere'],
                'y_voltampere': d['y_voltampere'],
                'b_voltampere': d['b_voltampere'],
                't_var': d['t_var'],
                'r_var': d['r_var'],
                'y_var': d['y_var'],
                'b_var': d['b_var'],
                'runhour': d['runhour'],
                'r_volt_thd': d['r_volt_thd'],
                'y_volt_thd': d['y_volt_thd'],
                'b_volt_thd': d['b_volt_thd'],
                'avg_volt_thd': d['avg_volt_thd'],
                'r_current_thd': d['r_current_thd'],
                'y_current_thd': d['y_current_thd'],
                'b_current_thd': d['b_current_thd'],
                'avg_current_thd': d['avg_current_thd'],
                'master_kwh':d['master_kwh'],
                'machine_kWh':d['machine_kWh'],
                'on_load_kwh':d['on_load_kwh'],
                'off_kwh':d['off_kwh'],
                'idle_kwh':d['idle_kwh']
            }

            meter_data[meter_id].append(temp)

        for key, value in meter_data.items():
            org_data.append({'label': label[key], 'data': value})

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": org_data,
            "data1": data
        }
        
        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_polling_time_analysis/", tags=["Analysis"])
async def Polling_time_analysis(period_id :str = Form(''),
                                plant_id :str = Form(''),
                                equipment_id: str = Form(''),
                                meter_status:int = Form(''),                    
                                from_date:str = Form(''),                    
                                to_date:str = Form(''),                    
                                cnx: AsyncSession = Depends(get_db)):
    try: 

        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
        
        if period_id == "":
            return _getErrorResponseJson("Period Id Is Required")
        
        if equipment_id == "":
            return _getErrorResponseJson("Equipment Id Is Required")
        
        if meter_status == "":
            return _getErrorResponseJson("Meter Status Is Required")
        
        if plant_id == "":
            return _getErrorResponseJson("Plant Id Is Required")
        
        mill_shift = 0
        no_of_shifts = 3
        mill_date = date.today()
        min_amps = 0
        max_amps = 0
        avg_amps = 0
        data1 = await shift_Lists(cnx, '',plant_id, '', '')
        # query = text(f'''SELECT * FROM master_shifts WHERE status = 'active' and  plant_id = '{plant_id}' ''')
        
        if len(data1) > 0:
            for shift_record in data1:
                mill_date = shift_record["mill_date"]
                mill_shift = shift_record["mill_shift"]  
                no_of_shifts = shift_record["no_of_shifts"] 

        if period_id not in ["#cur_shift","cur_shift","#previous_shift","#previous_day","#from_to","#this_week","#previous_week","#this_year","previous_year","#this_month","#previous_month"]:
            return _getErrorResponseJson("Invalid Period")
        
        if period_id == "#cur_shift" or period_id == "cur_shift":
            from_date = mill_date 
                
        if period_id == "#previous_shift" or period_id == "#previous_day":  
            if period_id == "#previous_shift":               
                if int(mill_shift) == 1:
                    mill_shift = no_of_shifts
                    from_date = mill_date - timedelta(days=1)
                else:
                    mill_shift = int(mill_shift) - 1
                    from_date = mill_date 

            if period_id == "#previous_day":             
                from_date = mill_date - timedelta(days=1)
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            res = await check_power_table(cnx,month_year)
            if len(res) == 0:
                return _getErrorResponseJson("Power Table Not Available...")   
                    
        if period_id == "from_to" or period_id == "#from_to":           
            if from_date == '':
                return _getErrorResponseJson("From Date Is Required")
            if to_date == '':
                return _getErrorResponseJson("To Date Is Required")  
            
            from_date = await parse_date(from_date)
            to_date = await parse_date(to_date)
            
        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or period_id == "#sel_year":
            if period_id  == "#this_week":
                dt = mill_date
                from_date=dt-timedelta(dt.weekday()+1)
                to_date = mill_date

            if period_id == "#previous_week":
                dt = mill_date
                current_week_start = dt - timedelta(days=dt.weekday())  
                from_date = current_week_start - timedelta(weeks=1)  
                to_date = from_date + timedelta(days=5)

            if period_id == "#this_month":
                from_date = mill_date.replace(day=1)
                to_date = mill_date

            if period_id == "#previous_month":
                from_date = mill_date.replace(day=1)                   
                from_date = (from_date - timedelta(days=1)).replace(day=1)
                to_date = from_date + timedelta(days=30)   

            if period_id=="#this_year": 
        
                from_date = mill_date.replace(day=1,month=1) 
                to_date = mill_date  
                

            if period_id=="#previous_year": 
                from_date = mill_date.replace(day=1, month=1, year=mill_date.year - 1)
                to_date = from_date.replace(day=1, month=12) + timedelta(days=30)
        
            if from_date != '' and to_date != '':

                month_year_range = [
                    (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                    for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                ]
                joins = []

                for month_year in month_year_range:
                    res1 = await check_polling_data_tble(cnx,month_year)

                    if len(res1) >0:
                        join_p = f"left join ems_v1_completed.dbo.polling_data_{month_year} cpd on cpd.meter_id = mm.meter_id and cpd.mill_date = cp.mill_date and cpd.mill_shift = cp.mill_shift"
                        joins.append(f"select machine_status,poll_duration, mill_date, mill_shift, meter_id from {join_p}")
                
                if len(joins) == []:
                    return _getErrorResponseJson("Polling Data Table Not Available...")         
                          
        data = await polling_analysis(cnx,period_id ,plant_id, equipment_id,from_date ,to_date ,mill_shift,meter_status)             
        
        result_data = []
        for entry in data:
            
            mc_state_changed_time = entry["mc_state_changed_time"]
            duration_end = mc_state_changed_time + timedelta(seconds=entry["poll_duration"])
            duration = f"{mc_state_changed_time} to {duration_end}"
    
                    
            result_entry = {
                'equipment_id': entry["equipment_id"],
                'meter_id': entry["meter_id"],
                'equipment_name': entry["equipment_name"],
                'equipment_code': entry["equipment_code"],
                'meter_status': entry["meter_status"],
                'mill_date': entry["mill_date"],
                'mill_shift': entry["mill_shift"],
                'mc_state_changed_time': entry["mc_state_changed_time"],
                'poll_duration': entry["poll_duration"],
                'min_amps': entry["min_amps"],
                'max_amps': entry["max_amps"],
                'avg_amps': entry["avg_amps"],
                'duration': duration,
                'kwh': entry["equipment_consumption"],
                'time_duration':entry["time_duration"]

            }
            
            result_data.append(result_entry)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result_data
        }
              
        return response
    except Exception as e:
        return get_exception_response(e)
