from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from src.models.parse_date import parse_date
from src.models.image import id
from datetime import datetime,date, timedelta
from pathlib import Path
from datetime import datetime
from log_file import createFolder
import re
from src.models.mysql.master_shift_model import shift_Lists
from collections import defaultdict

import  requests
from src.models.check_table import check_power_table,check_power_12_table,check_analysis_table,check_polling_data_tble,check_alarm_tble


static_dir = Path(__file__).parent 
base_path = Path(__file__).parent / "attachment"
mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}

async def current_power_dtl(cnx,company_id ,campus_id,bu_id ,plant_id ,plant_department_id ,equipment_group_id ,equipment_id,function_id ,meter_id ,group_for ,groupby ,period_id,from_date,to_date,shift_id,limit_report_for,limit_exception_for,limit_order_by ,limit_operation_value ,is_critical ,converter_id ,report_for,is_function , function_type ,reportfor,employee_id ,is_minmax,is_main_meter,is_demand,meter_type,is_plant_wise):    
    
    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
    completed_db="ems_v1_completed."           
    where = "" 

    where_p = "" 
    group_by = ""
    order_by = ""  
    function_where = ''
    group_by_poll = ''
    machine = ''
    result_p = ''
    poll_time = ''
    minmax_kwh = ''
    day = 1

    if  employee_id != '':
        query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
        res = cnx.execute(query).fetchall()
        if len(res)>0:
            for row in res:
                plant_id = row["plant_id"]
                plant_department_id = row["plant_department_id"]
                equipment_group_id = row["equipment_group_id"]
                
    if  company_id == '' or company_id == "0" or company_id == 'all':
        pass
    else:
        
        where += f" and  mm.company_id in ({company_id})" 
        where_p += f" and  mm.company_id in ({company_id})" 

    if  campus_id == '' or campus_id == "0" or campus_id == 'all':
        pass
    else:
        where += f" and  md.campus_id in ({campus_id})" 
        where_p += f" and  md.campus_id in ({campus_id})" 

    if  bu_id == '' or bu_id == "0" or bu_id == 'all':
        pass
    else:
        where += f" and  mm.bu_id in ({bu_id})" 
        where_p += f" and  mm.bu_id in ({bu_id})" 

    if plant_id == '' or plant_id == "0" or plant_id == 'all' or is_demand == 'yes':
        pass
    else:
        # plant_id = await id(plant_id)
        where += f" and  mm.plant_id in ({plant_id})"          
        where_p += f" and  mm.plant_id in ({plant_id})"          
        
    if plant_department_id == '' or plant_department_id == "0" or plant_department_id == 'all':
        pass
    else:
        
        where += f" and mm.plant_department_id in ({plant_department_id})"
        where_p += f" and mm.plant_department_id in ({plant_department_id})"
        
    if equipment_group_id == '' or equipment_group_id == 0 or equipment_group_id == 'all':
        pass
    else:
        equipment_group_id = await id(equipment_group_id)
        where += f" and mm.equipment_group_id in ({equipment_group_id})"
        where_p += f" and mm.equipment_group_id in ({equipment_group_id})"
        
    if equipment_id == '' or equipment_id == 0 or equipment_id == 'all':
        pass
    else:
        equipment_id = await id(equipment_id)

        where += f" and me.equipment_id in ({equipment_id})"
        where_p += f" and me.equipment_id in ({equipment_id})"
        
    if function_id == '' or function_id == 'all':
        pass
    else:
        function_id = await id(function_id)
        if function_type == 'function_2':
            where += f"and  mm.function2_id in ({function_id})"
            where_p += f"and  mm.function2_id in ({function_id})"
        else:
            where += f"and  mm.function_id in ({function_id})"
            where_p += f"and  mm.function_id in ({function_id})"

    if meter_id == '' or meter_id == 'all':
        pass
    else:
        meter_id = await id(meter_id)
        where += f" and mm.meter_id in ({meter_id})"
        where_p += f" and mm.meter_id in ({meter_id})"
        
    if converter_id == '' or converter_id == 'all':
        pass
    else:
        where += f" and mm.converter_id = {converter_id}"
        where_p += f" and mm.converter_id = {converter_id}"
    
    if function_type !='':
        where += f" and mf.function_type = '{function_type}'"
        where_p += f" and mf.function_type = '{function_type}'"

        if function_type == 'function_2':
            function_where += f" mm.function2_id = mf.function_id"  
        else:
            function_where += f" mm.function_id = mf.function_id"
    else:
        function_where = f" mm.function_id = mf.function_id"

    if meter_type != '' and meter_type != None and meter_type != 'all':
        where +=f" and mm.meter_type = '{meter_type}'"

    if groupby == 'equipment' or groupby == 'equipment_group':
        equipment = f'''
                        inner join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id
                        left join master_equipment_calculations mec on mec.equipment_id = mm.equipment_id AND mec.meter_communication = 'equipment' and mec.status = 'active'
                        '''
        formula = " ifnull(mec.formula1,'') as meter_formula,"
  
    else:
        equipment = f''' 
                        left join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id'''
        formula = " '' as meter_formula,"
        
    # if groupby == 'equipment' or groupby == 'equipment_group':
    #     equipment = f'''inner join ems_v1.master_equipment_meter mem on mem.meter_id = mm.meter_id 
    #                     inner join ems_v1.master_equipment me on me.equipment_id = mem.equipment_id'''
  
    # elif group == '':
    #     equipment = f'''left join ems_v1.master_equipment_meter mem on mem.meter_id = mm.meter_id 
    #                     left join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id'''
    # else:
    #     equipment = f'''left join ems_v1.master_equipment_meter mem on mem.meter_id = mm.meter_id 
    #                     left join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id'''
        
    mill_date = date.today()
    mill_shift = 0
    no_of_shifts = 3

    group_id = ""
    group_code = ""
    group_name = ""
    tablename = ''
    table_name = ''

    current_shift = ''
    poll_duration = ''
    join = ''

    if company_id != '' and company_id != '0' and bu_id != '' and bu_id != '0' and plant_id != '' and plant_id != '0':
        data1 = await shift_Lists(cnx, '',plant_id, bu_id, company_id)        
        if len(data1) > 0:
            for shift_record in data1:
                mill_date = shift_record["mill_date"]
                mill_shift = shift_record["mill_shift"]  
                no_of_shifts = shift_record["no_of_shifts"]  
 
    if reportfor == '12to12':
        if period_id == "sel_date":            
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year}_12 as cp " 

            query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12'"""
            result_query = cnx.execute(query).fetchall()

            if len(result_query)==0:
                return _getErrorResponseJson("12to12 table not available...") 

            where += f''' and cp.mill_date = '{from_date}' '''

        elif period_id == "from_to":            
               
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

        
            if from_date.month == to_date.month:
                table_name=f"  {completed_db}power_{month_year}_12 as cp "
            else:
                field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kWh,reverse_master_kwh,reverse_kwh,meter_status_code,actual_ton'
                
                month_year_range = [
                    (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                    for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                ]
                union_queries = []
                for month_year in month_year_range:
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12'"""
                    result_query = cnx.execute(query).fetchall()
                    
                    if len(result_query) > 0:
                        table_name = f"ems_v1_completed.power_{month_year}_12"
                        union_queries.append(f"SELECT {field_name} FROM {table_name}")


                subquery_union = " UNION ALL ".join(union_queries)
                table_name = f"( {subquery_union}) cp"
    else:
        
        if period_id == "cur_shift":               
            table_name = "ems_v1.current_power cp"  
            tablename = "ems_v1.current_power "  
            # where += f''' and cp.mill_date = '{mill_date}' AND cp.mill_shift = '{mill_shift}' '''      
            current_shift += f""" Inner JOIN master_shifts ms 
                            ON
                                ms.company_id=mm.company_id AND 
                                ms.bu_id=mm.bu_id AND 
                                ms.plant_id=mm.plant_id AND 
                                ms.status='active' AND 
                                ms.mill_date=cp.mill_date AND 
                                  ms.mill_shift=cp.mill_shift """

        elif period_id == "#cur_shift":
            where += f''' and cp.mill_date = '{mill_date}' AND cp.mill_shift = '{mill_shift}' '''              
            table_name = "ems_v1.current_power cp" 
            
        elif period_id == "sel_shift":                  
                   
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year} as cp" 
            
            
            where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' AND cpd.mill_shift = '{shift_id}' '''
            result_p=await check_polling_data_tble(cnx,month_year)
             

        elif period_id == "#previous_shift":   
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year} as cp" 
           
        
            where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''   
            where_p += f''' and cpd.mill_date = '{from_date}' AND cpd.mill_shift = '{shift_id}' '''   
            result_p = await check_polling_data_tble(cnx,month_year)

        elif period_id == "sel_date":            
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""

            table_name=f"  {completed_db}power_{month_year} as cp "           
                     
            where += f''' and cp.mill_date = '{from_date}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' '''

            # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name ='polling_data_{month_year}'"""
            result_p = await check_polling_data_tble(cnx,month_year)

            # if len(result_p) > 0:   
                # if report_for == 'detail' or report_for == '':
                #         where_poll = ' and cpd.mill_date = cp.mill_date and cpd.mill_shift = cp.mill_shift'
                #         group_by_poll = " ,cpd.mill_date , cpd.mill_shift"
                    
                # if report_for == 'summary':
                #     where_poll = ' and cpd.mill_date = cp.mill_date'
                #     group_by_poll = " ,cpd.mill_date"

                # if groupby == 'meter':
                #     join = f'''left join (select 
                #                 cpd.meter_id,
                #                 min(cpd.mill_date) mill_date,
                #                 min(cpd.mill_shift) mill_shift,
                #                 SUM(CASE WHEN cpd.meter_status = 0 THEN cpd.poll_duration ELSE 0 END) AS off_time,
                #                 SUM(CASE WHEN cpd.meter_status = 1 THEN cpd.poll_duration ELSE 0 END) AS idle_time,
                #                 SUM(CASE WHEN cpd.meter_status = 2 THEN cpd.poll_duration ELSE 0 END) AS on_load_time
                #             from
                #                 ems_v1_completed.polling_data_{month_year} cpd
                #             where cpd.mill_date = '{from_date}' {machine} 
                #             group by cpd.meter_id {group_by_poll}) as cpd 
                #             on cpd.meter_id = cp.meter_id {where_poll}'''
                

        elif period_id == "#sel_date":

            from_date = mill_date
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year} as cp "
           
            where += f''' and cp.mill_date = '{from_date}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' '''

            # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
            result_query = await check_power_table(cnx,month_year)

            if len(result_query) >0 :
                field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kWh,reverse_master_kwh,reverse_kwh,meter_status_code,equipment_kwh,actual_demand,demand_dtm,actual_ton,runhour,r_volt_thd,b_volt_thd,y_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd'
                table_name = f'''(select {field_name} from ems_v1.current_power union all select {field_name} from ems_v1_completed.power_{month_year}) as cp'''
       
        elif period_id == "#previous_day":             
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            result_p = await check_polling_data_tble(cnx,month_year)
            table_name=f"  {completed_db}power_{month_year} as cp "
            
            where += f''' and cp.mill_date = '{from_date}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' '''
            
        elif period_id  == "#this_week":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
       
        elif period_id == "#previous_week":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id == "#this_month":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
            day = to_date.day
                
        elif period_id == "#previous_month":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
            
        elif period_id=="#this_year": 
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id=="#previous_year": 
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id=="#sel_year": 
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id == "from_to" or period_id == '#from_to':            
    
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''

            
        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or  period_id=="#from_to" or period_id == "#sel_year":
            if from_date != '' and to_date != '':
                if from_date.month == to_date.month and from_date.year == to_date.year:
                    month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                    # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                    result_query = await check_power_table(cnx,month_year)
                  
                    if len(result_query) == 0:
                        return _getErrorResponseJson("power table not available...")    
                
                    table_name=f"  {completed_db}power_{month_year} as cp "
                
                    
                    # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name ='polling_data_{month_year}'"""
                    result_p = await check_polling_data_tble(cnx,month_year)
                            
                else:

                    field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kWh,reverse_master_kwh,reverse_kwh,meter_status_code,equipment_kwh,actual_demand,demand_dtm,actual_ton,runhour,r_volt_thd,b_volt_thd,y_volt_thd,avg_volt_thd,r_current_thd,y_current_thd,b_current_thd,avg_current_thd,ry_volt_thd,yb_volt_thd,br_volt_thd,vll_avg_thd,vln_avg_thd'
                    field_name1 = 'mill_date,mill_shift,kWh'
            
                    month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                    
                    union_queries = []
                    union_querie = []
                    joins = []

                    for month_year in month_year_range:
                        # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                        result_query = await check_power_table(cnx,month_year)

                        if len(result_query) > 0:
                            table_name = f"ems_v1_completed.power_{month_year}"
                            union_queries.append(f"SELECT {field_name} FROM {table_name}")
                            union_querie.append(f"SELECT {field_name1} FROM {table_name}")

                        # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name ='polling_data_{month_year}'"""
                        result_p = await check_polling_data_tble(cnx,month_year)

                        if len(result_p) > 0:
                            join_p = f"ems_v1_completed.polling_data_{month_year} "
                            joins.append(f"select meter_status,poll_duration,poll_consumption,equipment_consumption, mill_date, mill_shift, meter_id from {join_p}")

                    subquery_union = " UNION ALL ".join(union_queries)
                    subquery = " UNION ALL ".join(union_querie)
                    table_name = f"( {subquery_union}) cp"
                    tablename = f"( {subquery}) "

                        
    order_by_limit = ''
    on_min = ''
    on_max = ''
    if limit_report_for == "exception" :
        # if limit_exception_for == "kwh":
        #     order_by += "sum(cp.kwh)"
        order_by_limit += f"cp.{limit_exception_for}"    
        if limit_order_by == "asc":
            order_by_limit += " "+limit_order_by +","
        else:
            order_by_limit += " "+limit_order_by +","
    
    group_by_poll = ''

    if groupby !='' and groupby == "company":
        group_by += " mm.company_id "
        group_by_poll += " mm.company_id "
        order_by += " mm.company_id "
        group_id = '''mc.company_id AS group_id ,'''
        group_code = '''mc.company_code AS group_code ,'''
        group_name = '''mc.company_name AS group_name''' 
        
    elif groupby !='' and groupby == "bu":
        group_by += " mm.bu_id "
        group_by_poll += " mm.bu_id "
        order_by += " mm.bu_id "
        group_id = '''mb.bu_id AS group_id ,'''
        group_code = '''mb.bu_code AS group_code ,'''
        group_name = '''mb.bu_name AS group_name'''       
        
    elif groupby!='' and groupby == "plant" and is_demand != 'yes':
        group_by += " mm.plant_id "
        group_by_poll += " mm.plant_id "
        order_by += " md.plant_order "
        group_id = '''md.plant_id AS group_id, '''
        group_code = '''md.plant_code AS group_code ,'''
        group_name = '''md.plant_name AS group_name'''  
        if is_minmax =='yes':
            on_min = f"min_subquery.plant_id = mm.plant_id"
            on_max = f"max_subquery.plant_id = mm.plant_id"

    elif groupby!='' and groupby == "campus" :
        group_by += " md.campus_id "
        group_by_poll += " md.campus_id "
        order_by += " md.campus_id "
        group_id = '''md.campus_id AS group_id, '''
        group_code = f"''AS group_code,"
        group_name = '''mcs.campus_name AS group_name'''        
        
    elif groupby !='' and groupby == "plant_department":
        group_by += "  mm.plant_department_id "
        group_by_poll += "  mm.plant_department_id "
        order_by += "  mpd.plant_department_order "
        group_id = ''' mpd.plant_department_id AS group_id ,'''
        group_code = ''' mpd.plant_department_code AS group_code ,'''
        group_name = ''' mpd.plant_department_name AS group_name'''
        if is_minmax =='yes':
            on_min = f"min_subquery.plant_department_id = mm.plant_department_id"
            on_max = f"max_subquery.plant_department_id = mm.plant_department_id"
        
    elif groupby !='' and groupby == "equipment_group":
        if is_plant_wise  == 'yes':
            group_by += " md.plant_id,"
            order_by += "  md.plant_order,"
        group_by += " me.equipment_group_id"
        group_by_poll += " me.equipment_group_id"
        order_by += " mmt.equipment_group_order"
        group_id = '''mmt.equipment_group_id AS group_id ,'''
        group_code = '''mmt.equipment_group_code AS group_code ,'''
        group_name = '''mmt.equipment_group_name AS group_name'''
        if is_minmax =='yes':
            on_min = f"min_subquery.equipment_group_id = mm.equipment_group_id"
            on_max = f"max_subquery.equipment_group_id = mm.equipment_group_id"
        
    elif groupby !='' and groupby == "equipment":
        if is_plant_wise  == 'yes':
            group_by += " md.plant_id,"
            order_by += "  md.plant_order,"

        group_by += " me.equipment_id"
        group_by_poll += " me.equipment_id"
        order_by += " me.equipment_order"
        group_id = '''me.equipment_id AS group_id ,'''
        group_code = '''me.equipment_code AS group_code ,'''
        group_name = '''me.equipment_name AS group_name'''
        if is_minmax =='yes':
            on_min = f"min_subquery.equipment_id = mm.equipment_id"
            on_max = f"max_subquery.equipment_id = mm.equipment_id"
        
    elif groupby !='' and groupby == "function":           
        order_by += " mf.function_order"
        group_id = '''mf.function_id AS group_id ,'''
        group_code = '''mf.function_code AS group_code ,'''
        group_name = '''mf.function_name AS group_name'''

        if function_type == 'function_2':
            group_by += " mm.function2_id" 
        else:
            group_by += " mm.function_id"     

        if is_function !="":
            group_by += " ,mm.meter_id"
            order_by += " ,mm.meter_id"
        
    elif groupby !='' and groupby == "meter":             
        group_by += " mm.meter_id"
        group_by_poll += " mm.meter_id"
        order_by += " mm.meter_order"
        group_id = '''mm.meter_id AS group_id ,'''
        group_code = '''mm.meter_code AS group_code ,'''
        group_name = '''mm.meter_name AS group_name''' 
        if is_minmax =='yes':
            on_min = f"min_subquery.meter_id = mm.meter_id"
            on_max = f"max_subquery.meter_id = mm.meter_id"
         
       
    if limit_operation_value !='' and limit_operation_value != '0':           
        order_by += ' LIMIT '+str(limit_operation_value) 
    if is_critical == "yes" or is_critical == "no"  :
        where += f" and mm.major_nonmajor = '{is_critical}' "  

    data2 = ''   
    where_group_for = ""  
    
    # if group_for == "exception" and meter_id != 'all' and meter_id!= "":
    if group_for == "exception":
        if groupby == "company":
            where_group_for += "and group_type = 'company' " 
            if company_id != 'all' and company_id !=''and company_id != '0':
                where_group_for += f"and type_id = '{company_id}'"

        elif groupby == "plant":
            where_group_for += "and group_type = 'plant' " 
            if plant_id != 'all' and plant_id !='' and plant_id != '0':
                where_group_for += f"and type_id = '{plant_id}'"

        elif groupby == "plant_department":
            where_group_for += "and group_type = 'plant_department' "
            if plant_department_id != 'all' and plant_department_id !='' and plant_department_id != '0':
                where_group_for += f"and type_id = '{plant_department_id}'"

        elif groupby == "equipment_group":
            where_group_for += "and group_type = 'equipment_group' "
            if equipment_group_id != 'all' and equipment_group_id !='' and equipment_group_id != '0':
                where_group_for += f"and type_id = '{equipment_group_id}'"
        
        elif groupby == "equipment":
            where_group_for += "and group_type = 'equipment' "
            if equipment_id != 'all' and equipment_id !='' and equipment_id != '0':
                where_group_for += f"and type_id = '{equipment_id}'"

        elif groupby == "function":
            if function_type == 'function_1':
                where_group_for += "and group_type = 'function_1' "
            elif function_type == 'function_2':
                where_group_for += "and group_type = 'function_2' "
            
            else:
                where_group_for += "and group_type = 'function' "

            if function_id != 'all' and function_id !='' and function_id != '0':
                where_group_for += f"and type_id = '{function_id}'"

        elif is_main_meter == 'yes' and groupby == 'meter':
            if plant_id != 'all' and plant_id !='' and plant_id != '0':
                where_group_for += "and group_type = 'plant' " 
                where_group_for += f"and type_id = '{plant_id}'"
                
            if plant_department_id != 'all' and plant_department_id !='' and plant_department_id != '0':
                where_group_for += "and group_type = 'plant_department' " 
                where_group_for += f"and type_id = '{plant_department_id}'"

            if equipment_group_id != 'all' and equipment_group_id !='' and equipment_group_id != '0':
                where_group_for += "and group_type = 'equipment_group' " 
                where_group_for += f"and type_id = '{equipment_group_id}'"
            
            if equipment_id != 'all' and equipment_id !='' and equipment_id != '0':
                where_group_for += "and group_type = 'equipment' " 
                where_group_for += f"and type_id = '{equipment_id}'"

            if function_id != 'all' and function_id !='' and function_id != '0':
                if function_type!= '':
                    if function_type == 'function_1':
                        where_group_for += "and group_type = 'function_1' " 
                    if function_type == 'function_2':
                        where_group_for += "and group_type = 'function_2' " 
                else:
                    where_group_for += "and group_type = 'function' " 
                where_group_for += f"and type_id = '{function_id}'"
        
        sql = text(f'''SELECT * FROM ems_v1.master_meter_group where status = 'active' {where_group_for} ''') 
        data2 = await cnx.execute(sql)
        data2 = data2.fetchall()
        meter_id = []  
        
        if len(data2) > 0:
            for record in data2:
                meter_id.append(record["meter_id"]) 
                
            where += f" and mm.meter_id in ({','.join(str(x) for x in meter_id)})" 
          

    query1 = ''
    demand = ''
    minmax_join = ''
  
    if is_minmax == 'yes':
        query1 +=f'''   max_subquery.max_date as max_date,
                        min_subquery.min_date as min_date,
                        max_subquery.max_shift as max_shift,
                        min_subquery.min_shift as min_shift,'''
        
        if groupby == 'meter':
            minmax_kwh = f'''(CASE 
                                        WHEN mmf.kWh = '*' THEN cp.kWh * mmf.kWh_value 
                                        WHEN mmf.kWh = '/' THEN cp.kWh / mmf.kWh_value 
                                        ELSE cp.kWh 
                                    END) AS kwh,'''
        else:
            minmax_kwh = f'''SUM(cp.equipment_kwh) AS kwh,'''
        
        minmax_join = f'''LEFT JOIN (
                                SELECT 
                                    mm.meter_id,
                                    mm.company_id,
                                    mm.bu_id,
                                    mm.plant_id,
                                    md.campus_id,
                                    mm.plant_department_id,
                                    mm.equipment_group_id,
                                    me.equipment_id,
                                    {minmax_kwh}
                                    cp.mill_date AS max_date,
                                    cp.mill_shift AS max_shift
                               FROM 
                                    {table_name}
                                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                                    INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                                    INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                                    INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                                    INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                                    INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                                    INNER JOIN ems_v1.master_model mdl ON mdl.model_id = mm.model_name
                                    INNER JOIN ems_v1.master_model_make mk ON mk.model_make_id = mdl.model_make_id
                                    LEFT JOIN ems_v1.master_function mf ON {function_where}
                                    LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                                    inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                                    inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
                                    left join master_meter_communication mmc on mmc.meter_status_code = cp.meter_status_code
                                    {equipment}
                                    left JOIN ems_v1.master_equipment_group mmt ON me.equipment_group_id = mmt.equipment_group_id
                                    {current_shift}                                
                                WHERE  
                                cp.status = '0' and mm.status = 'active' 
                                {where}
                                    
                                ORDER BY kwh DESC LIMIT 1
                                
                            ) AS max_subquery ON {on_max}
                            LEFT JOIN (
                                SELECT 
                                
                                    mm.meter_id,
                                    mm.company_id,
                                    mm.bu_id,
                                    mm.plant_id,
                                    md.campus_id,
                                    mm.plant_department_id,
                                    mm.equipment_group_id,
                                    me.equipment_id,
                                    {minmax_kwh}
                                    cp.mill_date AS min_date,
                                    cp.mill_shift AS min_shift
                                FROM 
                                    {table_name}
                                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                                    INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                                    INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                                    INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                                    INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                                    INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                                    INNER JOIN ems_v1.master_model mdl ON mdl.model_id = mm.model_name
                                    INNER JOIN ems_v1.master_model_make mk ON mk.model_make_id = mdl.model_make_id
                                    inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                                    inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
                                    {equipment}
                                    left JOIN ems_v1.master_equipment_group mmt ON me.equipment_group_id = mmt.equipment_group_id
                                    {current_shift}                                
                                WHERE  
                                cp.status = '0' and mm.status = 'active' 
                                {where}                        
                                
                            ORDER BY kwh ASC LIMIT 1

                            ) AS min_subquery ON {on_min}
'''

    else:
        query1 +=f''' '' as min_date,
                      '' as max_date,
                      '' as min_shift,
                      '' as max_shift,'''
        
    if is_demand == 'yes':
        if plant_id != '' and plant_id != 'all' and plant_id != '0':
            sql = text(f" select * from master_plant where plant_id = '{plant_id}'")
            data = await cnx.execute(sql)
            data = data.fetchall()
            if len(data)>0:
                for row in data:
                    campus_id = row["campus_id"]
                where +=f" and md.campus_id = '{campus_id}'"
        where +=f" and mm.main_demand_meter = 'yes'"

    if report_for == 'detail' or report_for =='' :
        group_by = " cp.mill_date , cp.mill_shift," + group_by
        group_by_poll = " cpd.mill_date , cpd.mill_shift," + group_by_poll
        order_by = " cp.mill_date, cp.mill_shift," + order_by
    
    elif report_for == 'summary':
        group_by = " cp.mill_date," + group_by
        group_by_poll = " cpd.mill_date," + group_by_poll
        order_by = " cp.mill_date," + order_by 

    if group_by != "":
        group_by = f"group by  {group_by} "    
        group_by_poll = f"group by  {group_by_poll} "    
    
    if order_by != "":
        order_by = f"order by {order_by_limit} {order_by}"

    poll_duration = f''' '' as off_time,
                         '' as idle_time,
                         '' as on_load_time,
                         '' as on_load_kwh,
                         '' as off_kwh,
                         '' as idle_kwh,'''

    if report_for != "12to12" and is_minmax != 'yes':  
        if period_id == "cur_shift":
            if groupby == 'meter':
                poll_duration = f'''concat(SEC_TO_TIME(sum(cp.off_time))) as off_time,
                                    concat(SEC_TO_TIME(sum(cp.idle_time))) as idle_time,
                                    concat(SEC_TO_TIME(sum(cp.on_load_time))) as on_load_time,
                                    concat(SEC_TO_TIME(sum(cp.on_load_time+cp.idle_time+cp.off_time))) as total_time,
                                    ROUND(sum(CASE WHEN mmf.kWh = '*' THEN cp.on_load_kwh * mmf.kWh_value WHEN mmf.kWh = '/' THEN cp.on_load_kwh /mmf.kWh_value else cp.on_load_kwh end ),prf.kWh)  as on_load_kwh,
                                    ROUND(sum(CASE WHEN mmf.kWh = '*' THEN cp.off_kwh * mmf.kWh_value WHEN mmf.kWh = '/' THEN cp.off_kwh /mmf.kWh_value else cp.off_kwh end ),prf.kWh) as off_kwh,
                                    ROUND(sum(CASE WHEN mmf.kWh = '*' THEN cp.idle_kwh * mmf.kWh_value WHEN mmf.kWh = '/' THEN cp.idle_kwh /mmf.kWh_value else cp.idle_kwh end ),prf.kWh)as idle_kwh,'''
            else:
                poll_duration = f'''
                                concat(SEC_TO_TIME(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'  THEN cp.on_load_time ELSE 0 END))) AS on_load_time,
                                concat(SEC_TO_TIME(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'  THEN cp.idle_time ELSE 0 END))) AS idle_time,
                                concat(SEC_TO_TIME(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'  THEN cp.off_time ELSE 0 END))) AS off_time,
                                concat(SEC_TO_TIME(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'  THEN cp.off_time+cp.on_load_time+cp.idle_time ELSE 0 END))) AS total_time,
                                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'THEN  cp.equipment_on_load_kwh ELSE 0 END ),prf.kWh) as on_load_kwh,
                                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'THEN  cp.equipment_off_kwh  ELSE 0 END ),prf.kWh) as off_kwh,
                                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'THEN  cp.equipment_idle_kwh ELSE 0 END ),prf.kWh) as idle_kwh,
                                
                                '''

        else:
            
            if groupby == 'meter':
                poll_time = f'''SUM(CASE WHEN cpd.meter_status = 0 THEN cpd.poll_duration ELSE 0 END) AS off_time,
                                SUM(CASE WHEN cpd.meter_status = 1 THEN cpd.poll_duration ELSE 0 END) AS idle_time,
                                SUM(CASE WHEN cpd.meter_status = 2 THEN cpd.poll_duration ELSE 0 END) AS on_load_time,
                                SUM(cpd.poll_duration ) AS total_time,
                                sum(CASE WHEN cpd.meter_status = 0 then CASE WHEN mmf.kWh = '*' then cpd.poll_consumption * mmf.kWh_value when  mmf.kWh = '/' then cpd.poll_consumption / mmf.kWh_value else cpd.poll_consumption end else 0 end) as off_kwh,
                                sum(CASE WHEN cpd.meter_status = 1 then CASE WHEN mmf.kWh = '*' then cpd.poll_consumption * mmf.kWh_value when  mmf.kWh = '/' then cpd.poll_consumption / mmf.kWh_value else cpd.poll_consumption end else 0 end) as idle_kwh,
                                sum(CASE WHEN cpd.meter_status = 2 then CASE WHEN mmf.kWh = '*' then cpd.poll_consumption * mmf.kWh_value when  mmf.kWh = '/' then cpd.poll_consumption / mmf.kWh_value else cpd.poll_consumption end else 0 end) as on_load_kwh
                                
                                '''
            else:
                poll_time = f'''SUM(CASE WHEN  mm.meter = 'equipment' and  mm.is_poll_meter = 'yes'  THEN CASE WHEN cpd.meter_status = 0 THEN cpd.poll_duration ELSE 0 END ELSE 0 END) AS off_time,
                                SUM(CASE WHEN  mm.meter = 'equipment' and  mm.is_poll_meter = 'yes'  THEN CASE WHEN cpd.meter_status = 1 THEN cpd.poll_duration ELSE 0 END ELSE 0 END) AS idle_time,
                                SUM(CASE WHEN  mm.meter = 'equipment' and  mm.is_poll_meter = 'yes'  THEN CASE WHEN cpd.meter_status = 2 THEN cpd.poll_duration ELSE 0 END ELSE 0 END) AS on_load_time,
                                SUM(CASE WHEN  mm.meter = 'equipment' and  mm.is_poll_meter = 'yes'  THEN cpd.poll_duration ELSE 0 END) AS total_time,
                                SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes' and cpd.meter_status = 0 THEN cpd.equipment_consumption ELSE 0 END) AS off_kwh,
                                SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes' and cpd.meter_status = 1 THEN cpd.equipment_consumption ELSE 0 END) AS idle_kwh,
                                SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes' and cpd.meter_status = 2 THEN cpd.equipment_consumption ELSE 0 END) AS on_load_kwh                               
                                
                                '''
                
            if len(result_p) > 0: 
                poll_duration = ''' TIME_FORMAT(SEC_TO_TIME(SUM(cpd.off_time)),'%H:%i:%s') AS off_time,
                                    TIME_FORMAT(SEC_TO_TIME(SUM(cpd.idle_time)),'%H:%i:%s') AS idle_time,
                                    TIME_FORMAT(SEC_TO_TIME(SUM(cpd.on_load_time)),'%H:%i:%s') AS on_load_time,
                                    TIME_FORMAT(SEC_TO_TIME(SUM(cpd.total_time)),'%H:%i:%s') AS total_time,
                                    ROUND(ifnull(sum(cpd.off_kwh),0),prf.kWh) as off_kwh,
                                    ROUND(ifnull(sum(cpd.on_load_kwh),0),prf.kWh) as on_load_kwh,
                                    ROUND(ifnull(sum(cpd.idle_kwh),0),prf.kWh) as idle_kwh,
                '''
                 
                # where_poll = ''
                # if report_for == 'detail' or report_for == '':
                #     where_poll = ' and cpd.mill_date = cp.mill_date and cpd.mill_shift = cp.mill_shift'
                    
                # if report_for == 'summary':
                #     where_poll = ' and cpd.mill_date = cp.mill_date'
                    
                join = f'''
                        left join (select 
                            cpd.meter_id,
                            min(cpd.mill_date) mill_date,
                            min(cpd.mill_shift) mill_shift,
                            {poll_time}
                        from
                            ems_v1_completed.polling_data_{month_year} cpd
                            INNER JOIN master_meter mm on mm.meter_id = cpd.meter_id
                            INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                            INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                            INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                            INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                            INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                            LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                            inner JOIN ems_v1.master_meter_factor mmf ON mm.meter_id = mmf.meter_id and mmf.plant_id = md.plant_id 
                            inner JOIN ems_v1.master_parameter_roundoff prf ON mm.plant_id = prf.plant_id
                            {equipment}
                        where mm.status = 'active' {where_p} 
                          {group_by_poll}) as cpd 
                        on cpd.meter_id = cp.meter_id AND cpd.mill_date = cp.mill_date AND cpd.mill_shift  =cp.mill_shift      '''
                              
        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or period_id=="#from_to" or period_id == "#sel_year":
            if from_date.month == to_date.month and from_date.year == to_date.year:
                pass
            else:
                if len(joins)>0:
                    join = " UNION ALL ".join(joins)
                    month_year_poll = f"( {join})"    
                    # where_poll = ''
                    # if report_for == 'detail' or report_for == '':
                    #     where_poll = ' and cpd.mill_date = cp.mill_date and cpd.mill_shift = cp.mill_shift'
                    
                    # if report_for == 'summary':
                    #     where_poll = ' and cpd.mill_date = cp.mill_date'
                    
                    join = f'''left join (select 
                                    cpd.meter_id,
                                    min(cpd.mill_date) mill_date,
                                    min(cpd.mill_shift) mill_shift,
                                    {poll_time}
                                from
                                    {month_year_poll} cpd
                                    INNER JOIN master_meter mm on mm.meter_id = cpd.meter_id
                                    INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                                    INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                                    INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                                    INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                                    INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                                    LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                                    inner JOIN ems_v1.master_meter_factor mmf ON mm.meter_id = mmf.meter_id and mmf.plant_id = md.plant_id
                                    inner JOIN ems_v1.master_parameter_roundoff prf ON mm.plant_id = prf.plant_id
                                    {equipment}                                          
                                where mm.status = 'active' {where_p}  
                                  {group_by_poll}) as cpd 
                                on cpd.meter_id = cp.meter_id  AND cpd.mill_date = cp.mill_date AND cpd.mill_shift  =cp.mill_shift      '''


    query = text(f'''
            SELECT                       
                mc.company_code,
                mc.company_name,
                mb.bu_code,
                mb.bu_name,
                md.plant_code,
                md.plant_name,
                mcs.campus_name,
                mpd.plant_department_code,
                mpd.plant_department_name,
                ifnull(mmt.equipment_group_code,'') equipment_group_code,
                ifnull(mmt.equipment_group_name,'') equipment_group_name,
                ifnull(me.equipment_code,'') equipment_code,
                ifnull(me.equipment_name,'') equipment_name,
                ifnull(mf.function_name,'') function_name,
                ifnull(mf.function_code,'') function_code,
                mm.meter_code,
                mm.meter_name,
                count(DISTINCT mm.meter_code) AS meter_count,
                COUNT(DISTINCT CASE WHEN mm.meter_type = 'Primary' THEN mm.meter_code END) AS pm_meter_count,
                cp.power_id,
                mm.company_id,
                mm.bu_id,
                mm.plant_id,
                md.campus_id,
                mm.plant_department_id,
                mm.equipment_group_id ,
                ifnull(me.equipment_id,0) equipment_id,
                ifnull(mf.function_id,0)function_id,
                ifnull(me.equipment_class_id,0) equipment_class_id,
                ifnull(ecls.equipment_class_code,'') equipment_class_code,
                cp.meter_id,
                cp.design_id,
                cp.beam_id,
                cp.date_time,
                cp.date_time1,
                cp.mill_date,
                cp.mill_shift,
                cp.meter_status_code,
                mm.meter_type,
                {formula}
                GROUP_CONCAT(DISTINCT me.equipment_id) AS equipment_ids,
                ROUND(AVG(case when mmf.vln_avg = '*' then cp.vln_avg_thd * mmf.vln_avg_value when  mmf.vln_avg = '/' then cp.vln_avg_thd / mmf.vln_avg_value else cp.vln_avg_thd end ),prf.vln_avg) AS vln_avg_thd,
                ROUND(AVG(case when mmf.vln_avg = '*' then cp.vln_avg * mmf.vln_avg_value when  mmf.vln_avg = '/' then cp.vln_avg / mmf.vln_avg_value else cp.vln_avg end ),prf.vln_avg) AS vln_avg,
                ROUND(AVG(case when mmf.r_volt = '*' then cp.r_volt * mmf.r_volt_value when  mmf.r_volt = '/' then cp.r_volt / mmf.r_volt_value else cp.r_volt end ),prf.r_volt) AS r_volt,
                ROUND(AVG(case when mmf.y_volt = '*' then cp.y_volt * mmf.y_volt_value when  mmf.y_volt = '/' then cp.y_volt / mmf.y_volt_value else cp.y_volt end ),prf.y_volt) AS y_volt,
                ROUND(AVG(case when mmf.b_volt = '*' then cp.b_volt * mmf.b_volt_value when  mmf.b_volt = '/' then cp.b_volt / mmf.b_volt_value else cp.b_volt end ),prf.b_volt) AS b_volt,
                ROUND(AVG(case when mmf.vll_avg = '*' then cp.vll_avg * mmf.vll_avg_value when  mmf.vll_avg = '/' then cp.vll_avg / mmf.vll_avg_value else cp.vll_avg end ),prf.vll_avg) AS vll_avg,
                ROUND(AVG(case when mmf.vll_avg = '*' then cp.vll_avg_thd * mmf.vll_avg_value when  mmf.vll_avg = '/' then cp.vll_avg_thd / mmf.vll_avg_value else cp.vll_avg_thd end ),prf.vll_avg) AS vll_avg_thd,
                ROUND(AVG(case when mmf.ry_volt = '*' then cp.ry_volt * mmf.ry_volt_value when  mmf.ry_volt = '/' then cp.ry_volt / mmf.ry_volt_value else cp.ry_volt end ),prf.ry_volt) AS ry_volt,
                ROUND(AVG(case when mmf.yb_volt = '*' then cp.yb_volt * mmf.yb_volt_value when  mmf.yb_volt = '/' then cp.yb_volt / mmf.yb_volt_value else cp.yb_volt end ),prf.yb_volt) AS yb_volt,
                ROUND(AVG(case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value when  mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end ),prf.br_volt) AS br_volt,
                ROUND(AVG(case when mmf.r_current = '*' then cp.r_current * mmf.r_current_value when  mmf.r_current = '/' then cp.r_current / mmf.r_current_value else cp.r_current end ),prf.r_current) AS r_current,
                ROUND(AVG(case when mmf.y_current = '*' then cp.y_current * mmf.y_current_value when  mmf.y_current = '/' then cp.y_current / mmf.y_current_value else cp.y_current end ),prf.y_current) AS y_current,
                ROUND(AVG(case when mmf.b_current = '*' then cp.b_current * mmf.b_current_value when  mmf.b_current = '/' then cp.b_current / mmf.b_current_value else cp.b_current end ),prf.b_current) AS b_current,
                ROUND(AVG(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value when  mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end ),prf.t_current) AS t_current,
                ROUND(AVG(case when mmf.t_watts = '*' then cp.t_watts * mmf.t_watts_value when  mmf.t_watts = '/' then cp.t_watts / mmf.t_watts_value else cp.t_watts end ),prf.t_watts) AS t_watts,
                ROUND(AVG(case when mmf.r_watts = '*' then cp.r_watts * mmf.r_watts_value when  mmf.r_watts = '/' then cp.r_watts / mmf.r_watts_value else cp.r_watts end ),prf.r_watts) AS r_watts,
                ROUND(AVG(case when mmf.y_watts = '*' then cp.y_watts * mmf.y_watts_value when  mmf.y_watts = '/' then cp.y_watts / mmf.y_watts_value else cp.y_watts end ),prf.y_watts) AS y_watts,
                ROUND(AVG(case when mmf.b_watts = '*' then cp.b_watts * mmf.b_watts_value when  mmf.b_watts = '/' then cp.b_watts / mmf.b_watts_value else cp.b_watts end ),prf.b_watts) AS b_watts,
                ROUND(AVG(case when mmf.t_var = '*' then cp.t_var * mmf.t_var_value when  mmf.t_var = '/' then cp.t_var / mmf.t_var_value else cp.t_var end ),prf.t_var) AS t_var,
                ROUND(AVG(case when mmf.r_var = '*' then cp.r_var * mmf.r_var_value when  mmf.r_var = '/' then cp.r_var / mmf.r_var_value else cp.r_var end ),prf.r_var) AS r_var,
                ROUND(AVG(case when mmf.y_var = '*' then cp.y_var * mmf.y_var_value when  mmf.y_var = '/' then cp.y_var / mmf.y_var_value else cp.y_var end ),prf.y_var) AS y_var,
                ROUND(AVG(case when mmf.b_var = '*' then cp.b_var * mmf.b_var_value when  mmf.b_var = '/' then cp.b_var / mmf.b_var_value else cp.b_var end ),prf.b_var) AS b_var,
                ROUND(AVG(case when mmf.t_voltampere = '*' then cp.t_voltampere * mmf.t_voltampere_value when  mmf.t_voltampere = '/' then cp.t_voltampere / mmf.t_voltampere_value else cp.t_voltampere end ),prf.t_voltampere) AS t_voltampere,
                ROUND(AVG(case when mmf.r_voltampere = '*' then cp.r_voltampere * mmf.r_voltampere_value when  mmf.r_voltampere = '/' then cp.r_voltampere / mmf.r_voltampere_value else cp.r_voltampere end ),prf.r_voltampere) AS r_voltampere,
                ROUND(AVG(case when mmf.y_voltampere = '*' then cp.y_voltampere * mmf.y_voltampere_value when  mmf.y_voltampere = '/' then cp.y_voltampere / mmf.y_voltampere_value else cp.y_voltampere end ),prf.y_voltampere) AS y_voltampere,
                ROUND(AVG(case when mmf.b_voltampere = '*' then cp.b_voltampere * mmf.b_voltampere_value when  mmf.b_voltampere = '/' then cp.b_voltampere / mmf.b_voltampere_value else cp.b_voltampere end ),prf.b_voltampere) AS b_voltampere,
                ROUND(AVG(case when mmf.avg_powerfactor = '*' then ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value when  mmf.avg_powerfactor = '/' then ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value else ABS(cp.avg_powerfactor) end ),prf.avg_powerfactor) AS avg_powerfactor,
                ROUND(AVG(CASE WHEN mm.meter_type = 'Primary' THEN CASE WHEN mmf.avg_powerfactor = '*' THEN ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value WHEN  mmf.avg_powerfactor = '/' THEN ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value ELSE ABS(cp.avg_powerfactor) END ELSE 0 END ),prf.avg_powerfactor) AS pm_avg_powerfactor,

                ROUND(AVG(case when mmf.r_powerfactor = '*' then ABS(cp.r_powerfactor) * mmf.r_powerfactor_value when  mmf.r_powerfactor = '/' then ABS(cp.r_powerfactor) / mmf.r_powerfactor_value else ABS(cp.r_powerfactor) end ),prf.r_powerfactor) AS r_powerfactor,
                ROUND(AVG(case when mmf.y_powerfactor = '*' then ABS(cp.y_powerfactor) * mmf.y_powerfactor_value when  mmf.y_powerfactor = '/' then ABS(cp.y_powerfactor) / mmf.y_powerfactor_value else ABS(cp.y_powerfactor) end ),prf.y_powerfactor) AS y_powerfactor,
                ROUND(AVG(case when mmf.b_powerfactor = '*' then ABS(cp.b_powerfactor) * mmf.b_powerfactor_value when  mmf.b_powerfactor = '/' then ABS(cp.b_powerfactor) / mmf.b_powerfactor_value else ABS(cp.b_powerfactor) end ),prf.b_powerfactor) AS b_powerfactor,
                ROUND(AVG(case when mmf.powerfactor = '*' then ABS(cp.powerfactor) * mmf.powerfactor_value when  mmf.powerfactor = '/' then ABS(cp.powerfactor) / mmf.powerfactor_value else ABS(cp.powerfactor) end ),prf.powerfactor) AS powerfactor,
                
                ROUND(AVG(case when mmf.kvah = '*' then cp.kvah * mmf.kvah_value when  mmf.kvah = '/' then cp.kvah / mmf.kvah_value else cp.kvah end ),prf.kvah) AS kvah,
                ROUND(SUM(case when mmf.kw = '*' then cp.t_watts * mmf.kw_value when  mmf.kw = '/' then cp.t_watts / mmf.kw_value else cp.t_watts end ),prf.kw) AS kw,
                ROUND(AVG(case when mmf.kvar = '*' then cp.kvar * mmf.kvar_value when  mmf.kvar = '/' then cp.kvar / mmf.kvar_value else cp.kvar end ),prf.kvar) AS kvar,
                ROUND(AVG(case when mmf.power_factor = '*' then cp.power_factor * mmf.power_factor_value when  mmf.power_factor = '/' then cp.power_factor / mmf.power_factor_value else cp.power_factor end ),prf.power_factor) AS power_factor,
                ROUND(AVG(case when mmf.kva = '*' then cp.kva * mmf.kva_value when  mmf.kva = '/' then cp.kva / mmf.kva_value else cp.kva end ),prf.kva) AS kva,

                ROUND(AVG(CASE WHEN cp.frequency <> 0 THEN CASE  WHEN mmf.frequency = '*' THEN cp.frequency * mmf.frequency_value   WHEN mmf.frequency = '/' THEN cp.frequency / mmf.frequency_value ELSE cp.frequency END ELSE '' END), prf.frequency) AS frequency,

                cp.machine_status,
                cp.status,
                cp.created_on,
                cp.created_by,
                cp.modified_on,
                cp.modified_by,
                
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS machine_kWh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ),prf.machine_kWh) AS master_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kWh,
                ROUND(SUM(CASE WHEN mm.meter_type = 'Primary' THEN CASE WHEN mmf.kWh = '*' THEN cp.kWh * mmf.kWh_value WHEN  mmf.kWh = '/' THEN cp.kWh / mmf.kWh_value ELSE cp.kWh END ELSE 0 END ),prf.kWh) AS pm_kwh,
               
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS total_kWh,
                ROUND(MIN(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kwh_min,
                ROUND(MAX(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kwh_max,
                ROUND(AVG(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS avg_kWh,
                {query1}
                
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_machine_kWh / mmf.machine_kWh_value else cp.reverse_machine_kWh end ),prf.machine_kWh) AS reverse_machine_kWh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_master_kwh / mmf.machine_kWh_value else cp.reverse_master_kwh end ),prf.machine_kWh) AS reverse_master_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_kwh / mmf.kWh_value else cp.reverse_kwh end ),prf.kWh) AS reverse_kwh,
                
                mm.ip_address,
                mm.address as slave_id,
                mm.port,
                mm.mac,
              
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),prf.kwh) AS kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),prf.kwh) AS kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),prf.kwh) AS kwh_3,
                ROUND(min(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end  END),prf.machine_kWh) AS start_kwh_1,
                ROUND(max(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end  END),prf.machine_kWh) AS start_kwh_2,
                ROUND(max(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end  END),prf.machine_kWh) AS start_kwh_3,     
                ROUND(min(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end  END),prf.machine_kWh) AS end_kwh_1,
                ROUND(max(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end  END),prf.machine_kWh) AS end_kwh_2,
                ROUND(max(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end  END),prf.machine_kWh) AS end_kwh_3,
                
                CASE WHEN cp.date_time <= DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN 'S' ELSE 'N' END AS nocom,
                CASE WHEN cp.date_time <= DATE_SUB(NOW(), INTERVAL 5 MINUTE) and cp.meter_status_code = 0 THEN 'Device Offline' ELSE mmc.meter_status_description END AS meter_status_description, 
                COUNT(DISTINCT CASE WHEN cp.date_time <= DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN mm.meter_code END) AS nocom_s_count,
                COUNT( DISTINCT CASE WHEN cp.date_time > DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN mm.meter_code END) AS nocom_n_count,
                COUNT( DISTINCT CASE WHEN mm.meter_type = 'Primary' AND cp.date_time <= DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN mm.meter_code END) AS pm_nocom_s_count,
                COUNT(DISTINCT  CASE WHEN mm.meter_type = 'Primary' AND cp.date_time > DATE_SUB(NOW(), INTERVAL 5 MINUTE) THEN mm.meter_code END) AS pm_nocom_n_count,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes' THEN  cp.equipment_kwh  ELSE 0 END),prf.kWh) AS equipment_kwh,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes' THEN  cp.equipment_kwh  ELSE 0 END),0) AS units,
                ROUND(SUM(CASE WHEN mm.meter = 'common' THEN cp.equipment_kwh ELSE 0 END),prf.kWh) AS common_kwh,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment'and mm.meter_type = 'primary' THEN  cp.equipment_kwh ELSE 0 END),prf.kWh) AS pm_equipment_kwh,
                ifnull(Round(Round(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes' THEN  cp.equipment_kwh ELSE 0 END),0)/Round(SUM(CASE WHEN mm.meter = 'equipment'AND mm.meter_type = 'primary' THEN  cp.actual_ton ELSE 0 END),0),0),0) units_per_ton,
                ROUND(SUM(CASE WHEN mm.meter = 'common' and mm.meter_type = 'primary' THEN  cp.equipment_kwh  ELSE 0 END),prf.kWh) AS pm_common_kwh,
                ROUND(SUM(cp.equipment_kwh),prf.kWh) AS calculated_kwh,
                
                '' as tooltip_kwh,
                '' as formula,
                mm.source,
                ROUND(SUM(CASE WHEN mm.main_demand_meter = 'yes' THEN CASE WHEN mmf.kva = '*' THEN cp.kva * mmf.kva_value WHEN  mmf.kva = '/' THEN cp.kva / mmf.kva_value ELSE cp.kva END ELSE 0 END),prf.kva) AS demand,       
                IFNULL(ROUND(AVG(CASE WHEN mm.main_demand_meter = 'yes' THEN CASE WHEN mmf.avg_powerfactor = '*' THEN ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value WHEN  mmf.avg_powerfactor = '/' THEN ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value ELSE ABS(cp.avg_powerfactor) END ELSE 0 END),prf.avg_powerfactor),0) AS dm_powerfactor,             
                ROUND(SUM(CASE WHEN mm.main_demand_meter = 'yes' THEN CASE WHEN mmf.kva = '*' THEN cp.actual_demand * mmf.kva_value WHEN  mmf.kva = '/' THEN cp.actual_demand / mmf.kva_value ELSE cp.actual_demand END ELSE 0 END),prf.kva) AS actual_demand,            
		        MIN(CASE WHEN mm.main_demand_meter = 'yes' THEN cp.demand_dtm ELSE NULL END) AS d_date_time,
                SUM(CASE WHEN mm.main_demand_meter = 'yes' THEN mm.max_demand else 0 end ) AS max_demand,  
                SUM(CASE WHEN mm.main_demand_meter = 'yes' THEN mm.max_demand else 0 end ) AS max_pf,  
                mm.meter,  
                mdl.model_name,
                mk.model_make_name,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment'AND mm.meter_type = 'primary' THEN cp.actual_ton else 0 end ),0) actual_ton,
                CONCAT(FLOOR(cp.runhour / 86400), ' days ',   SEC_TO_TIME(cp.runhour % 86400)) AS runhour,
                ROUND(AVG(case when mmf.r_volt_thd = '*' then cp.r_volt_thd * mmf.r_volt_thd_value when  mmf.r_volt_thd = '/' then cp.r_volt_thd / mmf.r_volt_thd_value else cp.r_volt_thd end ),prf.r_volt_thd) AS r_volt_thd,
                ROUND(AVG(case when mmf.y_volt_thd = '*' then cp.y_volt_thd * mmf.y_volt_thd_value when  mmf.y_volt_thd = '/' then cp.y_volt_thd / mmf.y_volt_thd_value else cp.y_volt_thd end ),prf.y_volt_thd) AS y_volt_thd,
                ROUND(AVG(case when mmf.b_volt_thd = '*' then cp.b_volt_thd * mmf.b_volt_thd_value when  mmf.b_volt_thd = '/' then cp.b_volt_thd / mmf.b_volt_thd_value else cp.b_volt_thd end ),prf.b_volt_thd) AS b_volt_thd,
                ROUND(AVG(case when mmf.avg_volt_thd = '*' then cp.avg_volt_thd * mmf.avg_volt_thd_value when  mmf.avg_volt_thd = '/' then cp.avg_volt_thd / mmf.avg_volt_thd_value else cp.avg_volt_thd end ),prf.avg_volt_thd) AS avg_volt_thd,
                ROUND(AVG(case when mmf.r_current_thd = '*' then cp.r_current_thd * mmf.r_current_thd_value when  mmf.r_current_thd = '/' then cp.r_current_thd / mmf.r_current_thd_value else cp.r_current_thd end ),prf.r_current_thd) AS r_current_thd,
                ROUND(AVG(case when mmf.y_current_thd = '*' then cp.y_current_thd * mmf.y_current_thd_value when  mmf.y_current_thd = '/' then cp.y_current_thd / mmf.y_current_thd_value else cp.y_current_thd end ),prf.y_current_thd) AS y_current_thd,
                ROUND(AVG(case when mmf.b_current_thd = '*' then cp.b_current_thd * mmf.b_current_thd_value when  mmf.b_current_thd = '/' then cp.b_current_thd / mmf.b_current_thd_value else cp.b_current_thd end ),prf.b_current_thd) AS b_current_thd,
                ROUND(AVG(case when mmf.avg_current_thd = '*' then cp.avg_current_thd * mmf.avg_current_thd_value when  mmf.avg_current_thd = '/' then cp.avg_current_thd / mmf.avg_current_thd_value else cp.avg_current_thd end ),prf.avg_current_thd) AS avg_current_thd,
                ROUND(AVG(case when mmf.r_volt_thd = '*' then cp.ry_volt_thd * mmf.r_volt_thd_value when  mmf.r_volt_thd = '/' then cp.ry_volt_thd / mmf.r_volt_thd_value else cp.ry_volt_thd end ),prf.r_volt_thd) AS ry_volt_thd,
                ROUND(AVG(case when mmf.y_volt_thd = '*' then cp.yb_volt_thd * mmf.y_volt_thd_value when  mmf.y_volt_thd = '/' then cp.yb_volt_thd / mmf.y_volt_thd_value else cp.yb_volt_thd end ),prf.y_volt_thd) AS yb_volt_thd,
                ROUND(AVG(case when mmf.b_volt_thd = '*' then cp.br_volt_thd * mmf.b_volt_thd_value when  mmf.b_volt_thd = '/' then cp.br_volt_thd / mmf.b_volt_thd_value else cp.br_volt_thd end ),prf.b_volt_thd) AS br_volt_thd,

                {poll_duration}
                {group_id}
                {group_code}
                {group_name}                       
            FROM 
                {table_name}                       
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                INNER JOIN ems_v1.master_model mdl ON mdl.model_id = mm.model_name
                INNER JOIN ems_v1.master_model_make mk ON mk.model_make_id = mdl.model_make_id
                LEFT JOIN ems_v1.master_function mf ON {function_where}
                LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
                left join master_meter_communication mmc on mmc.meter_status_code = cp.meter_status_code
                {equipment}
                left JOIN ems_v1.master_equipment_group mmt ON me.equipment_group_id = mmt.equipment_group_id
                left JOIN ems_v1.master_equipment_class ecls ON me.equipment_class_id = ecls.equipment_class_id
                {current_shift} 
                {join}   
                {minmax_join}                          
            WHERE  
                cp.status = '0' and mm.status = 'active' 
                {where}                        
                {group_by}
                {order_by}
                
            ''')
                    # ROUND(AVG(case when mmf.frequency = '*' then cp.frequency * mmf.frequency_value when  mmf.frequency = '/' then cp.frequency / mmf.frequency_value else cp.frequency end ),prf.frequency) AS frequency,
    # createFolder("Log/",f"Issue in returning data {query}")
    datas = await cnx.execute(query)
    datas = datas.mappings().all()
    return datas
    
async def function_dashboard(cnx,plant_id):

        mill_date = date.today()
        mill_shift = 0
        group_name = ''
        func_name = ''
        formula1 = ''
        results = ''

        # sql1 = f'select * from ems_v1.master_shifts'
        data = await shift_Lists(cnx, '',plant_id, '', '')
        if len(data)>0:
            for row in data:
                mill_date = row["mill_date"]
                mill_shift = row["mill_shift"]

        sql2 = f'SELECT * FROM ems_v1.master_energy_calculations ORDER BY group_name, s_no'
        result = await cnx.execute(sql2)
        result = result.fetchall()

        if len(result)>0:
            para = ''
            for rows in result:
                para = rows['parameter']

            if para == 'kw':
                para = "case when mmf.kw = '*' then p.t_watts * mmf.kw_value when  mmf.kw = '/' then p.t_watts / mmf.kw_value else p.t_watts end "

            if para == 'kWh':
                para = "case when mmf.kWh = '*' then p.kWh * mmf.kWh_value when  mmf.kWh = '/' then p.kWh / mmf.kWh_value else p.kWh end "
        
            sql3 = text(f'''
                    select 
                        p.meter_id,
                        min(mm.meter_name) as meter_name,
                        sum({para}) as kWh 
                    from 
                        ems_v1.current_power p
                        left join  ems_v1.master_meter mm on mm.meter_id=p.meter_id
                        left join  ems_v1.master_meter_factor mmf on mm.meter_id=mmf.meter_id
                    where 
                        p.mill_date = '{mill_date}' and p.mill_shift = {mill_shift}
                    group by 
                        p.meter_id 
                    order by 
                        p.meter_id''')
           
            res = await cnx.execute(sql3)
            res = res.fetchall()

            meter_id_dict={}
            # dict_tt ={}
            dict={}
            for row in res:
                dict[row['meter_id']] = row['kWh']
                # dict_tt[row['meter_name']] = row['kWh']
                meter_id_dict[row['meter_id']] = row['meter_name']
            datas = []

            for rows in result:
                group_name = rows['group_name']
                func_name = rows['function_name']
                formula = rows['formula2']
                formula1 = rows['formula1']
                
                results = eval(formula, {"dict": dict})
                
                # meter_ids = re.findall(r'dict\[(\d+)\]', formula)
        
                # # Convert extracted IDs to integers and filter out IDs not present in the dict
                # valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]

                # # Create a tooltip dictionary with valid meter IDs and their kWh values
                # formula_tooltip = {meter_id: dict[meter_id] for meter_id in valid_formula_meter_ids}
                meter_ids = re.findall(r'dict\[(\d+)\]', formula)

                valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]

                formula_tooltip = {meter_id_dict[meter_id]: dict[meter_id] for meter_id in valid_formula_meter_ids}
                datas.append({"group_name": group_name,"function_name": func_name,"function_value": results,"formula1": formula1,"tooltip":formula_tooltip})
        return datas

    
async def function_dashboard2(cnx,plant_id):
    
        mill_date = date.today()
        mill_shift = 0
        group_name = ''
        func_name = ''
        formula1 = ''
        results = ''

        # sql1 = f'select * from ems_v1.master_shifts'
        data = await shift_Lists(cnx, '',plant_id, '', '')
        if len(data)>0:
            for row in data:
                mill_date = row["mill_date"]
                mill_shift = row["mill_shift"]

        sql2 = f'SELECT * FROM ems_v1.master_energy_calculations2 ORDER BY group_name, s_no'
        result = await cnx.execute(sql2)
        result = result.fetchall()

        if len(result)>0:
            para = ''
            for rows in result:
                para = rows['parameter']

            if para == 'kw':
                para = "case when mmf.kw = '*' then p.t_watts * mmf.kw_value when  mmf.kw = '/' then p.t_watts / mmf.kw_value else p.t_watts end "

            if para == 'kWh':
                para = "case when mmf.kWh = '*' then p.kWh * mmf.kWh_value when  mmf.kWh = '/' then p.kWh / mmf.kWh_value else p.kWh end "
        
            sql3 = text(f'''
                    select 
                        p.meter_id,
                        min(mm.meter_name) as meter_name,
                        sum({para}) as kWh 
                    from 
                        ems_v1.current_power p
                        left join  ems_v1.master_meter mm on mm.meter_id=p.meter_id
                        left join  ems_v1.master_meter_factor mmf on mm.meter_id=mmf.meter_id
                    where 
                        p.mill_date = '{mill_date}' and p.mill_shift = {mill_shift}
                    group by 
                        p.meter_id 
                    order by 
                        p.meter_id''')

            res = await cnx.execute(sql3)
            res = res.fetchall()

            meter_id_dict={}
            # dict_tt ={}
            dict={}
            for row in res:
                dict[row['meter_id']] = row['kWh']
                # dict_tt[row['meter_name']] = row['kWh']
                meter_id_dict[row['meter_id']] = row['meter_name']
            datas = []

            for rows in result:
                group_name = rows['group_name']
                func_name = rows['function_name']
                formula = rows['formula2']
                formula1 = rows['formula1']
                
                results = eval(formula, {"dict": dict})
                
                # meter_ids = re.findall(r'dict\[(\d+)\]', formula)
        
                # # Convert extracted IDs to integers and filter out IDs not present in the dict
                # valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]

                # # Create a tooltip dictionary with valid meter IDs and their kWh values
                # formula_tooltip = {meter_id: dict[meter_id] for meter_id in valid_formula_meter_ids}
                meter_ids = re.findall(r'dict\[(\d+)\]', formula)

                valid_formula_meter_ids = [int(id) for id in meter_ids if int(id) in dict]
              
                formula_tooltip = {meter_id_dict[meter_id]: dict[meter_id] for meter_id in valid_formula_meter_ids}
            
                datas.append({"group_name": group_name,"function_name": func_name,"function_value": results,"formula1": formula1,"tooltip":formula_tooltip})
        
        return datas
    
async def communication_status_query(cnx,plant_department_id,plant_id,campus_id):
    
    where = ''
    if plant_department_id != '':
        where +=f"and mm.plant_department_id = '{plant_department_id}' "

    if plant_id != '' and plant_id != 0:
        where +=f" and mm.plant_id = {plant_id}"

    if campus_id != '' and campus_id != 0:
        where +=f" and mp.campus_id = {campus_id}"

    query = text(f'''
                select
                    c.converter_id,
                    c.converter_name,
                    c.ip_address,
                    mm.plant_id,
                    mp.campus_id
                from
                    master_meter mm
                    INNER JOIN ems_v1.master_converter_detail c ON  mm.converter_id = c.converter_id 
                    INNER JOIN master_plant mp ON mp.plant_id = mm.plant_id 
                where
                    1=1
                    {where}
                    group by c.converter_id
                 ''')
    data = await cnx.execute(query)
    data = data.fetchall()       
    return data

async def month_report(cnx,campus_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,employee_id,meter_id,month_year,report_for,report_type,meter_type,kwh,request):

        groupby = ""
        where = ""
        result_query = ''
        rslt = ''
        kwh_type = kwh
        if meter_id == "" or meter_id == 'all':
            pass
        else:
            meter_id = await id(meter_id)
            where += f" and mm.meter_id IN ({meter_id})"

        if  employee_id != '':
            query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
            res = await cnx.execute(query)
            res = res.fetchall()       
    
            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]

        if company_id !='' and company_id !=0 and company_id != 'all':
            where += f" and mm.company_id ={company_id}"

        if bu_id !='' and bu_id !=0 and bu_id != 'all':
            where += f" and mm.bu_id ={bu_id}"

        if plant_id !='' and plant_id !=0 and plant_id != 'all':
            where += f" and md.plant_id ={plant_id}"

        if campus_id !='' and campus_id !=0 and campus_id != 'all':
            where += f" and md.campus_id ={campus_id}"

        if plant_department_id !='' and plant_department_id != 0 and plant_department_id != 'all':
            where += f" and ms.plant_department_id ={plant_department_id}"

        if equipment_group_id !='' and equipment_group_id!= 0 and equipment_group_id != 'all':
            where += f" and mmt.equipment_group_id ={equipment_group_id}"

        if meter_type!= '':
            where += f" and mm.meter_type ='{meter_type}'"

        if report_for == '6to6':
            month, year = month_year.split('-')
            tbl_name = f"ems_v1_completed.power_{month}{year} cp" 

        else:
            month, year = month_year.split('-')
            tbl_name = f"ems_v1_completed.power_{month}{year}_12 cp"

        if report_type == 'date':
            day = "CONCAT('d', DAY(cp.mill_date))"
            
        elif report_type == 'shift':
            day = "CONCAT(CONCAT('ds', cp.mill_shift), '_', DAY(cp.mill_date))"
            groupby = ",cp.mill_shift"
        
        
        query = text(f'''
            SELECT
                mm.meter_code AS meter_code,
                mm.meter_name AS meter_name,
                {day} AS day,
                DATE_FORMAT(cp.mill_date, '%d-%m-%Y') AS mill_date,
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ELSE 0 END),prf.machine_kWh) AS master_kwh,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ELSE 0 END),prf.machine_kWh) AS machine_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value  when mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kwh,
                ROUND(SUM(cp.equipment_kwh),prf.kWh) AS calculated_kwh,
                mm.meter_type,
                md.plant_name,
                md.campus_id,
                c.campus_name
            FROM
                {tbl_name}
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                inner JOIN ems_v1.master_plant_wise_department ms ON ms.plant_department_id = mm.plant_department_id                   
                inner JOIN ems_v1.master_plant md ON md.plant_id = mm.plant_id                   
                inner JOIN ems_v1.master_campus c ON c.campus_id = md.campus_id                   
                LEFT JOIN ems_v1.master_equipment_group mmt ON mmt.equipment_group_id = mm.equipment_group_id 
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id                   
            WHERE
                1=1  and mm.status = 'active'{where} and DATE_FORMAT(cp.mill_date ,'%m-%Y') = '{month_year}' 
            GROUP BY
                mm.meter_id,
                cp.mill_date
                {groupby}               
        ''')
        rslt = await cnx.execute(query)
        rslt = rslt.fetchall()
        createFolder("Log/","result"+str(rslt))
        if rslt !='':
            output = {}

            if report_type == 'date':
                output_keys = [f'd{day}' for day in range(1, 32)]
                machine_kwh_keys = [f'machine_kwh_d{day}' for day in range(1, 32)]
                master_kwh_keys = [f'master_kwh_d{day}' for day in range(1, 32)]
                
            elif report_type == 'shift':
                output_keys = [f'ds{shift}_{day}' for day in range(1, 32) for shift in range(1, 4)]
                machine_kwh_keys = [f'machine_kwh_ds{shift}_{day}' for day in range(1, 32) for shift in range(1, 4)]
                master_kwh_keys = [f'master_kwh_ds{shift}_{day}' for day in range(1, 32) for shift in range(1, 4)]
            
            for row in rslt:
                meter_code = row.meter_code
                meter_name = row.meter_name
                meter_type = row.meter_type
                plant_name = row.plant_name
                day = row.day
                if kwh_type =='calculated':
                    kwh = row.calculated_kwh
                else:
                    kwh = row.kwh

                machine_kwh = row.machine_kwh
                master_kwh = row.master_kwh
            
                if meter_code not in output:
                    output[meter_code] = {
                        'meter_code': meter_code,
                        'meter_name': meter_name,
                        "meter_type": meter_type,
                        "plant_name": plant_name
                    }
                    for key, machine_kwh_key,master_kwh_key in zip(output_keys, machine_kwh_keys,master_kwh_keys):
                        output[meter_code][key] = 0
                        output[meter_code][machine_kwh_key] = 0
                        output[meter_code][master_kwh_key] = 0
            
                output[meter_code][day] = kwh
                output[meter_code][f'machine_kwh_{day}'] = machine_kwh
                output[meter_code][f'master_kwh_{day}'] = master_kwh
        
                result=list(output.values())
            if rslt == []:
                result = []
            
            
        createFolder("Log/","result"+str(123))
        return result
    

async def daily_report(cnx,date,report_for,request):
    try:
        res = ''
        ress = ''
        res_q4 = ''
        formula_d = 0
        formula_m = 0
        formula_y = 0
        func_name = ''
        formula = ''
        roundoff_value = 0
        type = ''
        dates=await parse_date(date)
        
        from_date_str = dates.strftime("%d-%m-%Y")
        datetime_obj = datetime.strptime(from_date_str, "%d-%m-%Y")
        f_date = datetime_obj.strftime("%d-%m-%Y") 
        formatted_date = datetime_obj.strftime("%d-%b-%Y") 
        month = f_date[3:5]
        given_year = formatted_date[7:]
        print("month",month)
        if int(month) >=4:
            year = formatted_date[7:]
            next_year = int(given_year) +1
        if int(month) <4:
            year = int(given_year) -1
            next_year =formatted_date[7:]
        print("next_year",next_year)

        query = f'''SELECT * FROM ems_v1.master_energy_calculations ORDER BY s_no '''
        result = await cnx.execute(query)
        result = result.fetchall()

        para = ''
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
        completed_db="ems_v1_completed."    
        table_name = ""
        where = ""
        if report_for == '6to6':
            for rows in result:
                para = rows['parameter']
                    
            if para == 'kw':
                para = "case when mmf.kw = '*' then p.t_watts * mmf.kw_value when  mmf.kw = '/' then p.t_watts / mmf.kw_value else p.t_watts end "

            if para == 'kWh':
                para = "case when mmf.kWh = '*' then p.kWh * mmf.kWh_value when  mmf.kWh = '/' then p.kWh / mmf.kWh_value else p.kWh end "
            
            month_year=f"""{mill_month[dates.month]}{str(dates.year)}"""
            table_name=f"  {completed_db}[power_{month_year}] as p"
            tblname = f'power_{month_year}'
            query1 = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND  TABLE_NAME = '{tblname}'"""
            print(query1)
        else:
            para = "case when mmf.kWh = '*' then p.kWh * mmf.kWh_value when  mmf.kWh = '/' then p.kWh / mmf.kWh_value else p.kWh end "
            
            month_year=f"""{mill_month[dates.month]}{str(dates.year)}"""
            table_name=f"  {completed_db}power_{month_year}_12 as p"
            tblname = f'power_{month_year}_12'
            query1 = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed'  AND TABLE_NAME = '{tblname}'"""
            print(query1)
        query1 = await cnx.execute(query1)
        query1 = query1.fetchall()
        where += f'''p.mill_date = '{dates}' '''   
       
        if len(query1)>0:     
            
            query2= f''' 
            SELECT 
                p.meter_id,
                sum({para}) as kWh,
                SUM(case when mmf.kWh = '*' then p.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then p.reverse_kwh / mmf.kWh_value else p.reverse_kwh end ) AS reverse_kwh,
                min(mm.meter_name) as meter_name
            from {table_name} 
                left join  ems_v1.master_meter mm on mm.meter_id=p.meter_id
                left join  ems_v1.master_meter_factor mmf on mm.meter_id=mmf.meter_id
            where 
                {where} 
            group by 
                p.meter_id 
            order by 
                p.meter_id '''
            createFolder("Log/","query for day "+str(query2))
            res = await cnx.execute(query2)
            res = res.fetchall()

            dict={}
            dict_r_d={}
            
            for row in res:
                dict[row['meter_id']] = row['kWh']
                dict_r_d[row['meter_id']] = row['reverse_kwh']
              
            query3= f''' 
            SELECT 
                p.meter_id,
                sum({para}) as total ,
                SUM(case when mmf.kWh = '*' then p.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then p.reverse_kwh / mmf.kWh_value else p.reverse_kwh end ) AS reverse_kwh,
                min(mm.meter_name) as meter_name
            from 
                {table_name}  
                left join  ems_v1.master_meter mm on mm.meter_id=p.meter_id
                left join  ems_v1.master_meter_factor mmf on mm.meter_id=mmf.meter_id
            group by 
                p.meter_id 
            order by 
                p.meter_id '''
            ress = await cnx.execute(query3)
            ress = ress.fetchall()
            createFolder("Log/","query for month "+str(query3))
            dict1={}
            dict_r_m = {}
            
            for row in ress:
                dict1[row['meter_id']] = row['total']
                dict_r_m[row['meter_id']] = row['reverse_kwh']   
        else:
            pass
        
        table_names = []
    
        for month in range(4, 13):
            month_year = f"{mill_month[month]}{year}"

            if report_for == "12to12":
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12' """
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()
            else:
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}' """
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()

            if len(result_query) > 0:
                table_names.append(f"ems_v1_completed.power_{month_year}")
        
        for month in range(1, 4):
            month_year = f"{mill_month[month]}{next_year}"

            if report_for == "12to12":
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12' """
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()
            else:
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()
            if len(result_query) > 0:        
                table_names.append(f"ems_v1_completed.power_{month_year}")

        if len(table_names)==0:
            return _getErrorResponseJson("table not available")
        if report_for =="12to12":
            type = f"_12 p"
        else:
            type = f" p"
        
        union_query = " UNION ALL ".join([f"SELECT p.meter_id, {para} as data, case when mmf.kWh = '*' then p.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then p.reverse_kwh / mmf.kWh_value else p.reverse_kwh end  AS reverse_kwh, mm.meter_name FROM {table_name}{type}  left join  ems_v1.master_meter] mm on mm.meter_id=p.meter_id left join  ems_v1.master_meter_factor] mmf on mm.meter_id=mmf.meter_id" for table_name in table_names])

        query4 = f"""
                SELECT 
                    pp.meter_id, 
                    SUM(pp.data) as total_kwh ,
                    sum(pp.reverse_kwh) as reverse_kwh ,
                    min(pp.meter_name) as meter_name
                FROM 
                    ({union_query}) AS pp 
                    
                GROUP BY 
                    pp.meter_id 
                ORDER BY 
                    pp.meter_id"""
        createFolder("Log/","query for year "+str(query4))
        res_q4 = await cnx.execute(query4)
        res_q4 = res_q4.fetchall()
        
        dict2={}
        dict_r_y = {}
        
        for row in res_q4:
            dict2[row['meter_id']] = row['total_kwh']
            dict_r_y[row['meter_id']] = row['reverse_kwh']
                     
        rows_to_write = []

        for rows in result:
            func_name = rows['function_name']
            formula = rows['formula2']
            roundoff_value = rows['roundoff_value']
            
            if len(res) == 0:
                # day_resultss = 0
                formula_d = 0
            else:
                if func_name == 'Power Import':
                    # day_resultss = dict[14]
                    formula_d = dict[14]

                elif func_name == 'Power Export':
                    # day_resultss = dict_r_d[14]
                    formula_d = dict_r_d[14]
                else:
                    # day_resultss = eval(formula, {"dict": dict})
                    
                    numbers = re.findall(r'\[(\d+)\]', formula)
                    valid_ids = [int(num) for num in numbers if num.isdigit() and int(num) in dict]
                    numeric_formula = formula
                    for meter_id in valid_ids:
                        numeric_value = dict.get(meter_id, 0)  # Get the value from dict2 or use 0 if not found
                        numeric_formula = numeric_formula.replace(f'[{meter_id}]', str(numeric_value))
                    formula_d = re.sub(r'dict', '', numeric_formula)

            if len(ress) == 0:
                month_resultsss = 0
                formula_m = 0
            else:
                if func_name == 'Power Import':
                    # month_resultsss = dict1[14]
                    formula_m = dict1[14]
                elif func_name == 'Power Export':
                    # month_resultsss = dict_r_m[14]
                    formula_m = dict_r_m[14]
                else:
                    # month_resultsss = eval(formula, {"dict": dict1})
                    numbers = re.findall(r'\[(\d+)\]', formula)

                    valid_ids = [int(num) for num in numbers if num.isdigit() and int(num) in dict1]
                    numeric_formula = formula
                    for meter_id in valid_ids:
                        numeric_value = dict1.get(meter_id, 0)  # Get the value from dict2 or use 0 if not found
                        numeric_formula = numeric_formula.replace(f'[{meter_id}]', str(numeric_value))
                    formula_m = re.sub(r'dict', '', numeric_formula)

            if len(res_q4) == 0:
                year_resultsss = 0
                formula_y = 0
            else:
                if func_name == 'Power Import':
                    # year_resultsss = dict2[14]
                    formula_y = dict2[14]
                    print("formula_y",formula_y)
                elif func_name == 'Power Export':
                    # year_resultsss = dict_r_y[14]
                    formula_y = dict_r_y[14]

                else:
                    # year_resultsss = eval(formula, {"dict": dict2})
                    
                    numbers = re.findall(r'\[(\d+)\]', formula)

                    valid_ids = [int(num) for num in numbers if num.isdigit() and int(num) in dict2]
                    numeric_formula = formula
                    for meter_id in valid_ids:
                       
                        numeric_value = dict2.get(meter_id, 0)  # Get the value from dict2 or use 0 if not found
                        numeric_formula = numeric_formula.replace(f'[{meter_id}]', str(numeric_value))
                    formula_y = re.sub(r'dict', '', numeric_formula)

            rows_to_write.append({
                                    "func_name": func_name,
                                    'formula_d': formula_d,
                                    "formula_m": formula_m,
                                    "formula_y": formula_y,
                                    "roundoff_value":roundoff_value 
                                })
            # print("rows_to_write:", rows_to_write)
        
        return rows_to_write
    except Exception as e:
        return get_exception_response(e)
    
async def year_wise_report_print(cnx,campus_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,year,report_for,employee_id,kwh_type,request):

        groupby = ""
        where = ""
        result = ''
        output = {}
        

        if meter_id == "" or meter_id == 'all':
            pass
        else:
            meter_id = await id(meter_id)
            where += f" and mm.meter_id IN ({meter_id})"

        if  employee_id != '':
            query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
            res = await cnx.execute(query)
            res = res.fetchall()

            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]

        if company_id !='' and company_id !="0":
            where += f" and mm.company_id ={company_id}"

        if bu_id !='' and bu_id !="0":
            where += f" and mm.bu_id ={bu_id}"

        if campus_id !='' and campus_id !="0":
            where += f" and md.campus_id ={campus_id}"

        if plant_id !='' and plant_id !="0":
            where += f" and md.plant_id ={plant_id}"

        if plant_department_id !=''and plant_department_id != "0":
            where += f" and ms.plant_department_id ={plant_department_id}"
            
        if equipment_group_id !='' and equipment_group_id !="0":
            where += f" and mmt.equipment_group_id ={equipment_group_id}"

        mill_month = {1: "01", 2: "02", 3: "03", 4: "04", 5: "05", 6: "06",7: "07", 8: "08", 9: "09", 10: "10", 11: "11", 12: "12"}
        tables_to_union = []
        for month in range(4, 13):
            month_year = f"{mill_month[month]}{year}"
            print(month_year)
            if report_for == '12to12':
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12'"""
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()
                if len(result_query) > 0:
                    tables_to_union.append(f"select kwh, meter_id,mill_date,equipment_kwh from ems_v1_completed.power_{month_year}_12")
                print(month_year)
            else:
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()

                if len(result_query) > 0:
                    tables_to_union.append(f"select kwh, meter_id,mill_date,equipment_kwh from ems_v1_completed.power_{month_year}")
        
        next_year = int(year) + 1
        mill_month = {1: "01", 2: "02", 3: "03"}

        for month in range(1, 4):
            month_year = f"{mill_month[month]}{next_year}"
            print(month_year)
            if report_for == '12to12':
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12' """
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()

                print("result_query",result_query)
                if len(result_query) > 0:
                    tables_to_union.append(f"select kwh, meter_id,mill_date,equipment_kwh from ems_v1_completed.power_{month_year}_12")
                tables_union_query = " UNION ALL ".join(tables_to_union)
            else:   
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}' """
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()

                print("result_query",result_query)
                if len(result_query) > 0:
                    tables_to_union.append(f"select kwh, meter_id,mill_date,equipment_kwh from ems_v1_completed.power_{month_year}")
                tables_union_query = " UNION ALL ".join(tables_to_union)
                print("tables_union_query",tables_union_query)

        if len(tables_union_query)== []:
            return _getErrorResponseJson("table not available")
        
        query = text(f'''
            SELECT
                mm.meter_code AS meter_code,
                mm.meter_name AS meter_name,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value  when mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kwh,
                DATE_FORMAT(min(cp.mill_date), '%m-%Y') AS mill_date,
                ROUND(SUM(cp.equipment_kwh),prf.kWh) AS calculated_kwh
            FROM
                ({tables_union_query}) cp
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                inner JOIN ems_v1.master_plant_wise_department ms ON ms.plant_department_id = mm.plant_department_id                   
                inner JOIN ems_v1.master_plant md ON md.plant_id = mm.plant_id                   
                LEFT JOIN ems_v1.master_equipment_group mmt ON mmt.equipment_group_id = mm.equipment_group_id 
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id                                  
            WHERE
                1=1  and mm.status = 'active' {where}
            GROUP BY
                mm.meter_code,
                mm.meter_name,
                MONTH(cp.mill_date), 
                YEAR(cp.mill_date) 
            ORDER BY 
                min(cp.mill_date),  
                min(cp.meter_id)
        ''')

        print(query)
        rslt = await cnx.execute(query)
        rslt = rslt.fetchall()
        if len(rslt)>0:
          output = {}  # Initialize the output dictionary
        
        output_keys = [
            f"04-{year}", f"05-{year}", f"06-{year}",
            f"07-{year}", f"08-{year}", f"09-{year}",
            f"10-{year}", f"11-{year}", f"12-{year}",
            f"01-{next_year}", f"02-{next_year}", f"03-{next_year}"
        ]
        
        for row in rslt:
            meter_code = row['meter_code']
            meter_name = row['meter_name']
            mill_date = row['mill_date']
            # kwh = row['kwh']
            if kwh_type =='calculated':
                    kwh = row.calculated_kwh
            else:
                kwh = row.kwh
            
            if meter_code not in output:
                output[meter_code] = {
                    'meter_code': meter_code,
                    'meter_name': meter_name
                }
                for key in output_keys:
                    output[meter_code][key] = 0
            
            output[meter_code][mill_date] = kwh
        
        result = list(output.values())
               
        return result
    
 
async def year_report_print(cnx,year,report_type,request):


        where = ""
        type = ''
        mill_date = date.today()
        func_name = ''
        formula = ''
        meter_name = ''
        
        kwh = 0
        reverse_kwh = 0
        meter_id = 0
        query = text(f'''SELECT * FROM ems_v1.master_energy_calculations ORDER BY s_no ''')
        result = await cnx.execute(query)
        result = result.fetchall()
        if len(result)>0:

            query = text(f'''select mill_date from ems_v1.master_shifts''')
            shift=await cnx.execute(query)
            shift = shift.fetchall()
    
            for row in shift :
                mill_date = row['mill_date']  
                
            next_year = int(year) + 1
            mill_month = {1: "01", 2: "02", 3: "03", 4: "04", 5: "05", 6: "06",7: "07", 8: "08", 9: "09", 10: "10", 11: "11", 12: "12"}
            tables_to_union = []
            for month in range(4, 13):
                month_year = f"{mill_month[month]}{year}"

                if report_type == "12to12":
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12' """
                    result_query = await cnx.execute(query)
                    result_query = result_query.fetchall()
                else:
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}' """
                    result_query = await cnx.execute(query)
                    result_query = result_query.fetchall()
                if len(result_query) > 0:
                    tables_to_union.append(f"{month_year}")
            
            for month in range(1, 4):
                month_year = f"{mill_month[month]}{next_year}"

                if report_type == "12to12":
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12' """
                    result_query = await cnx.execute(query)
                    result_query = result_query.fetchall()
                else:
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}' """
                    result_query = await cnx.execute(query)
                    result_query = result_query.fetchall()
                if len(result_query) > 0:
                    tables_to_union.append(f"{month_year}")
            print("tables_to_union",tables_to_union)

            if len(tables_to_union)==0:
                 return _getErrorResponseJson("table not available")
            
            if report_type =="12to12":
                type = f"_12 cp"
            else:
                type = f" cp"

            result_dict= {}
            
            for table_name in tables_to_union:
                query = text(f'''
                SELECT
                    mm.meter_name AS meter_name,
                    min(mm.meter_id) meter_id ,
                    SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value  when mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ) AS kwh,
                    SUM(case when mmf.kWh = '*' then cp.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_kwh / mmf.kWh_value else cp.reverse_kwh end ) AS reverse_kwh,
                    FORMAT(min(cp.mill_date), 'dd-MM-yyyy') AS mill_date
                FROM
                    ems_v1_completed.power_{table_name}{type}
                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                    LEFT JOIN ems_v1.master_meter_factor mmf ON mmf.meter_id = mm.meter_id
                WHERE
                    1=1 {where}
                GROUP BY
                    mm.meter_name,
                    DAY(cp.mill_date) 
                ORDER BY 
                    min(cp.mill_date),  
                    min(cp.meter_id)
            ''')
                
                rslt = await cnx.execute(query)
                rslt = rslt.fetchall()
                result_dict[table_name] = rslt
            # if len(result_dict[table_name])==0:
            #     return JSONResponse({"iserror": False, "message": "no data available"})
            results = {}
            dict2 = {}
            dict2_r = {}
            roundoff_value_day = 1
            roundoff_value_month = 10
            roundoff_value_year = 1
            formula = 0
            roundoff_values={}

            for table_name, table_data in result_dict.items():
                table_dict = {}
                for row in table_data:
                    meter_id = row['meter_id']
                    mill_date = row['mill_date']
                    kwh = row['kwh']
                    reverse_kwh = row['reverse_kwh']
                
                    if table_name not in dict2:
                        dict2[table_name] = {}
                        dict2_r[table_name] = {}

                    if mill_date not in dict2[table_name]:
                        dict2[table_name][mill_date] = {}
                        dict2_r[table_name][mill_date] = {}

                    if meter_id not in dict2[table_name][mill_date]:
                        dict2[table_name][mill_date][meter_id] = kwh
                        dict2_r[table_name][mill_date][meter_id] = reverse_kwh
                    # print("dict2",dict2)
                    
                    for row in result:
                        func_name = row['function_name']
                        formula = row['formula2']
                        roundoff_value_month = row['roundoff_value']
                        if table_name not in results:
                            results[table_name] = {}

                        if mill_date not in results[table_name]:
                            results[table_name][mill_date] = {}
                        
                        if len(table_data) == 0:
                            formula = 0
                        else:
                            if func_name == 'Power Import':
                                if table_name in dict2 and mill_date in dict2[table_name]:
                                    formula = dict2[table_name][mill_date].get(14, 0)  
                                else:
                                    formula = 0

                            elif func_name == 'Power Export':
                                if table_name in dict2_r and mill_date in dict2_r[table_name]:
                                    formula = dict2_r[table_name][mill_date].get(14, 0)
                                else:
                                    formula = 0
                            else:
                                formula = re.sub(r'dict\[(\d+)\]', lambda match: str(dict2.get(table_name, {}).get(mill_date, {}).get(int(match.group(1)), 0)), formula)
                                formula = eval(formula)
                        results[table_name][mill_date][func_name] = formula
                        roundoff_values[(table_name, mill_date, func_name)] = roundoff_value_month # Add roundoff_value_month

            aggregated_results = {}
        
            for table_name, funcs in results.items():
                for mill_date, formula_result in funcs.items():
                    for func_name, value in formula_result.items():
                        if table_name not in aggregated_results:
                            aggregated_results[table_name] = {}

                        if func_name not in aggregated_results[table_name]:
                            aggregated_results[table_name][func_name] = {"formula": 0.00, "count": 0, "roundoff_value_month": 0}     

                        if aggregated_results[table_name][func_name]["formula"] == 0.00:
                            aggregated_results[table_name][func_name]["formula"] = value
                        else:
                            aggregated_results[table_name][func_name]["formula"] +=value
                        aggregated_results[table_name][func_name]["count"] += 1
                        roundoff_value_month = roundoff_values.get((table_name, mill_date, func_name), 0)
                        aggregated_results[table_name][func_name]["roundoff_value_month"] = roundoff_value_month

            createFolder("YearReport_Log/", "aggregated_results " + f'{aggregated_results}')
            meter_data = {"year_record": {}}

            for table_name, functions in aggregated_results.items():
                for func_name, values in functions.items():
                    kwh = values["formula"]
                    count = values["count"]
                    roundoff_value_month = values["roundoff_value_month"]  
                    
                    if func_name not in meter_data["year_record"]:
                        meter_data["year_record"][func_name] = {}

                    avg_kwh = kwh / count if count > 0 else kwh 
                    meter_data["year_record"][func_name][table_name] = {"roundoff_value_month": roundoff_value_month, "formulas": avg_kwh}
                
            current_month = list(result_dict.keys())[-1]
            meter_data["current_month_record"] = {}

            for row in result_dict[current_month]:
                meter_name = row['meter_name']
                mill_date = row['mill_date']
                kwh = row['kwh']
                reverse_kwh = row['reverse_kwh']

                for row in result:
                    func_name = row['function_name']
                    formula = row['formula2']
                    roundoff_value_day = row['roundoff_value']

                    if current_month not in results:
                        results[current_month] = {}

                    if mill_date not in results[current_month]:
                        results[current_month][mill_date] = {}
                    
                    if roundoff_value_day not in results[current_month][mill_date]:
                        results[current_month][mill_date][roundoff_value_day] = {}
                
                    if len(result_dict[current_month]) == 0:
                        formula_result = 0
                    else:
                        if func_name == 'Power Import':
                            if table_name in dict2 and mill_date in dict2[table_name]:
                                formula = dict2[table_name][mill_date].get(14, 0)
                            else:
                                formula = 0

                        elif func_name == 'Power Export':
                            if table_name in dict2_r and mill_date in dict2_r[table_name]:
                                formula = dict2_r[table_name][mill_date].get(14, 0)
                            else:
                                formula = 0
                        else:
                            formula = re.sub(r'dict\[(\d+)\]', lambda match: str(dict2.get(table_name, {}).get(mill_date, {}).get(int(match.group(1)), 0)), formula)
                            
                    if func_name not in meter_data["current_month_record"]:
                        meter_data["current_month_record"][func_name] = {}
                    
                    meter_data["current_month_record"][func_name][mill_date] = {
                "formula": formula,
                "roundoff_value_day": roundoff_value_day
            }
            para = "case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end "
            union_query = " UNION ALL ".join([f"SELECT cp.meter_id, {para} as data, (case when mmf.kWh = '*' then cp.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_kwh / mmf.kWh_value else cp.reverse_kwh end ) AS reverse_kwh, mm.meter_name FROM ems_v1_completed.power_{table_name}{type}  left join  ems_v1.master_meter mm on mm.meter_id=cp.meter_id left join  ems_v1.master_meter_factor mmf on mm.meter_id=mmf.meter_id" for table_name in tables_to_union])
            query4 = f"""
            SELECT 
                pp.meter_id, 
                SUM(pp.data) as total_kwh ,
                SUM(pp.reverse_kwh) AS reverse_kwh,
                min(pp.meter_name) as meter_name
            FROM 
                ({union_query}) AS pp 
                
            GROUP BY 
                pp.meter_id 
            ORDER BY 
                pp.meter_id"""
            createFolder("YearReport_Log/","query for year "+str(query4))
            res_q4 = await cnx.execute(query4)
            res_q4 = res_q4.fetchall()
            
            dict_y={}
            dict3 = {}
            dict_r_y = {}
            for row in res_q4:
                dict_y[row['meter_id']] = row['total_kwh']
                dict_r_y[row['meter_id']] = row['reverse_kwh']

            for row in result:
                func_name = row['function_name']
                formula = row['formula2']
                roundoff_value_year = row['roundoff_value']
                if len(res_q4) == 0:
                    formula_y = 0
                else:
                    if func_name == 'Power Import':
                        formula_y = dict_y[14]

                    elif func_name == 'Power Export':
                        formula_y = dict_r_y[14]
                    else:
                        numbers = re.findall(r'\[(\d+)\]', formula)
                        valid_ids = [int(num) for num in numbers if num.isdigit() and int(num) in dict_y]
                        numeric_formula = formula
                        for meter_id in valid_ids:
                            numeric_value = dict_y.get(meter_id, 0)  
                            numeric_formula = numeric_formula.replace(f'[{meter_id}]', str(numeric_value))
                        formula_y = re.sub(r'dict', '', numeric_formula)
                        
                # dict3[func_name] = formula_y
                dict3[func_name] = {
                    'formula_y': formula_y,
                    'roundoff_value_year': roundoff_value_year
                }
            createFolder("YearReport_Log/", "meter_data" + f"{meter_data}")
            response_data = {}
            response_data = {
                "meter_data": meter_data,
                "dict3": dict3,
                "res_q4": res_q4,
                "year": year,
                "next_year": next_year,
                "mill_date": mill_date,
                "report_type":report_type
            }
        
        return response_data
    
async def holiday_report(cnx,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,equipment_id,holiday_type,holiday_year,groupby,limit_report_for,limit_exception_for,limit_order_by,limit_operation_value,is_critical,is_month_wise):
       
        subquery_union = ''
        tables_to_union = ''
        where = ''

        if company_id != '' and company_id != 'all' and company_id != None:
            where += f" and mm.company_id = {company_id}"

        if bu_id != '' and bu_id != 'all' and bu_id != None:
            where += f" and mm.bu_id = {bu_id}"

        if plant_id != '' and plant_id != 'all' and plant_id != None:
            where += f" and mm.plant_id = {plant_id}"

        if plant_department_id != '' and plant_department_id != 'all' and plant_department_id != None:
            where += f" and mm.plant_department_id = {plant_department_id}"

        if equipment_group_id != '' and equipment_group_id != 'all' and equipment_group_id != None:
            where += f" and mm.equipment_group_id = {equipment_group_id}"

        if meter_id != '' and meter_id != 'all' and meter_id != None:
            where += f" and mm.meter_id = {meter_id}"

        if equipment_id != '' and equipment_id != 'all' and equipment_id != None:
            where += f" and me.equipment_id = {equipment_id}"
            
        if holiday_type != '' and holiday_type != None and holiday_type != 'all':
            where += f" and mhd.holiday_type = '{holiday_type}'"

        mill_month = {1: "01", 2: "02", 3: "03", 4: "04", 5: "05", 6: "06",7: "07", 8: "08", 9: "09", 10: "10", 11: "11", 12: "12"}
        tables_to_union = []

        for month in range(1, 13):
            month_year = f"{mill_month[month]}{holiday_year}"
            # print(month_year)
            query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}' """
            result_query = await cnx.execute(query)
            result_query = result_query.fetchall()

            if len(result_query) > 0:
                tables_to_union.append(f"select mill_date, mill_shift, machine_kWh, master_kwh, kWh, meter_id from ems_v1_completed.power_{month_year}")
                 
        if len(tables_to_union) == 0:
            return _getErrorResponseJson("power table not available...")    
        subquery_union = " UNION ALL ".join(tables_to_union)

        group_by = ''
        order_by = ''

        if limit_report_for == 'exception':
            if limit_exception_for!='':
                order_by += f'''sum(cp.{limit_exception_for}) '''

            if limit_order_by != '':
                order_by += f'''{limit_order_by}, ''' 

        if groupby == 'plant':
            group_by += f'md.plant_id'
            order_by += f'md.plant_order'

        if groupby == 'plant_department':
            group_by += f' ms.plant_department_id'
            order_by += f' ms.plant_department_order'

        if groupby == 'equipment_group':
            group_by += f'  mmt.equipment_group_id'
            order_by += f' mmt.equipment_group_order'

        if groupby == 'meter':
            group_by += f' mm.meter_id'
            order_by += f' mm.meter_order'

        if groupby == 'equipment':
            group_by += f' me.equipment_id'
            order_by += f' me.equipment_order'
            where +=f" and me.status = 'active'"

        if limit_operation_value != '':
            order_by += f''' LIMIT {limit_operation_value}'''

        if is_critical != '':
           where += f" and mm.major_nonmajor = '{is_critical}'"
        
        if is_month_wise == 'yes':
            group_by = " MONTH(cp.mill_date), YEAR(cp.mill_date), " + group_by
            order_by = " MONTH(cp.mill_date), YEAR(cp.mill_date), " + order_by  
                       
        if group_by != "":
            group_by = f"group by {group_by} "    
        if order_by != "":
            order_by = f"order by {order_by}"

        if order_by != "":
            order_by = f"{order_by}"

        query = text(f'''
                select 
                    cp.mill_date,
                    cp.mill_shift,
                    DATE_FORMAT(cp.mill_date, '%M') AS month,
                    mc.company_code,
                    mc.company_name,
                    bu.bu_code,
                    bu.bu_name,
                    md.plant_code,
                    md.plant_name,
                    ms.plant_department_code,
                    ms.plant_department_name,
                    mmt.equipment_group_code,
                    mmt.equipment_group_name,
                    mm.meter_code,
                    mm.meter_name,
                    me.equipment_code,
                    me.equipment_name,
                    mm.meter_id,
                    mm.company_id,
                    mm.bu_id,
                    mm.plant_id,
                    mm.plant_department_id,
                    mm.equipment_group_id,
                    mhm.equipment_id,
                    ROUND(sum(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end) ,prf.machine_kWh) AS end_kwh,
                    ROUND(sum(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ),prf.machine_kWh) AS start_kwh,
                    ROUND(sum(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kWh,
                    mhd.description,
                    mhd.holiday_type,
                    mhd.Weekend_day,
                    mh.id,
                    '' tooltip_start_kwh,
                    '' tooltip_end_kwh,
                    '' tooltip_kwh,
                    '' formula 
                    from
                        ({subquery_union}) as cp
                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                    INNER JOIN ems_v1.master_company mc ON mc.company_id = mm.company_id
                    INNER JOIN ems_v1.master_business_unit bu ON bu.bu_id = mm.bu_id
                    INNER JOIN ems_v1.master_plant md ON md.plant_id = mm.plant_id
                    INNER JOIN ems_v1.master_plant_wise_department ms ON ms.plant_department_id = mm.plant_department_id
                    LEFT JOIN ems_v1.master_equipment_group mmt ON mmt.equipment_group_id = mm.equipment_group_id 
                    left join ems_v1.master_equipment_meter mem on mem.meter_id = mm.meter_id
                    left join ems_v1.master_equipment me on me.equipment_id = mem.equipment_id
                    INNER JOIN ems_v1.master_holiday_meter mhm ON mhm.equipment_id = me.equipment_id 
                    INNER JOIN ems_v1.master_holiday_date mhd ON mhd.holiday_date = cp.mill_date 
                    INNER JOIN ems_v1.master_holiday mh ON mh.id = mhm.ref_id and mh.id = mhd.ref_id and mh.status = 'active'
                    INNER JOIN ems_v1.master_meter_factor mmf ON mm.meter_id = mmf.meter_id   
                    INNER JOIN ems_v1.master_parameter_roundoff prf ON mm.plant_id = prf.plant_id  
                    where mh.status = 'active' and mm.status = 'active'  and mh.holiday_year = {holiday_year} 
                    {where}
                    {group_by}
                    {order_by}
                    ''')
        createFolder("Holiday_Log/", "holiday_reort" + str(query))
        data = await cnx.execute(query)
        data = data.fetchall()
                
        return data

async def alarmreport(cnx,request,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,report_for,period_id,from_date,to_date,shift_id, employee_id  ):

    try:
        # client_ip = request.client.host 
        # print(client_ip)
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
        data2 = ''
        join = ''

        if  employee_id != '':
            query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
            res = await cnx.execute(query)
            res = res.fetchall()
            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]
        
        start_time = date.today()
       
        where = ""  
        data1 = await shift_Lists(cnx, '',plant_id, bu_id, company_id)
        mill_date = date.today()
        mill_shift = 0        

        if len(data1) > 0:
           for shift_record in data1:
              mill_date = shift_record["mill_date"]
              mill_shift = shift_record["mill_shift"]            
       
        if period_id == "cur_shift":          
            join = f"present_alarm pa"
            where += f'''  and pa.mill_date = '{mill_date}' AND pa.mill_shift = '{mill_shift}' ''' 
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) '''
            
        elif period_id == "sel_shift":            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            result_a=await check_alarm_tble(cnx,month_year)
            if len(result_a) > 0: 
                join = f"ems_v1_completed.alarm_{month_year} pa"

            where += f'''  and pa.mill_date = '{from_date}' AND pa.mill_shift = '{shift_id}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 
        
        elif period_id == "#previous_shift":   
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            join=f" ems_v1_completed.alarm_{month_year} as pa" 
        
            where += f''' and pa.mill_date = '{from_date}' AND pa.mill_shift = '{shift_id}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id == "sel_date":            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            result_a=await check_alarm_tble(cnx,month_year)

            if len(result_a) > 0: 
                join = f"ems_v1_completed.alarm_{month_year} pa"
            where += f'''and pa.mill_date = '{from_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time)'''
        
        elif period_id == "#previous_day":             
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            join=f"  ems_v1_completed.alarm_{month_year} as pa "
            where += f''' and pa.mill_date = '{from_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id  == "#this_week":
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id == "#previous_week":
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id == "#this_month":
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id == "#previous_month":
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id=="#this_year": 
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id=="#previous_year": 
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) ''' 

        elif period_id == "from_to":            
                    
            where += f''' and pa.mill_date  >= '{from_date}' and pa.mill_date <= '{to_date}' '''
            duration = f'''TIMESTAMPDIFF(SECOND, pa.start_time, pa.stop_time) '''
            # if shift_id != '':                
            #     where += f''' and pa.mill_shift = '{shift_id}' ''' 
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
                       
        elif period_id == "live_alarm":
            join = f"present_alarm pa"
            where += f''' and pa.start_time <> '1900-01-01 00:00:00'  and pa.stop_time is Null '''  
            sql = text(f'''SELECT  start_time FROM ems_v1.present_alarm ORDER BY start_time DESC limit 1''')

            data = await cnx.execute(sql)
            data = data.fetchall()
            duration = f'''TIMESTAMPDIFF(second, pa.start_time, now())'''
            for i in data:
                start_time = i['start_time']
            # print(start_time)

            sql1= text(f''' UPDATE ems_v1.master_company
            SET alarm_status = CASE WHEN alarm_last_time = '{start_time}' THEN alarm_status ELSE 1 END,
                alarm_last_time = '{start_time}'
            WHERE company_id = '{company_id}'
        ''')

            await cnx.execute(sql1)
            await cnx.commit()
            query2=text(f'''select * from ems_v1.master_company where company_id = '{company_id}' and alarm_status = 1''')
            data2 = await cnx.execute(query2)
            data2 = data2.fetchall() 

        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or period_id == "#sel_year":
            if from_date != '' and to_date != '':
                if from_date.month == to_date.month and from_date.year == to_date.year:
                    month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                    
                    result_query = await check_alarm_tble(cnx,month_year)
                    if len(result_query) > 0: 
                        join = f'''ems_v1_completed.alarm_{month_year} pa'''
        
                else:
                    month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                    
                    joins = []

                    for month_year in month_year_range:
                        result_p = await check_alarm_tble(cnx,month_year)
                        if len(result_p) > 0:
                            join_p = f"ems_v1_completed.alarm_{month_year} "
                            joins.append(f"select start_time,stop_time,description,parameter_name,mill_date,mill_shift,alarm_target_id,meter_id from {join_p}")
                    if len(joins)>0:
                        join = " UNION ALL ".join(joins)
                        join = f"( {join}) pa"    
                        
        if meter_id != '' and meter_id != 'all':
            where += f" and mm.meter_id = '{meter_id}' "   

        if company_id !='' and company_id !="0" and company_id != 'all' and company_id != None:
            where += f" and mc.company_id = '{company_id}' "
        
        if plant_id !='' and plant_id !="0" and plant_id != 'all' and plant_id != None:
            where += f" and md.plant_id = '{plant_id}' " 

        if plant_department_id !='' and  plant_department_id !="0" and plant_department_id != 'all' and plant_department_id != None:
            where += f" and ms.plant_department_id = '{plant_department_id}' "   

        if equipment_group_id !='' and equipment_group_id!="0" and equipment_group_id != 'all' and equipment_group_id != None:
            where += f" and mmt.equipment_group_id = '{equipment_group_id}' " 

        groupby = "" 
        
        # if report_for == "detail" or report_for == '':
        #     groupby = f'''group by pa.meter_id, ma.alarm_name, pa.mill_date, pa.mill_shift, pa.start_time''' 

        if report_for == 'summary':
            groupby = f'''group by ma.alarm_name'''
        
        if report_for == "summary":
            sql = text(f'''
                SELECT 
                    ma.alarm_name,		    
                    pa.parameter_name,
                    mm.meter_name,
                    sum({duration}) as duration,
                    pa.mill_date,
                    pa.mill_shift,
                    pa.meter_id
                ''')
        else:
            sql = text(f'''
                SELECT 
                    mm.meter_code,
                    mm.meter_name,
                    ma.alarm_name,
                    ma.parameter_name,
                    pa.start_time,
                    pa.stop_time,
                    {duration} as duration,
                    pa.description,
                    pa.mill_date,
                    pa.mill_shift,
                    pa.meter_id
                ''')
           
        query = text(f''' 
            {sql}
            FROM 
                ems_v1.master_alarm_target ma 
                INNER JOIN {join} on pa.alarm_target_id = ma.alarm_target_id
                INNER JOIN ems_v1.master_meter mm on pa.meter_id = mm.meter_id
                LEFT JOIN ems_v1.master_company mc on mc.company_id=ma.company_id
                LEFT JOIN ems_v1.master_business_unit mb on mb.bu_id=ma.bu_id
                LEFT JOIN ems_v1.master_plant md on md.plant_id=ma.plant_id
                LEFT JOIN ems_v1.master_plant_wise_department ms on ms.plant_department_id=ma.plant_department_id
                LEFT JOIN ems_v1.master_equipment_group mmt on mmt.equipment_group_id=ma.equipment_group_id
            WHERE 1=1 {where} {groupby}
         ''')
        
        print(query)
        data = await cnx.execute(query)
        data = data.fetchall() 
        result = {"data":data,"data1":data2}              
        return result
    except Exception as e:
        return get_exception_response(e)
    
async def hour_wise_analysis_report(cnx,campus_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,employee_id,meter_id,from_date,meter_type,kwh,month_year,from_time,to_time,request,a_shift_start_time):

        where = ""
        rslt = ''
        kwh_type = kwh
        if meter_id == "" or meter_id == 'all':
            pass
        else:
            meter_id = await id(meter_id)
            where += f" and mm.meter_id IN ({meter_id})"

        if  employee_id != '':
            query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
            res = await cnx.execute(query)
            res = res.fetchall()       
    
            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]

        if company_id !='' and company_id !=0 and company_id != 'all':
            where += f" and mm.company_id ={company_id}"

        if bu_id !='' and bu_id !=0 and bu_id != 'all':
            where += f" and mm.bu_id ={bu_id}"

        if plant_id !='' and plant_id !=0 and plant_id != 'all':
            where += f" and md.plant_id ={plant_id}"

        if campus_id !='' and campus_id !=0 and campus_id != 'all':
            where += f" and md.campus_id ={campus_id}"

        if plant_department_id !='' and plant_department_id != 0 and plant_department_id != 'all':
            where += f" and ms.plant_department_id ={plant_department_id}"

        if equipment_group_id !='' and equipment_group_id!= 0 and equipment_group_id != 'all':
            where += f" and mmt.equipment_group_id ={equipment_group_id}"

        if meter_type!= '':
            where += f" and mm.meter_type ='{meter_type}'"

        if from_time !='':
            where += f" and DATE_FORMAT(cp.created_on ,'%H:%i:%s')>='{from_time}' "
    
        if to_time !='':
            where += f" and DATE_FORMAT(cp.created_on ,'%H:%i:%s')<='{to_time}' " 
        
        if a_shift_start_time != '':
            next_date = from_date + timedelta(days=1)
            next_date = next_date.strftime("%Y-%m-%d") 
            where +=f" and DATE_FORMAT(cp.created_on ,'%Y-%m-%d %H:%i:%s')<'{next_date} {a_shift_start_time}:00'"
        query=text(f'''
            SELECT
                mm.meter_id,
                mm.meter_code,
                mm.meter_name,
                MIN(cp.created_on) AS start_time,
                MAX(cp.created_on) AS end_time,
                mm.meter_type,
                md.plant_name,
                HOUR(cp.created_on) as first_hour,
                CONCAT('h', HOUR(cp.created_on)) AS hour,
                ROUND(MIN(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS master_kwh,
                ROUND(MAX(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS machine_kwh,
                ROUND(Max(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) - ROUND(Min(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS kwh,
                ROUND(SUM(cp.diff_equipment_on_load_kwh+cp.diff_equipment_off_kwh +cp.diff_equipment_idle_kwh)) as calculated_kwh
            FROM
                ems_v1_completed.power_analysis_{month_year} cp
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                inner JOIN ems_v1.master_plant_wise_department ms ON ms.plant_department_id = mm.plant_department_id                   
                inner JOIN ems_v1.master_plant md ON md.plant_id = mm.plant_id                   
                inner JOIN ems_v1.master_campus c ON c.campus_id = md.campus_id                   
                LEFT JOIN ems_v1.master_equipment_group mmt ON mmt.equipment_group_id = mm.equipment_group_id 
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id                   
            WHERE
                1=1  and mm.status = 'active'{where} and cp.mill_date  = '{from_date}' 
            GROUP BY
                mm.meter_id,
                cp.mill_date,
                DAY(cp.created_on),
                hour(cp.created_on)
            order by cp.created_on
          ''') 
        rslt = await cnx.execute(query)
        rslt = rslt.fetchall()
        createFolder("Log/","result"+str(rslt))
        first_hour = ''
        if rslt !='':
            hourly_data = defaultdict(dict)
            # Iterate through JSON data to populate hourly kWh consumption
            for entry in rslt:
                meter_id = entry["meter_id"]
                meter_code = entry["meter_code"]
                hour_key = entry["hour"]
                kwh = entry["kwh"]
                calculated_kwh = entry["calculated_kwh"]
                machine_kwh = entry["machine_kwh"]
                master_kwh = entry["master_kwh"]
                hourly_data[meter_code].setdefault("meter_code", meter_code)
                hourly_data[meter_code].setdefault("meter_name", entry["meter_name"])
                hourly_data[meter_code].setdefault("meter_type", entry["meter_type"])
                hourly_data[meter_code].setdefault("plant_name", entry["plant_name"])
                if kwh_type == 'Calculated':
                    hourly_data[meter_code][hour_key] = calculated_kwh
                else:
                    hourly_data[meter_code][hour_key] = kwh
                hourly_data[meter_code][f"machine_kwh_{hour_key}"] = machine_kwh
                hourly_data[meter_code][f"master_kwh_{hour_key}"] = master_kwh
            hourly_data = list(hourly_data.values())
            for row in rslt:
                first_hour = row["first_hour"]
                break
        else:
            rslt = []
            first_hour = 0
        hourly_data = {"hourly_data":hourly_data,"start_time":first_hour}
        return hourly_data
   
    
async def import_export_dtl(cnx,company_id,bu_id,meter_id,period_id,from_date,to_date,shift_id,report_for,employee_id):
    
        meter_id = id(meter_id)
        
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}       
        
        where = "" 
        group_by = ''
        order_by = ''
        plant_id = ''
        plant_department_id = ''
        equipment_group_id = ''
        if meter_id == '':
            pass
        else:
            where += f" and mm.meter_id in ({meter_id})"
        if  employee_id != '':
            query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
            res = await cnx.execute(query)
            res = res.fetchall()

            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]
                    
        if plant_id !='' and plant_id !=0:
            where += f" and md.plant_id ={plant_id}"
        if plant_department_id !='' and plant_department_id != 0:
            where += f" and ms.plant_department_id ={plant_department_id}"
        if equipment_group_id !='' and equipment_group_id!= 0:
            where += f" and mmt.equipment_group_id ={equipment_group_id}"

        data1 = await shift_Lists(cnx, '',plant_id, bu_id, company_id)
        mill_date = date.today()
        mill_shift = 0
        table_name = ''
        
        if len(data1) > 0:
           for shift_record in data1:
              mill_date = shift_record["mill_date"]
              mill_shift = shift_record["mill_shift"]  

        field_name_import = '''mill_date, mill_shift, meter_id, master_kwh as start_kwh, machine_kWh as end_kwh, kwh, 'Import' as kwh_type '''
        field_name_export = '''mill_date, mill_shift, meter_id,reverse_master_kwh as start_kwh, reverse_machine_kWh as end_kwh, reverse_kwh as kwh, 'Export' as kwh_type'''       

        if period_id == 'cur_shift':            
            table_name = f'(select {field_name_import} from ems_v1.current_power UNION All select {field_name_export} from ems_v1.current_power)cp'
            where += f" and cp.mill_date = '{mill_date}' and cp.mill_shift ='{mill_shift}' "

        elif period_id == 'sel_shift' or period_id == 'sel_date':
                   
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"ems_v1_completed.power_{month_year}" 
            where += f" and cp.mill_date = '{mill_date}' "

            table_name = f'(select {field_name_import} from {table_name} UNION All select {field_name_export} from {table_name})cp'

            if period_id == 'sel_shift':
                where += f" and cp.mill_shift ='{shift_id}' " 
   
        elif period_id == "from_to":             
                    
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
            where += f''' and  cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            
            if shift_id != "":                
                where += f''' and cp.mill_shift = '{shift_id}' ''' 
            
            if from_date.month == to_date.month:
                query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                result_query = await cnx.execute(query)
                result_query = result_query.fetchall()
                
                if len(result_query) == 0:
                    return _getErrorResponseJson("analysis table not available...")   
                     
                table_name=f"ems_v1_completed.power_{month_year}" 
                table_name = f'(select {field_name_import} from {table_name} UNION All select {field_name_export} from {table_name})cp'
            else:
                
                month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                union_queries_export = []
                union_queries_import = []

                for month_year in month_year_range:
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                    result_query = await cnx.execute(query)
                    result_query = result_query.fetchall()

                    print(query)
                    if len(result_query) > 0:
                        table_name = f"ems_v1_completed.power_{month_year}"
                        union_queries_export.append(f"SELECT {field_name_export} FROM {table_name}")
                        union_queries_import.append(f"SELECT {field_name_import} FROM {table_name}")

                subquery_union_import = " UNION ALL ".join(union_queries_import)
                subquery_union_export = " UNION ALL ".join(union_queries_export)
                table_name = f"({subquery_union_import} union all {subquery_union_export}) cp"
    
        if report_for == 'detail' or report_for == '':
            group_by = " ,cp.mill_date, cp.mill_shift"
            order_by = " ,cp.mill_date, cp.mill_shift" 
        
        if report_for == 'summary':
            group_by = " ,cp.mill_date" 
            order_by = " ,cp.mill_date"   
                       
        if group_by != "":
            group_by = f"{group_by} "    
        if order_by != "":
            order_by = f"{order_by}"

        query = text(f'''
                select 
                    min(cp.mill_date) mill_date,
                    min(cp.mill_shift) mill_shift,
                    min(cp.meter_id) meter_id,
                    min(mm.meter_name) meter_name,
                    min(mm.meter_code) meter_code,
                    cp.kwh_type,
                    ROUND(SUM(
                        case when cp.kwh_type = 'Export' then
                            case when mmf.reverse_machine_kWh = '*' then cp.start_kwh * mmf.reverse_machine_kWh_value when  mmf.reverse_machine_kWh = '/' then cp.start_kwh / mmf.reverse_machine_kWh_value else cp.start_kwh end 
                        else
                            case when mmf.machine_kWh = '*' then cp.start_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.start_kwh / mmf.machine_kWh_value else cp.start_kwh end 
                        end
                     ),2) AS start_kwh,
                    ROUND(SUM(
                        case when cp.kwh_type = 'Export' then
                            case when mmf.reverse_machine_kWh = '*' then cp.end_kwh * mmf.reverse_machine_kWh_value when  mmf.reverse_machine_kWh = '/' then cp.end_kwh / mmf.reverse_machine_kWh_value else cp.end_kwh end
                        else
                            case when mmf.machine_kWh = '*' then cp.end_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.end_kwh / mmf.machine_kWh_value else cp.end_kwh end
                        end
                     ),2) AS end_kwh,
                    ROUND(SUM(
                        case when cp.kwh_type = 'Export' then
                            case when mmf.reverse_kwh = '*' then cp.kWh * mmf.reverse_kwh_value when  mmf.reverse_kwh = '/' then cp.kWh / mmf.reverse_kwh_value else cp.kWh end 
                        else
                            case when mmf.kwh = '*' then cp.kWh * mmf.kwh_value when  mmf.kwh = '/' then cp.kWh / mmf.kwh_value else cp.kWh end 
                        end
                     ),2) AS kwh
                     
                from
                    {table_name}
                    INNER JOIN master_meter mm ON cp.meter_id = mm.meter_id and mm.import_export = 'yes'
                    left JOIN master_meter_factor mmf ON mm.meter_id = mmf.meter_id  
                    LEFT JOIN ems_v1.master_plant_wise_department ms ON ms.plant_department_id = mm.plant_department_id                   
                    LEFT JOIN ems_v1.master_plant md ON md.plant_id = mm.plant_id                   
                    LEFT JOIN ems_v1.master_equipment_group mmt ON mmt.equipment_group_id = mm.equipment_group_id 
                    where 1=1 
                    {where}
                    group by mm.meter_id {group_by}, cp.kwh_type
                    order by mm.meter_id {order_by}
                    ''')
   
        createFolder("Log/","current_power_ie api query "+str(query))

        data = await cnx.execute(query)
        data = data.fetchall()
        return data
      
async def get_hour_wise_report_model(cnx,plant_id,meter_id,period_id,from_date,to_date,shift_id,from_time,to_time):


        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
        
        where = "" 
        if plant_id != '':
            where += f" and mm.plant_id = {plant_id}"  

        if period_id == 'cur_shift': 
            # query=text(f'''SELECT * FROM ems_v1.master_shifts WHERE status='active' and  plant_id = {plant_id} ''')
            data1 = await shift_Lists(cnx, '',plant_id, '', '')
            mill_date = date.today()
            mill_shift = 0       
            if len(data1) > 0:
                for shift_record in data1:
                    mill_date = shift_record["mill_date"]
                    mill_shift = shift_record["mill_shift"]  
                        
            table_name = 'ems_v1.current_power_analysis cp'
            where += f" and cp.mill_date = '{mill_date}' and cp.mill_shift ='{mill_shift}' "

        elif period_id == 'sel_shift' or period_id == 'sel_date':
                      
            month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
            table_name=f"ems_v1_completed.power_analysis_{month_year}" 
            where += f" and cp.mill_date = '{mill_date}' "

            field_name = 'id,meter_id, created_on, mill_date, mill_shift, kwh, master_kwh, machine_kWh'
            table_name = f'(select {field_name} from ems_v1.current_power_analysis UNION All select {field_name} from {table_name})cp'

            if period_id == 'sel_shift':
                
                where += f" and cp.mill_shift ='{shift_id}' " 
   
        elif period_id == "from_to":            
   
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
        
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            
            if shift_id != "":                
                where += f''' and cp.mill_shift = '{shift_id}' ''' 
            field_name = 'id,meter_id, created_on, mill_date, mill_shift, kwh, master_kwh, machine_kWh'
            
            if from_date.month == to_date.month:
                table_name=f"ems_v1_completed.power_analysis_{month_year}" 
                table_name = f'(select {field_name} from ems_v1.current_power_analysis UNION All select {field_name} from {table_name})cp'
            else:
                month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                union_queries = []

                for month_year in month_year_range:
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_analysis_{month_year}'"""
                    result_query = await cnx.execute(query)
                    result_query = result_query.fetchall()
                    print(query)
                    if len(result_query) > 0:
                        table_name = f"ems_v1_completed.power_analysis_{month_year}"
                        union_queries.append(f"SELECT {field_name} FROM {table_name}")

                if len(union_queries) == 0:
                    createFolder("Log_ct/","analysis table not available...")  
                    return _getErrorResponseJson("analysis table not available...") 
                    
                subquery_union = " UNION ALL ".join(union_queries)
                table_name = f"(SELECT {field_name} FROM ems_v1.current_power_analysis UNION ALL {subquery_union}) cp"
        
        if from_time !='':
            where += f" and DATE_FORMAT(cp.created_on ,'%H:%i')>='{from_time}' "
    
        if to_time !='':
            where += f" and DATE_FORMAT(cp.created_on ,'%H:%i')<='{to_time}' "

        query=text(f'''
            SELECT
                mm.meter_id,
                mm.meter_code,
                mm.meter_name,
                mm.plant_id,
                EXTRACT(HOUR FROM cp.created_on) AS hour_of_day,
                cp.created_on AS date_time,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS end_kwh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ),prf.machine_kWh) AS start_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kWh     
            FROM
                {table_name}
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                INNER JOIN ems_v1.master_meter_factor mmf ON mm.meter_id = mmf.meter_id and mmf.plant_id = mm.plant_id
                INNER JOIN ems_v1.master_parameter_roundoff prf ON mm.plant_id = prf.plant_id                   
            WHERE 
                mm.meter_id in ({meter_id}) 
                {where}
            group by 
                mm.meter_id, 
                EXTRACT(DAY FROM cp.created_on),
                EXTRACT(HOUR FROM cp.created_on)
            ORDER BY
                mm.meter_id,
                EXTRACT(DAY FROM cp.created_on),
                EXTRACT(HOUR FROM cp.created_on);
          ''')  
     
        data = await cnx.execute(query)
        data = data.fetchall()
        return data
    
async def get_equipment_cal_dtl(cnx):
    sql = f'''select * from master_equipment_calculations where status = 'active' '''
    result = await cnx.execute(sql)
    result = result.fetchall()
    return result

import json
async def gateway_log(cnx,mac,from_date,from_time,to_time):
    where = ''
    dates = date.today()
    

    if from_date != '':
        where += f" where DATE_FORMAT(date_time,'%d-%m-%Y') = '{from_date}'"
        print(where)
    if mac != '':
        where +=f" and mac = '{mac}'"
    if from_time != '':
        where += f" and TIME(date_time)>='{from_time}'"
    if to_time != '':
        where += f" and TIME(date_time)<='{to_time}'"
    res = []
    
    if from_date[:11] == dates:
        sql = f" select * from gateway.log {where}"
    else:
        from_date = await parse_date(from_date)
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        sql = f" select * from gateway_completed.log_{month_year} {where}"
    print(sql)
    
    result = await cnx.execute(sql)
    result = result.mappings().all()
    slave_id = 0
    is_error = 0
    print(result)
    for row in result:
        received_packet = row['received_packet'].replace("'", "\"")
        received_packet_data = json.loads(received_packet)
        data = received_packet_data['data']
        try:
            modbus_data = data['modbus']
            print(modbus_data)
            mac = row["mac"]
            date_time = row["date_time"]
            kwh = 0
            for data in modbus_data:
                slave_id = int(data['sid'])
                try:
                    kwh = int(data['kwh'])
                except:
                    kwh = 0
                print(slave_id)
                is_error = int(data['stat']) 
                packet = {'slave_id': slave_id, 'is_error': is_error,'data':f'{data}','mac':mac,'date_time':date_time,'kwh':kwh}
                
                res.append(packet)
        except Exception as e:
            print("packet",res)
    

    return res

async def route_card_kwh(cnx,equipment_id,from_date,to_date,groupby):
    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
    
    where = ''
   
    if from_date != '':
        where += f" and cp.created_on  >= '{from_date}'"

    if equipment_id != '':
        where +=f" and mm.equipment_id = {equipment_id}"

    if to_date != '':
        where += f" and cp.created_on <='{to_date}'"
    
    
    table = f"current_power_analysis "
    
    month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""   
    print(month_year)   
    
        
    fields = "meter_id,mill_date,mill_shift,created_on,machine_kwh,on_load_time,idle_time,off_time,diff_equipment_on_load_kwh,diff_equipment_off_kwh,diff_equipment_idle_kwh"
    data = await check_analysis_table(cnx,month_year)
    if len(data)>0:
        fields = "meter_id,mill_date,mill_shift,created_on,machine_kwh,on_load_time,idle_time,off_time,diff_equipment_on_load_kwh,diff_equipment_off_kwh,diff_equipment_idle_kwh"
        table = f" (select {fields} from current_power_analysis union all select {fields} from ems_v1_completed.power_analysis_{month_year})"
    
    if from_date.month != to_date.month :
        month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                    
        union_queries = []
        for month_year in month_year_range:
            # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
            result_query = await check_analysis_table(cnx,month_year)

            if len(result_query) > 0:
                table_name = f"ems_v1_completed.power_analysis_{month_year}"
                union_queries.append(f"SELECT {fields} FROM {table_name}")

        subquery_union = " UNION ALL ".join(union_queries)
        
        table = f"( select {fields} from current_power_analysis union all {subquery_union}) "
                    
    sql = text(f'''
            select 
                mm.meter_id,
                mm.meter_name,
                mm.meter_code,
                mm.equipment_id,
                me.equipment_name,
                me.equipment_code,
                cp.mill_date,
                cp.mill_shift,
                cp.created_on as date_time,
                ROUND(MIN(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS start_kwh,
                ROUND(MAX(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS end_kwh,
                ROUND(Max(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) - ROUND(Min(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS kWh,
                SEC_TO_TIME(SUM(CASE WHEN mm.is_poll_meter = 'yes' and mm.meter = 'equipment' THEN cp.on_load_time ELSE 0 END)) AS on_load_time,
                SEC_TO_TIME(SUM(CASE WHEN mm.is_poll_meter = 'yes' and mm.meter = 'equipment' THEN cp.idle_time ELSE 0 END)) AS idle_time,
                SEC_TO_TIME(SUM(CASE WHEN mm.is_poll_meter = 'yes' and mm.meter = 'equipment' THEN cp.off_time ELSE 0 END)) AS off_time,
                SUM(CASE WHEN mm.is_poll_meter = 'yes' and mm.meter = 'equipment' THEN cp.diff_equipment_on_load_kwh ELSE 0 END ) as on_load_kwh,
                SUM(CASE WHEN mm.is_poll_meter = 'yes' and mm.meter = 'equipment' THEN cp.diff_equipment_off_kwh ELSE 0 END ) as off_kwh,
                SUM(CASE WHEN mm.is_poll_meter = 'yes' and mm.meter = 'equipment' THEN cp.diff_equipment_idle_kwh ELSE 0 END ) as idle_kwh,
                '' as formula,
                '' as tooltip_kwh
            from {table} cp
                inner join master_meter mm on mm.meter_id = cp.meter_id
                INNER JOIN master_plant mp ON mp.plant_id = mm.plant_id
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = mp.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mp.plant_id 
                left JOIN master_equipment me ON me.equipment_id = mm.equipment_id
            where  1=1 {where}
            group by mm.equipment_id ''')
    createFolder("routecardkwhlog/",f"{sql}...")  
    result = await cnx.execute(sql)
    result = result.mappings().all()
    return result

async def demand_report(period_id,campus_id,meter_id,from_date,to_date,shift_id,from_time,to_time,filter_type,parameter,duration,main_demand_meter,date_time,end_time,cnx):
    print(date_time)
    
    print(date_time)
    mill_date = date.today()
    mill_shift = 0
    no_of_shifts = 3
    current_shift = ''
    where = ''
    group_by = ''
    datas = ''
    table_name = ''
   
    if meter_id != '' and meter_id != "0":
        where += f" and mm.meter_id = '{meter_id}'"

    if campus_id != '' and campus_id != "0":
        where += f" and mp.campus_id = '{campus_id}'"

    if period_id == "live":  
        table_name = "ems_v1.current_power cp" 
        current_shift += f""" INNER JOIN master_shifts ms 
                        ON
                            ms.company_id=mm.company_id AND 
                            ms.bu_id=mm.bu_id AND 
                            ms.plant_id=mm.plant_id AND 
                            ms.status='active' AND 
                            ms.mill_date=cp.mill_date AND 
                            ms.mill_shift=cp.mill_shift """ 

    if period_id == "cur_shift":               
        table_name = "ems_v1.current_power_analysis cp"  
       
        current_shift += f""" INNER JOIN master_shifts ms 
                        ON
                            ms.company_id=mm.company_id AND 
                            ms.bu_id=mm.bu_id AND 
                            ms.plant_id=mm.plant_id AND 
                            ms.status='active' AND 
                            ms.mill_date=cp.mill_date AND 
                            ms.mill_shift=cp.mill_shift """
        
    elif period_id == "sel_shift":                  
                
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp" 
        where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''

    elif period_id == "sel_date":            
            
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp "                       
        where += f''' and cp.mill_date = '{from_date}' '''

    elif period_id == "#this_month":
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp "   
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
        where += f''' and cp.avg_powerfactor <> 0  '''

    elif period_id == "from_to":            
    
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            
        if from_date != '' and to_date != '':
            if from_date.month == to_date.month and from_date.year == to_date.year:
                month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                result_query = await check_analysis_table(cnx,month_year)
                
                if len(result_query) == 0:
                    return _getErrorResponseJson("Analysis table not available...")    
            
                table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp "
      
            else:

                field_name = 'mill_date,mill_shift,kva,actual_demand,demand_dtm,avg_powerfactor,created_on'
                
                month_year_range = [
                    (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                    for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                ]
                
                union_queries = []

                for month_year in month_year_range:
                    # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                    result_query = await check_analysis_table(cnx,month_year)

                    if len(result_query) > 0:
                        table_name = f"ems_v1_completed.power_analysis_{month_year}"
                        union_queries.append(f"SELECT {field_name} FROM {table_name}")
             
                subquery_union = " UNION ALL ".join(union_queries)
                table_name = f"( {subquery_union}) cp"
        
    query1 = ''
    query2 = ''
    partition = ''
    if parameter == 'pf' and filter_type != 'all':
        query1 = 'WITH RankedData AS ('
        query2 = '''
                    )
                    SELECT meter_id,company_id,bu_id,plant_id,campus_id,campus_name,max_demand,mill_date,mill_shift,demand,actual_demand,d_date_time,dm_powerfactor,date_time
                    FROM RankedData
                    WHERE row_num = 1'''
        partition = ',ROW_NUMBER() OVER (PARTITION BY ABS(cp.avg_powerfactor) ORDER BY cp.created_on) AS row_num'
        
    elif parameter == 'demand'and filter_type != 'all':
        query1 = 'WITH RankedData AS ('
        query2 = '''
                    )
                    SELECT meter_id,company_id,bu_id,plant_id,campus_id,campus_name,max_demand,mill_date,mill_shift,demand,actual_demand,d_date_time,dm_powerfactor,date_time
                    FROM RankedData
                    WHERE row_num = 1'''
        partition = ',ROW_NUMBER() OVER (PARTITION BY cp.actual_demand ORDER BY cp.created_on) AS row_num'

    elif filter_type == 'all' and duration != '' and duration != 1:
        query1 = 'SELECT data.* FROM ('
        query2 = f''') DATA
                    HAVING 
                        MOD(data.duration, {duration}) = 1'''
        partition = ", ROW_NUMBER() OVER (ORDER BY cp.created_on) AS duration"

    if from_time !='':
        where += f" and DATE_FORMAT(cp.created_on ,'%H:%i:%s')>='{from_time}' "

    if to_time !='':
        where += f" and DATE_FORMAT(cp.created_on ,'%H:%i:%s')<='{to_time}' "

    if main_demand_meter == 'yes':
        where +=f" and mm.main_demand_meter = 'yes'"
        group_by = f" group by mp.campus_id, mm.meter_id"
    else:
        group_by = f" group by mm.meter_id"

    if period_id != "#this_month" and period_id != 'live':
        group_by += f" ,cp.created_on "

    if date_time != '' and period_id == "peak_time":
        
        where +=f" and cp.created_on >='{date_time}' and cp.created_on <='{end_time}'"
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp " 

    if table_name != '':
        query = f'''
            {query1}
            select
                
                mm.meter_id,
                mm.meter_name,
                mm.meter_code,
                mm.company_id,
                mm.bu_id,
                mm.plant_id,
                mp.campus_id,
                mcs.campus_name,
                mm.max_demand,
                mm.max_pf,
                cp.mill_date,
                cp.mill_shift,
                ROUND(AVG(CASE WHEN mmf.kva = '*' THEN cp.kva * mmf.kva_value WHEN  mmf.kva = '/' THEN cp.kva / mmf.kva_value ELSE cp.kva END ),prf.kva) AS demand,       
                ROUND(AVG(CASE WHEN mmf.kva = '*' THEN cp.actual_demand * mmf.kva_value WHEN  mmf.kva = '/' THEN cp.actual_demand / mmf.kva_value ELSE cp.actual_demand END ),prf.kva) AS actual_demand,
                MAX(cp.demand_dtm) AS d_date_time,
                AVG(CASE WHEN mmf.avg_powerfactor = '*' THEN ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value WHEN  mmf.avg_powerfactor = '/' THEN ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value ELSE ABS(cp.avg_powerfactor) END ) AS dm_powerfactor, 
                IFNULL(ROUND(AVG(CASE WHEN mmf.avg_powerfactor = '*' THEN ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value WHEN  mmf.avg_powerfactor = '/' THEN ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value ELSE ABS(cp.avg_powerfactor) END),prf.avg_powerfactor),'') AS dm_avg_powerfactor,       
                IFNULL(ROUND(AVG(CASE WHEN mmf.kva = '*' THEN cp.kva * mmf.kva_value WHEN  mmf.kva = '/' THEN cp.kva / mmf.kva_value ELSE cp.kva END),prf.kva),'') AS avg_actual_demand,
                cp.created_on as date_time
                {partition}
                
                from 
                    {table_name}
                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                    INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                    INNER JOIN ems_v1.master_business_unit bu ON mm.bu_id = bu.bu_id
                    INNER JOIN ems_v1.master_plant mp ON mm.plant_id = mp.plant_id
                    INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = mp.campus_id
                    INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                    LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                    inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = mp.plant_id AND mmf.meter_id = mm.meter_id
                    inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mp.plant_id 
                    {current_shift}
                where mm.status = 'active'  {where}
                {group_by}
                {query2}
                '''
        print(query)
        datas = await cnx.execute(query)
        datas = datas.mappings().all()
    return datas

async def availability_report(cnx,campus_id,from_date,to_date,from_year,to_year,report_type):

    where = ''
    where1 = ''
    on_where = ''
    if from_date != '':
        if report_type == 'with_rate':
            on_where += f" and msed.mill_date >='{from_date}'"
        else:
            where += f" and msed.mill_date >='{from_date}'"
        where1 += f" and cp.mill_date >='{from_date}'"
        
    if to_date != '':
        if report_type == 'with_rate':
            on_where += f" and msed.mill_date <='{to_date}'"
        else:
            where += f" and msed.mill_date <='{to_date}'"
        where1 += f" and cp.mill_date <='{to_date}'"
        
    if campus_id != '':
        where += f" and mse.campus_id ='{campus_id}'"
        where1 += f" and mp.campus_id ='{campus_id}'"

    field_name = 'meter_id,equipment_kwh,kWh,mill_date'
    month_year_range = [
        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
    ]
    
    union_queries = []


    for month_year in month_year_range:
        
        result_query = await check_power_table(cnx,month_year)

        if len(result_query) > 0:
            table_name = f"ems_v1_completed.power_{month_year}"
            union_queries.append(f"SELECT {field_name} FROM {table_name}")

    subquery_union = " UNION ALL ".join(union_queries)
    table_name = f"( {subquery_union}) cp"

    query = f'''
            select
                ifnull(SUM(msed.consumption),0) AS consumption,
                mse.*,
                ifnull(mbr.budget,0) budget,
                ifnull(mbr.actual,0) actual,
                ifnull(mbr.budget_mix,0) budget_mix,
                msed.mill_date
            FROM 
                master_source_entry mse
                left JOIN master_source_entry_date msed ON msed.energy_source_id = mse.id {on_where}
                left JOIN master_budget_rate mbr ON mbr.energy_source_name = mse.energy_source_name 
                and mbr.campus_id = mse.campus_id and mbr.month = msed.mill_date and mbr.source_type = 'external'
            where 1=1  and mse.status ='active' and mse.source_type = 'external' {where} 
            group by mse.id
            order by mse.id
            '''
    createFolder("Log/", "availability report query " + str(query))
    data = await cnx.execute(query)
    data = data.mappings().all()
    query2 = f'''
            select
                mbe.*,
                mp.plant_name,
                mp.plant_code,
                mm.plant_id,
                mp.campus_id,
                c.campus_name,
                cp.mill_date,
                ROUND(SUM(CASE WHEN mm.meter_type = 'primary' and mbd.department_type = 'utility' THEN cp.equipment_kwh ELSE 0 END),prf.kWh) AS pm_common_kwh,  
                ROUND(SUM(CASE WHEN mm.meter_type = 'Primary' and mbd.department_type = 'overall' THEN cp.equipment_kwh ELSE 0 END),prf.kWh) AS pm_kwh,
                mbd.department_type,
                mbd.is_corporate
            from 
                {table_name}
                inner join master_meter mm on mm.meter_id = cp.meter_id
                inner join master_plant mp on mp.plant_id = mm.plant_id
                inner join master_campus c on c.campus_id = mp.campus_id
                inner join master_plant_wise_department md on md.plant_department_id = mm.plant_department_id
                inner join master_budget_department mbd on mbd.department_id = mm.plant_department_id 
                inner join master_budget_entry mbe on mbe.reporting_department_id = mbd.reporting_department_id
                inner join master_meter_factor mmf on mmf.meter_id = mm.meter_id and mmf.plant_id = mm.plant_id
                inner join master_parameter_roundoff prf on prf.plant_id = mp.plant_id
            where mm.status = 'active' and mbe.status = 'active' and mbe.financial_year >= '{from_year}' and mbe.financial_year<={to_year} {where1}
            group by mp.campus_id,mbd.reporting_department_id
            order by mm.plant_id,mbd.reporting_department_id
            '''
    createFolder("Log/", "availability report query2 " + str(query2))
    data2 = await cnx.execute(query2)
    data2 = data2.mappings().all()
    
    query3 = f'''
            select
                mp.plant_name,
                mp.plant_code,
                mm.plant_id,
                mp.campus_id,
                cp.mill_date,
                mm.source,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kWh,
                ifnull(mbr.actual,0) actual,
                ifnull(mbr.budget,0) budget,
                ifnull(mbr.budget_mix,0) budget_mix
            from 
                {table_name}
                inner join master_meter mm on mm.meter_id = cp.meter_id
                inner join master_plant mp on mp.plant_id = mm.plant_id
                inner join master_campus c on c.campus_id = mp.campus_id
                inner join master_plant_wise_department md on md.plant_department_id = mm.plant_department_id
                inner join master_meter_factor mmf on mmf.meter_id = mm.meter_id and mmf.plant_id = mm.plant_id
                inner join master_parameter_roundoff prf on prf.plant_id = mp.plant_id
                left JOIN master_budget_rate mbr ON mbr.energy_source_name = mm.source 
                and mbr.campus_id = mp.campus_id and mbr.month = cp.mill_date and mbr.source_type = 'internal' 
            where mm.status = 'active' and mm.source <> 'EB'and  mm.meter_type = 'primary' {where1}
            group by mp.campus_id,mm.source
            order by mm.plant_id,mm.source
            '''
    createFolder("Log/", "availability report query3 " + str(query3))
    data3 = await cnx.execute(query3)
    data3 = data3.mappings().all()

    if data != [] and data2!= []:
        result = {"data":data,"data2":data2,"data3":data3}
    else:
        result = []
    return result

async def tnebreportdetail(cnx,campus_id,period_id,from_date,to_date):
    where = '' 
    where_k = '' 
    sel = ''
    left = ''
    table_name = ''
    if campus_id != '':
        where +=f" and mse.campus_id = {campus_id}"
        where_k +=f" and mp.campus_id = {campus_id}"

    if period_id == "date":

        from_date = await parse_date(from_date)
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        where +=f" and msed.mill_date = '{from_date}'"
        where_k +=f" and cp.mill_date = '{from_date}'"
        table_name = f'ems_v1_completed.power_{month_year} cp'
    
    elif period_id =="month":
        from_date= await parse_date(from_date)  
        to_date = from_date.replace(day=1, month=from_date.month + 1)
        to_date = to_date - timedelta(days = 1)
        print("to_date",to_date)
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
       
        where +=f" and msed.mill_date >= '{from_date}' and msed.mill_date <= '{to_date}'"
        where_k +=f" and cp.mill_date >= '{from_date}' and cp.mill_date <= '{to_date}'"
        table_name = f'ems_v1_completed.power_{month_year} cp'
        
        
    elif period_id =="from_to":
        to_date = await parse_date(to_date)
        from_date = await parse_date(from_date)
        where +=f" and msed.mill_date >= '{from_date}' and msed.mill_date <= '{to_date}'"
        where_k +=f" and cp.mill_date >= '{from_date}' and cp.mill_date <= '{to_date}'"
        field_name = 'meter_id,kwh,mill_date'
            
        month_year_range = [
            (from_date + timedelta(days=31 * i)).strftime("%m%Y")
            for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
        ]
        
        union_queries = []
        for month_year in month_year_range:
            result_query = await check_power_table(cnx,month_year)

            if len(result_query) > 0:
                table_name = f"ems_v1_completed.power_{month_year}"
                union_queries.append(f"SELECT {field_name} FROM {table_name}")
        subquery_union = " UNION ALL ".join(union_queries)
        table_name = f"( {subquery_union}) cp"

    sel = f"cp.kwh,"
    left = f'''left join (SELECT
                cp.meter_id,
                cp.mill_date,
                ROUND(SUM(CASE WHEN mmf.kWh = '*' THEN cp.kWh * mmf.kWh_value WHEN  mmf.kWh = '/' THEN cp.kWh / mmf.kWh_value ELSE cp.kWh END ),prf.kWh) AS kWh
    
                FROM {table_name}
                INNER JOIN master_meter mm ON mm.meter_id = cp.meter_id
                INNER JOIN master_plant mp ON mp.plant_id = mm.plant_id
                INNER JOIN master_meter_factor mmf ON mmf.meter_id = mm.meter_id AND mmf.plant_id = mm.plant_id
                INNER JOIN master_parameter_roundoff prf ON prf.plant_id = mm.plant_id
                WHERE mm.main_demand_meter = 'yes' {where_k}
                GROUP BY cp.mill_date) cp on cp.mill_date = msed.mill_date '''
    


    query = f'''
            select
                c.campus_name,
                mse.energy_source_name,
                mse.period_type,
                c.campus_id,
                {sel}
                msed.* 
            from
                master_source_entry mse
                inner join master_source_entry_date msed on msed.energy_source_id = mse.id
                inner join master_campus c on c.campus_id = msed.campus_id and c.campus_id = mse.campus_id    
                {left}
                where mse.status = 'active' {where}
                group by msed.mill_date
                order by msed.mill_date'''
    data = await cnx.execute(query)
    data = data.mappings().all()
    
    return data

async def avg_demand_report(cnx,campus_id,period_id,from_date,to_date,meter_id,main_demand_meter):
    
    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
    where = ''
    table_name = ''
    group_by = ''
    field_name = 'id,meter_id, created_on, mill_date, mill_shift, avg_powerfactor,kva'
    
    if period_id == "#this_month":
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

    if campus_id != '' and campus_id != "0":
        where +=f" and md.campus_id = '{campus_id}'"
    
    if meter_id != '' and meter_id != "0":
        where +=f" and mm.meter_id = '{meter_id}'"
    
         
    month_year_range = [
                (from_date + timedelta(days=30 * i)).strftime("%m%Y")
                for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
            ]
    union_queries = []
    

    for month_year in month_year_range:
        query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND  TABLE_NAME = 'power_analysis_{month_year}'"""
        result_query = await cnx.execute(query)
        result_query = result_query.fetchall()
        print(query)
        if len(result_query) > 0:
            table_name = f"ems_v1_completed.power_analysis_{month_year}"
            union_queries.append(f"SELECT {field_name} FROM {table_name}")
    
    subquery_union = " UNION ALL ".join(union_queries)
    table_name = f"({subquery_union}) cp"

    if main_demand_meter == 'yes':
        where +=f" and mm.main_demand_meter = 'yes'"
        group_by = f" group by md.campus_id"
    else:
        group_by = f" group by mm.meter_id"
        
    query = f'''
            select 
                cp.meter_id,
                md.campus_id,
                mcs.campus_name,
                mm.max_demand,
                mm.max_pf,
                IFNULL(ROUND(AVG(CASE WHEN mmf.avg_powerfactor = '*' THEN ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value WHEN  mmf.avg_powerfactor = '/' THEN ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value ELSE ABS(cp.avg_powerfactor) END),prf.avg_powerfactor),'') AS dm_avg_powerfactor,       
                IFNULL(ROUND(AVG(CASE WHEN mmf.kva = '*' THEN cp.kva * mmf.kva_value WHEN  mmf.kva = '/' THEN cp.kva / mmf.kva_value ELSE cp.kva END),prf.kva),'') AS avg_actual_demand
            from
                {table_name}
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
            where mm.status = 'active' AND cp.avg_powerfactor <>0 {where}
            {group_by}'''
    data = await cnx.execute(query)
    data = data.mappings().all()
    
    return data

async def minmax_kwh_dtl(cnx, plant_department_id ,equipment_group_id ,equipment_id,meter_id  ,groupby ,period_id,from_date,to_date,shift_id,reportfor):    
    

    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
    completed_db="ems_v1_completed."           
    where = "" 

    where_p = "" 
    group_by = ""
    order_by = ""  
    function_where = ''
    group_by_poll = ''
    minmax_kwh = ''
        
    if plant_department_id != '' and plant_department_id != "0" and plant_department_id != 'all':
        
        where += f" and mm.plant_department_id in ({plant_department_id})"
        where_p += f" and mm.plant_department_id in ({plant_department_id})"
        
    if equipment_group_id != '' and equipment_group_id != 0 and equipment_group_id != 'all':
        
        where += f" and mm.equipment_group_id in ({equipment_group_id})"
        where_p += f" and mm.equipment_group_id in ({equipment_group_id})"
        
    if equipment_id != '' and equipment_id != 0 and equipment_id != 'all':
        
        where += f" and me.equipment_id in ({equipment_id})"
        where_p += f" and me.equipment_id in ({equipment_id})"

    if meter_id != '' and meter_id != 'all':
        where += f" and mm.meter_id in ({meter_id})"
        where_p += f" and mm.meter_id in ({meter_id})"

    if groupby == 'equipment' or groupby == 'equipment_group':
        equipment = f'''
                        inner join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id
                        left join master_equipment_calculations mec on mec.equipment_id = mm.equipment_id AND mec.meter_communication = 'equipment' and mec.status = 'active'
                        '''
       
    else:
        equipment = f''' 
                        left join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id'''
       
        
    mill_date = date.today()
    mill_shift = 0
    no_of_shifts = 3

    table_name = ''

    current_shift = ''
    join = ''
 
    if reportfor == '12to12':
        if period_id == "sel_date":            
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year}_12 as cp " 

            query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12'"""
            result_query = cnx.execute(query).fetchall()

            if len(result_query)==0:
                return _getErrorResponseJson("12to12 table not available...") 

            where += f''' and cp.mill_date = '{from_date}' '''

        elif period_id == "from_to":            
               
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

        
            if from_date.month == to_date.month:
                table_name=f"  {completed_db}power_{month_year}_12 as cp "
            else:
                field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kWh,reverse_master_kwh,reverse_kwh,meter_status_code,actual_ton'
                
                month_year_range = [
                    (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                    for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                ]
                union_queries = []
                for month_year in month_year_range:
                    query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}_12'"""
                    result_query = cnx.execute(query).fetchall()
                    
                    if len(result_query) > 0:
                        table_name = f"ems_v1_completed.power_{month_year}_12"
                        union_queries.append(f"SELECT {field_name} FROM {table_name}")


                subquery_union = " UNION ALL ".join(union_queries)
                table_name = f"( {subquery_union}) cp"
    else:
        
        if period_id == "cur_shift":               
            table_name = "ems_v1.current_power cp"  
            # where += f''' and cp.mill_date = '{mill_date}' AND cp.mill_shift = '{mill_shift}' '''      
            current_shift += f""" Inner JOIN master_shifts ms 
                            ON
                                ms.company_id=mm.company_id AND 
                                ms.bu_id=mm.bu_id AND 
                                ms.plant_id=mm.plant_id AND 
                                ms.status='active' AND 
                                ms.mill_date=cp.mill_date AND 
                                  ms.mill_shift=cp.mill_shift """

        elif period_id == "#cur_shift":
            where += f''' and cp.mill_date = '{mill_date}' AND cp.mill_shift = '{mill_shift}' '''              
            table_name = "ems_v1.current_power cp" 
            
        elif period_id == "sel_shift":                  
                   
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year} as cp" 
            
            
            where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' AND cpd.mill_shift = '{shift_id}' '''
             

        elif period_id == "#previous_shift":   
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year} as cp" 
           
        
            where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' '''   
            where_p += f''' and cpd.mill_date = '{from_date}' AND cpd.mill_shift = '{shift_id}' '''   

        elif period_id == "sel_date":            
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""

            table_name=f"  {completed_db}power_{month_year} as cp "           
                     
            where += f''' and cp.mill_date = '{from_date}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' '''

            # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name ='polling_data_{month_year}'"""
            result_p = await check_polling_data_tble(cnx,month_year)                

        elif period_id == "#sel_date":

            from_date = mill_date
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  {completed_db}power_{month_year} as cp "
           
            where += f''' and cp.mill_date = '{from_date}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' '''

            # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
            result_query = await check_power_table(cnx,month_year)

            if len(result_query) >0 :
                field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kWh,reverse_master_kwh,reverse_kwh,meter_status_code,equipment_kwh,actual_demand,demand_dtm,actual_ton'
                table_name = f'''(select {field_name} from ems_v1.current_power union all select {field_name} from ems_v1_completed.power_{month_year}) as cp'''
       
        elif period_id == "#previous_day":             
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            result_p = await check_polling_data_tble(cnx,month_year)
            table_name=f"  {completed_db}power_{month_year} as cp "
            
            where += f''' and cp.mill_date = '{from_date}' '''
            where_p += f''' and cpd.mill_date = '{from_date}' '''
            
        elif period_id  == "#this_week":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
       
        elif period_id == "#previous_week":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id == "#this_month":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
            day = to_date.day
                
        elif period_id == "#previous_month":
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
            
        elif period_id=="#this_year": 
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id=="#previous_year": 
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id=="#sel_year": 
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''
        
        elif period_id == "from_to" or period_id == '#from_to':            
    
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
            where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            where_p += f''' and cpd.mill_date  >= '{from_date}' and cpd.mill_date <= '{to_date}' '''

            
        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or  period_id=="#from_to" or period_id == "#sel_year":
            if from_date != '' and to_date != '':
                if from_date.month == to_date.month and from_date.year == to_date.year:
                    month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                    # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                    result_query = await check_power_table(cnx,month_year)
                  
                    if len(result_query) == 0:
                        return _getErrorResponseJson("power table not available...")    
                
                    table_name=f"  {completed_db}power_{month_year} as cp "
                
                    
                   
                            
                else:

                    field_name = 'power_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,meter_id,design_id,beam_id,date_time,date_time1,mill_date,mill_shift,vln_avg,r_volt,y_volt,b_volt,vll_avg,ry_volt,yb_volt,br_volt,t_current,r_current,y_current,b_current,t_watts,r_watts,y_watts,b_watts,t_var,r_var,y_var,b_var,t_voltampere,r_voltampere,y_voltampere,b_voltampere,avg_powerfactor,r_powerfactor,y_powerfactor,b_powerfactor,powerfactor,kWh,kvah,kw,kvar,power_factor,kva,frequency,machine_status,status,created_on,created_by,modified_on,modified_by,machine_kWh,master_kwh,reverse_machine_kWh,reverse_master_kwh,reverse_kwh,meter_status_code,equipment_kwh,actual_demand,demand_dtm,actual_ton'
                    field_name1 = 'mill_date,mill_shift,kWh'
            
                    month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                    
                    union_queries = []
                    union_querie = []
                    joins = []

                    for month_year in month_year_range:
                        # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                        result_query = await check_power_table(cnx,month_year)

                        if len(result_query) > 0:
                            table_name = f"ems_v1_completed.power_{month_year}"
                            union_queries.append(f"SELECT {field_name} FROM {table_name}")
                            union_querie.append(f"SELECT {field_name1} FROM {table_name}")

                        # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name ='polling_data_{month_year}'"""

                    subquery_union = " UNION ALL ".join(union_queries)
                    table_name = f"( {subquery_union}) cp"

                        
    order_by_limit = ''
    on_min = ''
    on_max = ''
    
    if groupby !='' and groupby == "plant_department":
        group_by += "  mm.plant_department_id "
        order_by += "  mpd.plant_department_order "
        on_min = f"min_subquery.plant_department_id = mm.plant_department_id"
        on_max = f"max_subquery.plant_department_id = mm.plant_department_id"
        
    elif groupby !='' and groupby == "equipment_group":
        group_by += " me.equipment_group_id"
        group_by_poll += " me.equipment_group_id"
        order_by += " mmt.equipment_group_order"
        
        on_min = f"min_subquery.equipment_group_id = mm.equipment_group_id"
        on_max = f"max_subquery.equipment_group_id = mm.equipment_group_id"
        
    elif groupby !='' and groupby == "equipment":
        group_by += " me.equipment_id"
        group_by_poll += " me.equipment_id"
        order_by += " me.equipment_order"
        on_min = f"min_subquery.equipment_id = mm.equipment_id"
        on_max = f"max_subquery.equipment_id = mm.equipment_id"
        
        
    elif groupby !='' and groupby == "meter":             
        group_by += " mm.meter_id"
        group_by_poll += " mm.meter_id"
        order_by += " mm.meter_order"        
        on_min = f"min_subquery.meter_id = mm.meter_id"
        on_max = f"max_subquery.meter_id = mm.meter_id"
         
       

    query1 = ''
    demand = ''
    minmax_join = ''
  
    
    query1 +=f'''   max_subquery.max_date as max_date,
                    min_subquery.min_date as min_date,
                    max_subquery.max_shift as max_shift,
                    min_subquery.min_shift as min_shift'''
    
    if groupby == 'meter':
        minmax_kwh = f'''(CASE 
                                    WHEN mmf.kWh = '*' THEN cp.kWh * mmf.kWh_value 
                                    WHEN mmf.kWh = '/' THEN cp.kWh / mmf.kWh_value 
                                    ELSE cp.kWh 
                                END) AS kwh,'''
    else:
        minmax_kwh = f'''SUM(cp.equipment_kwh) AS kwh,'''
        
    minmax_join = f'''LEFT JOIN (
                                SELECT 
                                    mm.meter_id,
                                    mm.company_id,
                                    mm.bu_id,
                                    mm.plant_id,
                                    md.campus_id,
                                    mm.plant_department_id,
                                    mm.equipment_group_id,
                                    me.equipment_id,
                                    {minmax_kwh}
                                    cp.mill_date AS max_date,
                                    cp.mill_shift AS max_shift
                               FROM 
                                    {table_name}
                                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                                    INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                                    INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                                    INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                                    INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                                    INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                                    INNER JOIN ems_v1.master_model mdl ON mdl.model_id = mm.model_name
                                    INNER JOIN ems_v1.master_model_make mk ON mk.model_make_id = mdl.model_make_id
                                    LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                                    inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                                    inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
                                    left join master_meter_communication mmc on mmc.meter_status_code = cp.meter_status_code
                                    {equipment}
                                    left JOIN ems_v1.master_equipment_group mmt ON me.equipment_group_id = mmt.equipment_group_id
                                    {current_shift}                                
                                WHERE  
                                cp.status = '0' and mm.status = 'active' 
                                {where}
                                    
                                ORDER BY kwh DESC LIMIT 1
                                
                            ) AS max_subquery ON {on_max}
                            LEFT JOIN (
                                SELECT 
                                
                                    mm.meter_id,
                                    mm.company_id,
                                    mm.bu_id,
                                    mm.plant_id,
                                    md.campus_id,
                                    mm.plant_department_id,
                                    mm.equipment_group_id,
                                    me.equipment_id,
                                    {minmax_kwh}
                                    cp.mill_date AS min_date,
                                    cp.mill_shift AS min_shift
                                FROM 
                                    {table_name}
                                    INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                                    INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                                    INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                                    INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                                    INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                                    INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                                    INNER JOIN ems_v1.master_model mdl ON mdl.model_id = mm.model_name
                                    INNER JOIN ems_v1.master_model_make mk ON mk.model_make_id = mdl.model_make_id
                                    inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                                    inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
                                    {equipment}
                                    left JOIN ems_v1.master_equipment_group mmt ON me.equipment_group_id = mmt.equipment_group_id
                                    {current_shift}                                
                                WHERE  
                                cp.status = '0' and mm.status = 'active' 
                                {where}                        
                                
                            ORDER BY kwh ASC LIMIT 1

                            ) AS min_subquery ON {on_min}
'''

    if group_by != "":
        group_by = f"group by  {group_by} "    
        group_by_poll = f"group by  {group_by_poll} "    
    
    if order_by != "":
        order_by = f"order by {order_by_limit} {order_by}"

    query = text(f'''
            SELECT                       
                mpd.plant_department_code,
                mpd.plant_department_name,
                ifnull(mmt.equipment_group_code,'') equipment_group_code,
                ifnull(mmt.equipment_group_name,'') equipment_group_name,
                ifnull(me.equipment_code,'') equipment_code,
                ifnull(me.equipment_name,'') equipment_name,
                mm.meter_code,
                mm.meter_name,
                cp.power_id,
                mm.company_id,
                mm.bu_id,
                mm.plant_id,
                md.campus_id,
                mm.plant_department_id,
                mm.equipment_group_id ,
                ifnull(me.equipment_id,0) equipment_id,
                cp.meter_id,
                cp.date_time,
                cp.date_time1,
                cp.mill_date,
                cp.mill_shift,
                {query1}
                
                
            FROM 
                {table_name}                       
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                INNER JOIN ems_v1.master_company mc ON mm.company_id = mc.company_id
                INNER JOIN ems_v1.master_business_unit mb ON mm.bu_id = mb.bu_id
                INNER JOIN ems_v1.master_plant md ON mm.plant_id = md.plant_id
                INNER JOIN ems_v1.master_campus mcs ON mcs.campus_id = md.campus_id
                INNER JOIN ems_v1.master_plant_wise_department mpd ON mm.plant_department_id = mpd.plant_department_id
                INNER JOIN ems_v1.master_model mdl ON mdl.model_id = mm.model_name
                INNER JOIN ems_v1.master_model_make mk ON mk.model_make_id = mdl.model_make_id
                LEFT JOIN ems_v1.master_converter_detail mcd ON mm.converter_id = mcd.converter_id 
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id 
                left join master_meter_communication mmc on mmc.meter_status_code = cp.meter_status_code
                {equipment}
                left JOIN ems_v1.master_equipment_group mmt ON me.equipment_group_id = mmt.equipment_group_id
                left JOIN ems_v1.master_equipment_class ecls ON me.equipment_class_id = ecls.equipment_class_id
                {current_shift} 
                {join}   
                {minmax_join}                          
            WHERE  
                cp.status = '0' and mm.status = 'active' 
                {where}                        
                {group_by}
                {order_by}
                
            ''')
    
    createFolder("Current_power_log/","current_power api query "+str(query))
    datas = await cnx.execute(query)
    datas = datas.mappings().all()
    return datas

async def manualentryhistory(cnx,campus_id,company_id,bu_id,plant_id,meter_id,from_date,to_date):
    where = ''
    if campus_id != '' and campus_id != "0":
        where +=f" and mp.campus_id = '{campus_id}'"
    
    if company_id != '' and company_id != "0":
        where +=f" and  mm.company_id = '{company_id}'"

    if bu_id != '' and bu_id != "0":
        where +=f" and mm.bu_id = '{bu_id}'"
    
    if plant_id != '' and plant_id != "0":
        where +=f" and mm.plant_id = '{plant_id}'"
    
    if meter_id != '' and meter_id != "0":
        where +=f" and mm.meter_id = '{meter_id}'"

    from_date = await parse_date(from_date)
    to_date = await parse_date(to_date)
    
    query = f'''
            select 
                c.campus_name,
                mc.company_code,
                mc.company_name,
                bu.bu_code,
                bu.bu_name,
                mp.plant_code,
                mp.plant_name,
                mm.meter_code,
                mm.meter_name,
                mp.campus_id,
                mm.company_id,
                mm.bu_id,
                mm.plant_id,
                mm.meter_id,
                mh.*,
                ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user
            from
                manual_entry_history mh
                inner join master_meter mm on mm.meter_id = mh.meter_id
                inner join master_company mc on mc.company_id = mm.company_id
                inner join master_business_unit bu on bu.bu_id = mm.bu_id
                inner join master_plant mp on mp.plant_id = mm.plant_id
                inner join master_campus c on c.campus_id = mp.campus_id
                left join master_employee cu on cu.employee_id=mh.created_by
            where  mh.mill_date >= '{from_date}'  and mh.mill_date <= '{to_date}'  {where}'''
    
    datas = await cnx.execute(query)
    datas = datas.mappings().all()
    return datas

async def hour_report(cnx,campus_id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,employee_id,meter_id,from_date,meter_type,kwh,month_year,request):

        where = ""
        rslt = ''
        kwh_type = kwh
        if meter_id == "" or meter_id == 'all':
            pass
        else:
            meter_id = await id(meter_id)
            where += f" and mm.meter_id IN ({meter_id})"

        if  employee_id != '':
            query = text(f'''select * from ems_v1.master_employee where employee_id = {employee_id}''')
            res = await cnx.execute(query)
            res = res.fetchall()       
    
            if len(res)>0:
                for row in res:
                    plant_id = row["plant_id"]
                    plant_department_id = row["plant_department_id"]
                    equipment_group_id = row["equipment_group_id"]

        if company_id !='' and company_id !=0 and company_id != 'all':
            where += f" and mm.company_id ={company_id}"

        if bu_id !='' and bu_id !=0 and bu_id != 'all':
            where += f" and mm.bu_id ={bu_id}"

        if plant_id !='' and plant_id !=0 and plant_id != 'all':
            where += f" and md.plant_id ={plant_id}"

        if campus_id !='' and campus_id !=0 and campus_id != 'all':
            where += f" and md.campus_id ={campus_id}"

        if plant_department_id !='' and plant_department_id != 0 and plant_department_id != 'all':
            where += f" and ms.plant_department_id ={plant_department_id}"

        if equipment_group_id !='' and equipment_group_id!= 0 and equipment_group_id != 'all':
            where += f" and mmt.equipment_group_id ={equipment_group_id}"

        if meter_type!= '':
            where += f" and mm.meter_type ='{meter_type}'"

        query = text(f'''
        SELECT data.* FROM (
            SELECT
                mm.meter_code AS meter_code,
                mm.meter_name AS meter_name,
                CONCAT('h', Hour(cp.created_on)) hour,
                DATE_FORMAT(cp.mill_date, '%d-%m-%Y') AS mill_date,
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ELSE 0 END),prf.machine_kWh) AS master_kwh,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ELSE 0 END),prf.machine_kWh) AS machine_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value  when mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kwh,
                ROUND(SUM(cp.equipment_kwh),prf.kWh) AS calculated_kwh,
                mm.meter_type,
                md.plant_name,
                md.campus_id,
                c.campus_name,
                ROW_NUMBER() OVER (ORDER BY cp.created_on) AS duration
            FROM
                ems_v1_completed.power_analysis_{month_year} cp
                INNER JOIN ems_v1.master_meter mm ON mm.meter_id = cp.meter_id
                inner JOIN ems_v1.master_plant_wise_department ms ON ms.plant_department_id = mm.plant_department_id                   
                inner JOIN ems_v1.master_plant md ON md.plant_id = mm.plant_id                   
                inner JOIN ems_v1.master_campus c ON c.campus_id = md.campus_id                   
                LEFT JOIN ems_v1.master_equipment_group mmt ON mmt.equipment_group_id = mm.equipment_group_id 
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = md.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = md.plant_id                   
            WHERE
                1=1  and mm.status = 'active'{where} and cp.mill_date  = '{from_date}' 
            GROUP BY
                mm.meter_id,
                cp.mill_date,
                hour(cp.created_on) 
            ) DATA
                    HAVING 
                        MOD(data.duration, 1) = 0         
        ''')
        rslt = await cnx.execute(query)
        rslt = rslt.fetchall()
        createFolder("Log/","result"+str(rslt))
        # if rslt !='':
        #     output = {}

            
        #     output_keys = [f'h{hour}' for hour in range(1, 25)]
        #     machine_kwh_keys = [f'machine_kwh_h{hour}' for hour in range(1, 25)]
        #     master_kwh_keys = [f'master_kwh_h{hour}' for hour in range(1, 25)]
                
        #     for row in rslt:
        #         meter_code = row.meter_code
        #         meter_name = row.meter_name
        #         meter_type = row.meter_type
        #         plant_name = row.plant_name
        #         hour = row.hour
        #         if kwh_type =='calculated':
        #             kwh = row.calculated_kwh
        #         else:
        #             kwh = row.kwh

        #         machine_kwh = row.machine_kwh
        #         master_kwh = row.master_kwh
            
        #         if meter_code not in output:
        #             output[meter_code] = {
        #                 'meter_code': meter_code,
        #                 'meter_name': meter_name,
        #                 "meter_type": meter_type,
        #                 "plant_name": plant_name
        #             }
        #             for key, machine_kwh_key,master_kwh_key in zip(output_keys, machine_kwh_keys,master_kwh_keys):
        #                 output[meter_code][key] = 0
        #                 output[meter_code][machine_kwh_key] = 0
        #                 output[meter_code][master_kwh_key] = 0
            
        #         output[meter_code][hour] = kwh
        #         output[meter_code][f'machine_kwh_{hour}'] = machine_kwh
        #         output[meter_code][f'master_kwh_{hour}'] = master_kwh
        
        #         result=list(output.values())
        #     if rslt == []:
        #         result = []
            
            
        createFolder("Log/","result"+str(123))
        return rslt
async def transformerlossreport(cnx,campus_id,from_date,to_date):
    if from_date != '' and to_date != '':
        field_name = "kwh,equipment_kwh,mill_date,meter_id"
        month_year_range = [
            (from_date + timedelta(days=31 * i)).strftime("%m%Y")
            for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
        ]
        union_queries = []
        for month_year in month_year_range:
            result_query = await check_power_table(cnx,month_year)

            if len(result_query) > 0:
                table_name = f"ems_v1_completed.power_{month_year}"
                union_queries.append(f"SELECT {field_name} FROM {table_name}")

        subquery_union = " UNION ALL ".join(union_queries)
        table_name = f"( {subquery_union}) cp"
        
    sql = f'''
            select 
                mm.meter_id,
                mm.meter_name,
                CONCAT(mm.meter_code , '-' , mm.meter_name) AS meter_code,
                c.campus_name,
                mp.campus_id,
                mp.plant_name,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kWh,
                ROUND(SUM(cp.equipment_kwh),prf.kWh) AS calculated_kwh,
                cp.mill_date,
                CONCAT('d', DAY(cp.mill_date)) as day
            from 
                {table_name}
                inner join master_meter mm on mm.meter_id = cp.meter_id
                inner join master_plant mp on mp.plant_id = mm.plant_id
                inner join master_campus c on c.campus_id = mp.campus_id
                inner JOIN ems_v1.master_meter_factor mmf ON  mmf.plant_id = mp.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mp.plant_id 
            where mm.status = 'active' and cp.mill_date >='{from_date}' and cp.mill_date <='{to_date}' and mp.campus_id ='{campus_id}' and mm.main_transformer_meter = 'yes'
            group by mm.meter_id, cp.mill_date
                '''
    rslt = await cnx.execute(sql)
    rslt = rslt.fetchall()
    if rslt !='':
        
        output = {}
        output_keys = [f'd{day}' for day in range(1, 32)]
        cal_kwh_keys = [f'cal_kwh_d{day}' for day in range(1, 32)]
    
        for row in rslt:
            meter_code = row.meter_code
            meter_name = row.meter_name
            campus_name = row.campus_name
            day = row.day

            calculated_kwh = row.calculated_kwh
            kwh = row.kWh
        
            if meter_code not in output:
                output[meter_code] = {
                    'meter_code': meter_code,
                    'meter_name': meter_name,
                    "campus_name": campus_name
                }
                for key, cal_kwh_key in zip(output_keys, cal_kwh_keys):
                    output[meter_code][key] = 0
                    output[meter_code][cal_kwh_key] = 0
        
            output[meter_code][day] = kwh
            output[meter_code][f'cal_kwh_{day}'] = calculated_kwh
    
            result=list(output.values())
        if rslt == []:
            result = []
        
    return result