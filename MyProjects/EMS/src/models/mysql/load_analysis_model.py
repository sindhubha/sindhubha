from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.parse_date import parse_date
from datetime import date,timedelta
from log_file import createFolder
from src.models.check_table import check_power_table,check_power_12_table,check_analysis_table,check_polling_data_tble,check_alarm_tble,check_user_count


async def load_analysis_mdl(cnx,company_id,bu_id,plant_id,period_id,group_by,equipment_id,meter_id,from_date,to_date,shift_id,from_time,to_time,duration,meter_status):
    
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
        
        where = ''
        table_name = ''
        if duration == "":
            duration = 1
        
        if meter_id != '':
            where +=f" and mm.meter_id in ({meter_id})"

        if equipment_id != '':
            where +=f" and mm.equipment_id in ({equipment_id}) "

        if meter_status != '':
            where +=f" and cp.meter_status = '{meter_status}'"

        if period_id == 'cur_shift': 
            # query=text(f'''SELECT * FROM ems_v1.master_shifts WHERE status='active' and  plant_id = '{plant_id}' ''')
            # data1 = await cnx.execute(query)
            # data1 = data1.fetchall()
            # mill_date = date.today()
            # mill_shift = 0       
            # if len(data1) > 0:
            #     for shift_record in data1:
            #         mill_date = shift_record["mill_date"]
            #         mill_shift = shift_record["mill_shift"]        
            table_name = 'ems_v1.current_power_analysis cp'
            where += f"and cp.mill_date = '{from_date}' and cp.mill_shift ='{shift_id}' "

        elif period_id == 'sel_shift' or period_id == 'sel_date':
            
            mill_date= from_date
            month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
            table_name=f"ems_v1_completed.power_analysis_{month_year} cp" 
            where += f" and cp.mill_date = '{mill_date}' "

            if period_id == 'sel_shift':
                where += f" and cp.mill_shift ='{shift_id}' " 

        elif period_id == "#previous_shift":   
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp" 

            where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{shift_id}' ''' 

        elif period_id == "#previous_day":             
            
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
            table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp "
            where += f''' and cp.mill_date = '{from_date}' '''  
            
        if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="from_to" or  period_id=="#from_to" or period_id == "#sel_year":
            
            if from_date != '' and to_date != '':
                if from_date.month == to_date.month and from_date.year == to_date.year:
                    month_year = f"{mill_month[from_date.month]}{str(from_date.year)}"
                   
                    result_query = await check_analysis_table(cnx,month_year)
                  
                    if len(result_query) == 0:
                        return _getErrorResponseJson("Analysis table not available...")    
                
                    table_name=f"  ems_v1_completed.power_analysis_{month_year} as cp "
                else:

                    field_name = 'id,meter_id, created_on, mill_date, mill_shift, t_current, r_current, y_current, b_current, vll_avg, ry_volt, yb_volt, br_volt, vln_avg, r_volt, y_volt, b_volt, t_watts, kWh, kvah, kw, kvar, power_factor, r_watts, kva, y_watts, b_watts, avg_powerfactor, r_powerfactor, y_powerfactor, b_powerfactor, powerfactor, frequency, t_voltampere, r_voltampere, y_voltampere, b_voltampere, t_var, r_var, y_var, b_var, master_kwh, machine_kWh,equipment_kwh, meter_status, diff_equipment_on_load_kwh,diff_equipment_idle_kwh,diff_equipment_off_kwh'

            
                    month_year_range = [
                        (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                        for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                    ]
                    
                    union_queries = []
                    

                    for month_year in month_year_range:
                        # query = f"""SELECT table_name FROM information_schema.tables WHERE table_schema = 'ems_v1_completed' AND table_name = 'power_{month_year}'"""
                        result_query = await check_power_table(cnx,month_year)

                        if len(result_query) > 0:
                            table_name = f"ems_v1_completed.power_analysis_{month_year}"
                            union_queries.append(f"SELECT {field_name} FROM {table_name}")
                
                    subquery_union = " UNION ALL ".join(union_queries)
                    table_name = f"( {subquery_union}) cp"
            where += f''' and  cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

        if from_time !='':
            where += f" and DATE_FORMAT(cp.created_on ,'%H:%i:%s')>='{from_time}' "
    
        if to_time !='':
            where += f" and DATE_FORMAT(cp.created_on ,'%H:%i:%s')<='{to_time}' "
        
        groupby = ''
        orderby = ''
        join = ''
        if group_by == '' or group_by == 'meter':
            groupby +=f'mm.meter_id,'  
            orderby +=f'meter_id,' 
            join = f"left join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id"

        if group_by!= '' and group_by == 'equipment':
            where += f" and mm.is_poll_meter = 'yes' and mm.meter = 'equipment' " 
            groupby +=f'me.equipment_id,' 
            orderby +=f'equipment_id,'
            join = f"inner join ems_v1.master_equipment me on me.equipment_id = mm.equipment_id "


        query=text(f'''
            SELECT *
            FROM (
                SELECT 
                (@row_number := @row_number + 1) AS slno,
			    mm.meter_code,
			    mm.meter_name,
                case when mm.is_poll_meter = 'yes' and mm.meter = 'equipment' then mm.meter_name else '' end as equipment_poll_meter_name,
			    cp.meter_id,
                DATE_FORMAT(cp.created_on, '%Y-%m-%dT%H:%i:%s') AS date_time,
			    cp.mill_date,
			    cp.mill_shift,
                me.equipment_code,
                me.equipment_name,
                me.equipment_id,
			    ROUND(AVG(case when mmf.t_current = '*' then cp.t_current * mmf.t_current_value  when mmf.t_current = '/' then cp.t_current / mmf.t_current_value else cp.t_current end),prf.t_current) AS t_current,
                ROUND(AVG(case when mmf.r_current = '*' then cp.r_current * mmf.r_current_value  when mmf.r_current = '/' then cp.r_current / mmf.r_current_value else cp.r_current end),prf.r_current) AS r_current,
			    ROUND(AVG(case when mmf.y_current = '*' then cp.y_current * mmf.y_current_value  when mmf.y_current = '/' then cp.y_current / mmf.y_current_value else cp.y_current end),prf.y_current) AS y_current,
			    ROUND(AVG(case when mmf.b_current = '*' then cp.b_current * mmf.b_current_value  when mmf.b_current = '/' then cp.b_current / mmf.b_current_value else cp.b_current end),prf.b_current) AS b_current,
			    ROUND(AVG(case when mmf.vll_avg = '*' then cp.vll_avg * mmf.vll_avg_value  when mmf.vll_avg = '/' then cp.vll_avg / mmf.vll_avg_value else cp.vll_avg end),prf.vll_avg) AS vll_avg,
			    ROUND(AVG(case when mmf.ry_volt = '*' then cp.ry_volt * mmf.ry_volt_value  when mmf.ry_volt = '/' then cp.ry_volt / mmf.ry_volt_value else cp.ry_volt end),prf.ry_volt) AS ry_volt,
			    ROUND(AVG(case when mmf.yb_volt = '*' then cp.yb_volt * mmf.yb_volt_value  when mmf.yb_volt = '/' then cp.yb_volt / mmf.yb_volt_value else cp.yb_volt end),prf.yb_volt) AS yb_volt,
			    ROUND(AVG(case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value  when mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end),prf.br_volt) AS br_volt,
			    ROUND(AVG(case when mmf.vln_avg = '*' then cp.vln_avg * mmf.vln_avg_value  when mmf.vln_avg = '/' then cp.vln_avg / mmf.vln_avg_value else cp.vln_avg end),prf.vln_avg) AS vln_avg,
			    ROUND(AVG(case when mmf.r_volt = '*' then cp.r_volt * mmf.r_volt_value  when mmf.r_volt = '/' then cp.r_volt / mmf.r_volt_value else cp.r_volt end),prf.r_volt) AS r_volt,
			    ROUND(AVG(case when mmf.y_volt = '*' then cp.y_volt * mmf.y_volt_value  when mmf.y_volt = '/' then cp.y_volt / mmf.y_volt_value else cp.y_volt end),prf.y_volt) AS y_volt,
			    ROUND(AVG(case when mmf.b_volt = '*' then cp.b_volt * mmf.b_volt_value  when mmf.b_volt = '/' then cp.b_volt / mmf.b_volt_value else cp.b_volt end),prf.b_volt) AS b_volt,
			    ROUND(AVG(case when mmf.t_watts = '*' then cp.t_watts * mmf.t_watts_value  when mmf.t_watts = '/' then cp.t_watts / mmf.t_watts_value else cp.t_watts end),prf.t_watts) AS t_watts,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value  when mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end),prf.kWh) AS kWh,
			    ROUND(AVG(case when mmf.kvah = '*' then cp.kvah * mmf.kvah_value  when mmf.kvah = '/' then cp.kvah / mmf.kvah_value else cp.kvah end),prf.kvah) AS kvah,
			    ROUND(AVG(case when mmf.kw = '*' then cp.t_watts * mmf.kw_value  when mmf.kw = '/' then cp.t_watts / mmf.kw_value else cp.t_watts end),prf.kw)  AS kw,
			    ROUND(AVG(case when mmf.kvar = '*' then cp.kvar * mmf.kvar_value  when mmf.kvar = '/' then cp.kvar / mmf.kvar_value else cp.kvar end),prf.kvar) AS kvar,
			    ROUND(AVG(case when mmf.power_factor = '*' then cp.power_factor * mmf.power_factor_value  when mmf.power_factor = '/' then cp.power_factor / mmf.power_factor_value else cp.power_factor end),prf.power_factor) AS power_factor,
			    ROUND(AVG(case when mmf.r_watts = '*' then cp.r_watts * mmf.r_watts_value  when mmf.r_watts = '/' then cp.r_watts / mmf.r_watts_value else cp.r_watts end),prf.r_watts) AS r_watts,
			    ROUND(AVG(case when mmf.kva = '*' then cp.kva * mmf.kva_value  when mmf.kva = '/' then cp.kva / mmf.kva_value else cp.kva end),prf.kva) AS kva,
			    ROUND(AVG(case when mmf.y_watts = '*' then cp.y_watts * mmf.y_watts_value  when mmf.y_watts = '/' then cp.y_watts / mmf.y_watts_value else cp.y_watts end),prf.y_watts) AS y_watts,
			    ROUND(AVG(case when mmf.b_watts = '*' then cp.b_watts * mmf.b_watts_value  when mmf.b_watts = '/' then cp.b_watts / mmf.b_watts_value else cp.b_watts end),prf.b_watts) AS b_watts,
			    ROUND(AVG(case when mmf.avg_powerfactor = '*' then ABS(cp.avg_powerfactor) * mmf.avg_powerfactor_value  when mmf.avg_powerfactor = '/' then ABS(cp.avg_powerfactor) / mmf.avg_powerfactor_value else ABS(cp.avg_powerfactor) end),prf.avg_powerfactor) AS avg_powerfactor,
			    ROUND(AVG(case when mmf.r_powerfactor = '*' then ABS(cp.r_powerfactor) * mmf.r_powerfactor_value  when mmf.r_powerfactor = '/' then ABS(cp.r_powerfactor) / mmf.r_powerfactor_value else ABS(cp.r_powerfactor) end),prf.r_powerfactor) AS r_powerfactor,
			    ROUND(AVG(case when mmf.y_powerfactor = '*' then ABS(cp.y_powerfactor) * mmf.y_powerfactor_value  when mmf.y_powerfactor = '/' then ABS(cp.y_powerfactor) / mmf.y_powerfactor_value else ABS(cp.y_powerfactor) end),prf.y_powerfactor) AS y_powerfactor,
			    ROUND(AVG(case when mmf.b_powerfactor = '*' then ABS(cp.b_powerfactor) * mmf.b_powerfactor_value  when mmf.b_powerfactor = '/' then ABS(cp.b_powerfactor) / mmf.b_powerfactor_value else ABS(cp.b_powerfactor) end),prf.b_powerfactor) AS b_powerfactor,
			    ROUND(AVG(case when mmf.powerfactor = '*' then ABS(cp.powerfactor) * mmf.powerfactor_value  when mmf.powerfactor = '/' then ABS(cp.powerfactor) / mmf.powerfactor_value else ABS(cp.powerfactor) end),prf.powerfactor) AS powerfactor,
			    ROUND(AVG(case when mmf.frequency = '*' then cp.frequency * mmf.frequency_value  when mmf.frequency = '/' then cp.frequency / mmf.frequency_value else cp.frequency end),prf.frequency) AS frequency,
			    ROUND(AVG(case when mmf.t_voltampere = '*' then cp.t_voltampere * mmf.t_voltampere_value  when mmf.t_voltampere = '/' then cp.t_voltampere / mmf.t_voltampere_value else cp.t_voltampere end),prf.t_voltampere) AS t_voltampere,
			    ROUND(AVG(case when mmf.r_voltampere = '*' then cp.r_voltampere * mmf.r_voltampere_value  when mmf.r_voltampere = '/' then cp.r_voltampere / mmf.r_voltampere_value else cp.r_voltampere end),prf.r_voltampere) AS r_voltampere,
			    ROUND(AVG(case when mmf.y_voltampere = '*' then cp.y_voltampere * mmf.y_voltampere_value  when mmf.y_voltampere = '/' then cp.y_voltampere / mmf.y_voltampere_value else cp.y_voltampere end),prf.y_voltampere) AS y_voltampere,
			    ROUND(AVG(case when mmf.b_voltampere = '*' then cp.b_voltampere * mmf.b_voltampere_value  when mmf.b_voltampere = '/' then cp.b_voltampere / mmf.b_voltampere_value else cp.b_voltampere end),prf.b_voltampere) AS b_voltampere,
			    ROUND(AVG(case when mmf.t_var = '*' then cp.t_var * mmf.t_var_value  when mmf.t_var = '/' then cp.t_var / mmf.t_var_value else cp.t_var end),prf.t_var) AS t_var,
			    ROUND(AVG(case when mmf.r_var = '*' then cp.r_var * mmf.r_var_value  when mmf.r_var = '/' then cp.r_var / mmf.r_var_value else cp.r_var end),prf.r_var) AS r_var,
			    ROUND(AVG(case when mmf.y_var = '*' then cp.y_var * mmf.y_var_value  when mmf.y_var = '/' then cp.y_var / mmf.y_var_value else cp.y_var end),prf.y_var) AS y_var,
			    ROUND(AVG(case when mmf.b_var = '*' then cp.b_var * mmf.b_var_value  when mmf.b_var = '/' then cp.b_var / mmf.b_var_value else cp.b_var end),prf.b_var) AS b_var,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value  when mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end),prf.machine_kWh) AS master_kwh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value  when mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end),prf.machine_kWh) AS machine_kWh,
                ROUND(SUM(CASE WHEN mm.meter = 'common' and mm.meter_type = 'primary' THEN  cp.equipment_kwh  ELSE 0 END),prf.kWh) AS pm_common_kwh,
                ROUND(SUM(cp.equipment_kwh),prf.kWh) AS calculated_kwh,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'THEN  cp.diff_equipment_on_load_kwh ELSE 0 END ),prf.kWh) as on_load_kwh,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'THEN  cp.diff_equipment_off_kwh  ELSE 0 END ),prf.kWh) as off_kwh,
                ROUND(SUM(CASE WHEN mm.meter = 'equipment' and mm.is_poll_meter = 'yes'THEN  cp.diff_equipment_idle_kwh ELSE 0 END ),prf.kWh) as idle_kwh,
                SUM(case when mmf.runhour = '*' then cp.runhour * mmf.runhour_value when  mmf.runhour = '/' then cp.runhour / mmf.runhour_value else cp.runhour end ) AS runhour,
                ROUND(AVG(case when mmf.r_volt_thd = '*' then cp.r_volt_thd * mmf.r_volt_thd_value when  mmf.r_volt_thd = '/' then cp.r_volt_thd / mmf.r_volt_thd_value else cp.r_volt_thd end ),prf.r_volt_thd) AS r_volt_thd,
                ROUND(AVG(case when mmf.y_volt_thd = '*' then cp.y_volt_thd * mmf.y_volt_thd_value when  mmf.y_volt_thd = '/' then cp.y_volt_thd / mmf.y_volt_thd_value else cp.y_volt_thd end ),prf.y_volt_thd) AS y_volt_thd,
                ROUND(AVG(case when mmf.b_volt_thd = '*' then cp.b_volt_thd * mmf.b_volt_thd_value when  mmf.b_volt_thd = '/' then cp.b_volt_thd / mmf.b_volt_thd_value else cp.b_volt_thd end ),prf.b_volt_thd) AS b_volt_thd,
                ROUND(AVG(case when mmf.avg_volt_thd = '*' then cp.avg_volt_thd * mmf.avg_volt_thd_value when  mmf.avg_volt_thd = '/' then cp.avg_volt_thd / mmf.avg_volt_thd_value else cp.avg_volt_thd end ),prf.avg_volt_thd) AS avg_volt_thd,
                ROUND(AVG(case when mmf.r_current_thd = '*' then cp.r_current_thd * mmf.r_current_thd_value when  mmf.r_current_thd = '/' then cp.r_current_thd / mmf.r_current_thd_value else cp.r_current_thd end ),prf.r_current_thd) AS r_current_thd,
                ROUND(AVG(case when mmf.y_current_thd = '*' then cp.y_current_thd * mmf.y_current_thd_value when  mmf.y_current_thd = '/' then cp.y_current_thd / mmf.y_current_thd_value else cp.y_current_thd end ),prf.y_current_thd) AS y_current_thd,
                ROUND(AVG(case when mmf.b_current_thd = '*' then cp.b_current_thd * mmf.b_current_thd_value when  mmf.b_current_thd = '/' then cp.b_current_thd / mmf.b_current_thd_value else cp.b_current_thd end ),prf.b_current_thd) AS b_current_thd,
                ROUND(AVG(case when mmf.avg_current_thd = '*' then cp.avg_current_thd * mmf.avg_current_thd_value when  mmf.avg_current_thd = '/' then cp.avg_current_thd / mmf.avg_current_thd_value else cp.avg_current_thd end ),prf.avg_current_thd) AS avg_current_thd,         
                '' as formula,
                '' as tooltip_kwh
		    from (
             SELECT @row_number:=0
            ) AS rn_init,
                {table_name}   

		        inner join ems_v1.master_meter mm on mm.meter_id=cp.meter_id
                inner join ems_v1.master_meter_factor mmf on  mmf.plant_id = mm.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mm.plant_id  
                {join}
                                
		    where 
                1=1  and mm.status = 'active' {where} 
            GROUP BY {groupby} 
                cp.created_on
                ) AS subquery
            WHERE
                slno % {duration} = 0
            
		    order by {orderby} date_time                                
            ''')  
       
        data=await cnx.execute(query)
        data = data.fetchall()
        createFolder("Load_analysis_log/","load_analysis api query.. "+str(query))
        
        return data
  
async def polling_analysis(cnx,period_id ,plant_id, equipment_id,from_date ,to_date ,mill_shift,meter_status):
 
    mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}
    where = '' 
    result_p = ''       
    if equipment_id != '':
        where += f" and me.equipment_id in ({equipment_id})"

    if meter_status != '':
        where += f" and cp.meter_status in ({meter_status})"

    if period_id == "#cur_shift" or period_id == "cur_shift":
        where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{mill_shift}' '''              
        table_name = "ems_v1.current_polling_data cp" 
        
    elif period_id == "#previous_shift":   
        
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        table_name=f"  ems_v1_completed.polling_data_{month_year} as cp" 
    
        where += f''' and cp.mill_date = '{from_date}' AND cp.mill_shift = '{mill_shift}' '''   
        result_p = await check_polling_data_tble(cnx,month_year)

    elif period_id == "#sel_date":

        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        table_name=f"  ems_v1_completed.polling_data_{month_year} as cp "
        where += f''' and cp.mill_date = '{from_date}' '''
        result_query = await check_polling_data_tble(cnx,month_year)

        if len(result_query) >0 :
            field_name = 'meter_id,meter_status,mc_state_changed_time,mill_date,mill_shift,poll_duration,poll_consumption,equipment_consumption, avg_amps,min_amps,max_amps'
            table_name = f'''(select {field_name} from ems_v1.current_polling_data union all select {field_name} from ems_v1_completed.polling_data_{month_year}) as cp'''
    
    elif period_id == "#previous_day":             
        
        month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""
        result_p = await check_polling_data_tble(cnx,month_year)
        table_name=f"  ems_v1_completed.polling_data_{month_year} as cp "
        where += f''' and cp.mill_date = '{from_date}' '''
        
    elif period_id  == "#this_week":
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''

    elif period_id == "#previous_week":
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
    
    elif period_id == "#this_month":
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
 
    elif period_id == "#previous_month":
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
        
    elif period_id=="#this_year": 
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
    
    elif period_id=="#previous_year": 
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
    
    elif period_id=="#sel_year": 
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
    
    elif period_id == "from_to" or period_id == '#from_to':            
        where += f''' and cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
        
    if period_id == '#previous_week' or period_id == "#this_week" or period_id == "#this_month" or period_id == '#previous_month' or period_id=="#previous_year" or period_id=="#this_year" or period_id=="#from_to" or period_id == "#sel_year":
        if from_date != '' and to_date != '':
        
            field_name = 'meter_id,meter_status,mc_state_changed_time,mill_date,mill_shift,poll_duration,poll_consumption,equipment_consumption,avg_amps,min_amps,max_amps'
           
            month_year_range = [
                    (from_date + timedelta(days=31 * i)).strftime("%m%Y")
                    for i in range((to_date.year - from_date.year) * 12 + to_date.month - from_date.month + 1)
                ]
           
            union_queries = []
            joins = []

            for month_year in month_year_range:
                result_p = await check_polling_data_tble(cnx,month_year)

                if len(result_p) > 0:
                    join_p = f"ems_v1_completed.polling_data_{month_year} "
                    joins.append(f"select meter_id,meter_status,mc_state_changed_time,mill_date,mill_shift,poll_duration,poll_consumption,equipment_consumption,avg_amps,min_amps,max_amps from {join_p}")

            subquery_union = " UNION ALL ".join(joins)
            table_name = f"( {subquery_union}) cp"
            

    query = f'''
                select 
                    mm.equipment_id,
                    mm.meter_id,
                    me.equipment_code,
                    me.equipment_name,
                    mm.meter_code,
                    mm.meter_name,
                    cp.*,
                    concat(SEC_TO_TIME(cp.poll_duration)) as time_duration
                   
                from
                    {table_name} 
                inner join master_meter mm on mm.meter_id = cp.meter_id
                inner join master_equipment me on me.equipment_id = mm.equipment_id
                where mm.status = 'active' and mm.is_poll_meter = 'yes' {where} '''
                  
    createFolder("Load_analysis_log/","polling analysis api query.. "+str(query))      
    datas = await cnx.execute(query)
    datas = datas.mappings().all()
    return datas
 

        