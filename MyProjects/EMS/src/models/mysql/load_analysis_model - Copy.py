from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from src.models.parse_date import parse_date
from datetime import date,timedelta
from log_file import createFolder

async def load_analysis_mdl(cnx,company_id,bu_id,plant_id,period_id,group_by,equipment_id,meter_id,from_date,to_date,shift_id,from_time,to_time,duration):
    
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}    
        
        where = ''
        table_name = ''
        if duration == "":
            duration = 1
        
        if meter_id != '':
            where +=f" and cp.meter_id in ({meter_id})"

        if equipment_id != '':
            where +=f" and mem.equipment_id in ({equipment_id}) and me.status = 'active'"

        if period_id == 'cur_shift': 
            query=text(f'''SELECT * FROM ems_v1.master_shifts WHERE status='active' and  plant_id = '{plant_id}' ''')
            data1 = await cnx.execute(query)
            data1 = data1.fetchall()
            mill_date = date.today()
            mill_shift = 0       
            if len(data1) > 0:
                for shift_record in data1:
                    mill_date = shift_record["mill_date"]
                    mill_shift = shift_record["mill_shift"]  
                        
            table_name = 'ems_v1.current_power_analysis cp'
            where += f"and cp.mill_date = '{mill_date}' and cp.mill_shift ='{mill_shift}' "

        elif period_id == 'sel_shift' or period_id == 'sel_date':
            
            mill_date= from_date
            month_year=f"""{mill_month[mill_date.month]}{str(mill_date.year)}"""
            table_name=f"ems_v1_completed.power_analysis_{month_year}" 
            where += f" and cp.mill_date = '{mill_date}' "

            field_name = 'id,meter_id, created_on, mill_date, mill_shift, t_current, r_current, y_current, b_current, vll_avg, ry_volt, yb_volt, br_volt, vln_avg, r_volt, y_volt, b_volt, t_watts, kWh, kvah, kw, kvar, power_factor, r_watts, kva, y_watts, b_watts, avg_powerfactor, r_powerfactor, y_powerfactor, b_powerfactor, powerfactor, kwh_actual, frequency, t_voltampere, r_voltampere, y_voltampere, b_voltampere, t_var, r_var, y_var, b_var, master_kwh, machine_kWh'
            table_name = f'(select {field_name} from ems_v1.current_power_analysis UNION All select {field_name} from {table_name})cp'

            if period_id == 'sel_shift':
                where += f" and cp.mill_shift ='{shift_id}' "  
   
        elif period_id == "from_to":            
                    
            month_year=f"""{mill_month[from_date.month]}{str(from_date.year)}"""       
        
            where += f''' and  cp.mill_date  >= '{from_date}' and cp.mill_date <= '{to_date}' '''
            
            if shift_id != "":                
                where += f''' and cp.mill_shift = '{shift_id}' ''' 
            field_name = 'id,meter_id, created_on, mill_date, mill_shift, t_current, r_current, y_current, b_current, vll_avg, ry_volt, yb_volt, br_volt, vln_avg, r_volt, y_volt, b_volt, t_watts, kWh, kvah, kw, kvar, power_factor, r_watts, kva, y_watts, b_watts, avg_powerfactor, r_powerfactor, y_powerfactor, b_powerfactor, powerfactor, kwh_actual, frequency, t_voltampere, r_voltampere, y_voltampere, b_voltampere, t_var, r_var, y_var, b_var, master_kwh, machine_kWh'
            
            if from_date.month == to_date.month:
       
                table_name=f"ems_v1_completed.power_analysis_{month_year}" 
                table_name = f'(select {field_name} from ems_v1.[current_power_analysis] UNION All select {field_name} from {table_name})cp'
            else:
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
                table_name = f"(SELECT {field_name} FROM ems_v1.current_power_analysis UNION ALL {subquery_union}) cp"

        if from_time !='':
            where += f" and FORMAT(cp.created_on ,'HH:mm:ss')>='{from_time}' "
    
        if to_time !='':
            where += f" and FORMAT(cp.created_on ,'HH:mm:ss')<='{to_time}' "
        
        groupby = ''
        orderby = ''

        if group_by!= '' and group_by == 'meter':
            groupby +=f'mm.meter_id,'  
            orderby +=f'meter_id,' 

        if group_by!= '' and group_by == 'equipment':
            groupby +=f'me.equipment_id,' 
            orderby +=f'equipment_id,' 


        query=text(f'''
            SELECT *
            FROM (
                SELECT 
                (@row_number := @row_number + 1) AS slno,
			    mm.meter_code,
			    mm.meter_name,
			    cp.meter_id,
                DATE_FORMAT(cp.created_on, '%Y-%m-%dT%H:%i:%s') AS date_time,
			    cp.mill_date,
			    cp.mill_shift,
                me.equipment_code,
                me.equipment_name,
                mem.equipment_id,
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
			    ROUND(AVG(case when mmf.avg_powerfactor = '*' then cp.avg_powerfactor * mmf.avg_powerfactor_value  when mmf.avg_powerfactor = '/' then cp.avg_powerfactor / mmf.avg_powerfactor_value else cp.avg_powerfactor end),prf.avg_powerfactor) AS avg_powerfactor,
			    ROUND(AVG(case when mmf.r_powerfactor = '*' then cp.r_powerfactor * mmf.r_powerfactor_value  when mmf.r_powerfactor = '/' then cp.r_powerfactor / mmf.r_powerfactor_value else cp.r_powerfactor end),prf.r_powerfactor) AS r_powerfactor,
			    ROUND(AVG(case when mmf.y_powerfactor = '*' then cp.y_powerfactor * mmf.y_powerfactor_value  when mmf.y_powerfactor = '/' then cp.y_powerfactor / mmf.y_powerfactor_value else cp.y_powerfactor end),prf.y_powerfactor) AS y_powerfactor,
			    ROUND(AVG(case when mmf.b_powerfactor = '*' then cp.b_powerfactor * mmf.b_powerfactor_value  when mmf.b_powerfactor = '/' then cp.b_powerfactor / mmf.b_powerfactor_value else cp.b_powerfactor end),prf.b_powerfactor) AS b_powerfactor,
			    ROUND(AVG(case when mmf.powerfactor = '*' then cp.powerfactor * mmf.powerfactor_value  when mmf.powerfactor = '/' then cp.powerfactor / mmf.powerfactor_value else cp.powerfactor end),prf.powerfactor) AS powerfactor,
			    ROUND(SUM(case when mmf.kWh = '*' then cp.kwh_actual * mmf.kWh_value  when mmf.kWh = '/' then cp.kwh_actual / mmf.kWh_value else cp.kwh_actual end),prf.kWh) AS kwh_actual,
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
                '' as formula,
                '' as tooltip_kwh
		    from (
             SELECT @row_number:=0
            ) AS rn_init,
                {table_name}   

		        inner join ems_v1.master_meter mm on mm.meter_id=cp.meter_id
                inner join ems_v1.master_meter_factor mmf on  mmf.plant_id = mm.plant_id AND mmf.meter_id = mm.meter_id
                inner JOIN ems_v1.master_parameter_roundoff prf ON prf.plant_id = mm.plant_id  
                left join ems_v1.master_equipment_meter mem on mem.meter_id = mm.meter_id
                left join ems_v1.master_equipment me on me.equipment_id = mem.equipment_id                
		    where 
                1=1 {where} 
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
        # label = {}
        # meter_data = {}
        # org_data = []
        # for d in data:
        #     meter_id = d['meter_id']
        #     meter_name = d['meter_name']
        #     if meter_id not in label:        
        #         label[meter_id] = meter_name
        #     if meter_id not in meter_data:
        #         meter_data[meter_id] = []

        #     # set meter_data for meter_id
        #     temp = {
        #         'date_time': d['date_time'],
        #         't_current': d['t_current'],
        #         'r_current': d['r_current'],
        #         'y_current': d['y_current'],
        #         'b_current': d['b_current'],
        #         'vll_avg': d['vll_avg'],
        #         'ry_volt': d['ry_volt'],
        #         'yb_volt': d['yb_volt'],
        #         'br_volt': d['br_volt'],
        #         'vln_avg': d['vln_avg'],
        #         'r_volt': d['r_volt'],
        #         'y_volt': d['y_volt'],
        #         'b_volt': d['b_volt'],
        #         't_watts': d['t_watts'],
        #         'kWh': d['kWh'],
        #         'kvah': d['kvah'],
        #         'kw': d['kw'],
        #         'kvar': d['kvar'],
        #         'power_factor': d['power_factor'],
        #         'r_watts': d['r_watts'],
        #         'kva': d['kva'],
        #         'y_watts': d['y_watts'],
        #         'b_watts': d['b_watts'],
        #         'avg_powerfactor': d['avg_powerfactor'],
        #         'r_powerfactor': d['r_powerfactor'],
        #         'y_powerfactor': d['y_powerfactor'],
        #         'b_powerfactor': d['b_powerfactor'],
        #         'powerfactor': d['powerfactor'],
        #         'kwh_actual': d['kwh_actual'],
        #         'frequency': d['frequency'],
        #         't_voltampere': d['t_voltampere'],
        #         'r_voltampere': d['r_voltampere'],
        #         'y_voltampere': d['y_voltampere'],
        #         'b_voltampere': d['b_voltampere'],
        #         't_var': d['t_var'],
        #         'r_var': d['r_var'],
        #         'y_var': d['y_var'],
        #         'b_var': d['b_var'],
        #         'master_kwh':d['master_kwh'],
        #         'machine_kWh':d['machine_kWh']
        #     }

        #     meter_data[meter_id].append(temp)

        # for key, value in meter_data.items():
        #     org_data.append({'label': label[key], 'data': value})

        # return {"data":org_data,"data1":data}
        return data
  