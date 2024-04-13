from sqlalchemy import text
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import json
from log_file import createFolder

async def sld_dtl(cnx,sld_type):

        where = ''
        if sld_type == 1:
            where =f' where mm.meter_id in (6,7,8,11,12,13,16,17,18,19,20,21,22,23,24,25,26)'
        if sld_type == 2:
            where =f' where mm.meter_id in (27,28,30,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46)'
        if sld_type == 3:
            where =f' where mm.meter_id in (16,6,28,25,26,41,14)'
        sql = text(f'''
            select 
                mc.company_code,
                mc.company_name,
                mb.bu_code,
                mb.bu_name,
                md.plant_code,
                md.plant_name,
                ms.plant_plant_code,
                ms.plant_plant_name,
                mmt.equipment_group_code,
                mmt.equipment_group_name,
                mf.function_name,
                mf.function_code,
                mm.meter_code,
                mm.meter_name,
                count(mm.meter_name) AS meter_count,
                cp.power_id,
                cp.company_id,
                cp.bu_id,
                cp.plant_id,
                cp.plant_plant_id,
                cp.equipment_group_id,
                mf.function_id,
                cp.meter_id,
                cp.design_id,
                cp.beam_id,
                cp.date_time,
                cp.date_time1,
                cp.mill_date,
                cp.mill_shift,
                ROUND(AVG(case when mmf.vln_avg = '*' then cp.vln_avg * mmf.vln_avg_value when  mmf.vln_avg = '/' then cp.vln_avg / mmf.vln_avg_value else cp.vln_avg end ),prf.vln_avg) AS vln_avg,
                ROUND(AVG(case when mmf.r_volt = '*' then cp.r_volt * mmf.r_volt_value when  mmf.r_volt = '/' then cp.r_volt / mmf.r_volt_value else cp.r_volt end ),prf.r_volt) AS r_volt,
                ROUND(AVG(case when mmf.y_volt = '*' then cp.y_volt * mmf.y_volt_value when  mmf.y_volt = '/' then cp.y_volt / mmf.y_volt_value else cp.y_volt end ),prf.y_volt) AS y_volt,
                ROUND(AVG(case when mmf.b_volt = '*' then cp.b_volt * mmf.b_volt_value when  mmf.b_volt = '/' then cp.b_volt / mmf.b_volt_value else cp.b_volt end ),prf.b_volt) AS b_volt,
                ROUND(AVG(case when mmf.vll_avg = '*' then cp.vll_avg * mmf.vll_avg_value when  mmf.vll_avg = '/' then cp.vll_avg / mmf.vll_avg_value else cp.vll_avg end ),prf.vll_avg) AS vll_avg,
                ROUND(AVG(case when mmf.ry_volt = '*' then cp.ry_volt * mmf.ry_volt_value when  mmf.ry_volt = '/' then cp.ry_volt / mmf.ry_volt_value else cp.ry_volt end ),prf.ry_volt) AS ry_volt,
                ROUND(AVG(case when mmf.yb_volt = '*' then cp.yb_volt * mmf.yb_volt_value when  mmf.yb_volt = '/' then cp.yb_volt / mmf.yb_volt_value else cp.yb_volt end ),prf.yb_volt) AS yb_volt,
                ROUND(AVG(case when mmf.br_volt = '*' then cp.br_volt * mmf.br_volt_value when  mmf.br_volt = '/' then cp.br_volt / mmf.br_volt_value else cp.br_volt end ),prf.br_volt) AS br_volt,
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
                ROUND(AVG(case when mmf.avg_powerfactor = '*' then cp.avg_powerfactor * mmf.avg_powerfactor_value when  mmf.avg_powerfactor = '/' then cp.avg_powerfactor / mmf.avg_powerfactor_value else cp.avg_powerfactor end ),prf.avg_powerfactor) AS avg_powerfactor,
                ROUND(AVG(case when mmf.r_powerfactor = '*' then cp.r_powerfactor * mmf.r_powerfactor_value when  mmf.r_powerfactor = '/' then cp.r_powerfactor / mmf.r_powerfactor_value else cp.r_powerfactor end ),prf.r_powerfactor) AS r_powerfactor,
                ROUND(AVG(case when mmf.y_powerfactor = '*' then cp.y_powerfactor * mmf.y_powerfactor_value when  mmf.y_powerfactor = '/' then cp.y_powerfactor / mmf.y_powerfactor_value else cp.y_powerfactor end ),prf.y_powerfactor) AS y_powerfactor,
                ROUND(AVG(case when mmf.b_powerfactor = '*' then cp.b_powerfactor * mmf.b_powerfactor_value when  mmf.b_powerfactor = '/' then cp.b_powerfactor / mmf.b_powerfactor_value else cp.b_powerfactor end ),prf.b_powerfactor) AS b_powerfactor,
                ROUND(AVG(case when mmf.powerfactor = '*' then cp.powerfactor * mmf.powerfactor_value when  mmf.powerfactor = '/' then cp.powerfactor / mmf.powerfactor_value else cp.powerfactor end ),prf.powerfactor) AS powerfactor,
                
                ROUND(AVG(case when mmf.kvah = '*' then cp.kvah * mmf.kvah_value when  mmf.kvah = '/' then cp.kvah / mmf.kvah_value else cp.kvah end ),prf.kvah) AS kvah,
                ROUND(SUM(case when mmf.kw = '*' then cp.t_watts * mmf.kw_value when  mmf.kw = '/' then cp.t_watts / mmf.kw_value else cp.t_watts end ),prf.kw) AS kw,
                ROUND(AVG(case when mmf.kvar = '*' then cp.kvar * mmf.kvar_value when  mmf.kvar = '/' then cp.kvar / mmf.kvar_value else cp.kvar end ),prf.kvar) AS kvar,
                ROUND(AVG(case when mmf.power_factor = '*' then cp.power_factor * mmf.power_factor_value when  mmf.power_factor = '/' then cp.power_factor / mmf.power_factor_value else cp.power_factor end ),prf.power_factor) AS power_factor,
                ROUND(AVG(case when mmf.kva = '*' then cp.kva * mmf.kva_value when  mmf.kva = '/' then cp.kva / mmf.kva_value else cp.kva end ),prf.kva) AS kva,
                ROUND(AVG(case when mmf.frequency = '*' then cp.frequency * mmf.frequency_value when  mmf.frequency = '/' then cp.frequency / mmf.frequency_value else cp.frequency end ),prf.frequency) AS frequency,
                cp.meter_status,
                cp.status,
                cp.created_on,
                cp.created_by,
                cp.modified_on,
                cp.modified_by,
                
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ),prf.machine_kWh) AS machine_kWh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ),prf.machine_kWh) AS master_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.kWh * mmf.kWh_value when  mmf.kWh = '/' then cp.kWh / mmf.kWh_value else cp.kWh end ),prf.kWh) AS kWh,
                 
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_machine_kWh / mmf.machine_kWh_value else cp.reverse_machine_kWh end ),prf.machine_kWh) AS reverse_machine_kWh,
                ROUND(SUM(case when mmf.machine_kWh = '*' then cp.reverse_master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.reverse_master_kwh / mmf.machine_kWh_value else cp.reverse_master_kwh end ),prf.machine_kWh) AS reverse_master_kwh,
                ROUND(SUM(case when mmf.kWh = '*' then cp.reverse_kwh * mmf.kWh_value when  mmf.kWh = '/' then cp.reverse_kwh / mmf.kWh_value else cp.reverse_kwh end ),prf.kWh) AS reverse_kwh,
                
                mm.ip_address,
                mm.port,
                CASE WHEN cp.date_time <= DATE_SUB(NOW(), INTERVAL 2 MINUTE) THEN 'S' ELSE 'N' END AS nocom,         
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),prf.kWh) AS kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),prf.kWh) AS kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.kWh = '*' then cp.kwh * mmf.kwh_value when  mmf.kwh = '/' then cp.kwh / mmf.kwh_value else cp.kwh end ELSE 0 END),prf.kWh) AS kwh_3,
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ELSE 0 END),prf.machine_kWh) AS start_kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ELSE 0 END),prf.machine_kWh) AS start_kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kWh = '*' then cp.master_kwh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.master_kwh / mmf.machine_kWh_value else cp.master_kwh end ELSE 0 END),prf.machine_kWh) AS start_kwh_3,     
                ROUND(SUM(CASE WHEN cp.mill_shift = 1 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ELSE 0 END),prf.machine_kWh) AS end_kwh_1,
                ROUND(SUM(CASE WHEN cp.mill_shift = 2 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ELSE 0 END),prf.machine_kWh) AS end_kwh_2,
                ROUND(SUM(CASE WHEN cp.mill_shift = 3 THEN case when mmf.machine_kWh = '*' then cp.machine_kWh * mmf.machine_kWh_value when  mmf.machine_kWh = '/' then cp.machine_kWh / mmf.machine_kWh_value else cp.machine_kWh end ELSE 0 END),prf.machine_kWh) AS end_kwh_3                    
            from
                current_power cp
                INNER JOIN master_meter mm ON cp.meter_id = mm.meter_id
                INNER JOIN master_company mc ON mm.company_id = mc.company_id
                INNER JOIN master_business_unit mb ON mm.bu_id = mb.bu_id
                INNER JOIN master_plant md ON mm.plant_id = md.plant_id
                INNER JOIN master_plant_plant ms ON mm.plant_plant_id = ms.plant_plant_id
                INNER JOIN master_equipment_group mmt ON mm.equipment_group_id = mmt.equipment_group_id 
                LEFT JOIN master_function mf ON mm.function_id = mf.function_id
                LEFT JOIN master_function mff ON mm.function2_id = mff.function_id
                inner JOIN master_meter_factor mmf ON mm.meter_id = mmf.meter_id
                inner JOIN master_parameter_roundoff prf ON mm.plant_id = prf.plant_id 
            {where} 
            group by cp.meter_id
            ''')
        print(sql)
        data =await cnx.execute(sql)
        data = data.fetchall()
                
        return data

    
