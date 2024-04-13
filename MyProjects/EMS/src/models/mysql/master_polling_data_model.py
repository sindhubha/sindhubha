from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text
import json

async def  pollingtime_list(cnx,meter_id,campus_id,company_id,bu_id,plant_id):

        where = ''
        if meter_id != '':
            where += f" and  mm.meter_id = {meter_id}"

        if campus_id != '' and campus_id != "0":
            where += f" and  mp.campus_id = {campus_id}"

        if company_id != '' and company_id != "0":
            where += f" and  mp.company_id = {company_id}"

        if bu_id != '' and bu_id != "0":
            where += f" and  mp.bu_id = {bu_id}"

        if plant_id != '' and plant_id != "0":
            where += f" and  mp.plant_id = {plant_id}"
        
        main_query = text(f'''
            SELECT
                mm.meter_name,
                mm.meter_code,
                me.equipment_id,
                me.equipment_code,
                me.equipment_name,
                mm.meter_order as meter_order,
                mm.meter_state_condition1,
                mm.meter_state_condition2,
                mm.meter_state_condition3,
                mm.meter_state_condition4,
                mm.meter_state_condition5,
                mm.meter_state_condition6,
                GROUP_CONCAT(mm.meter_id SEPARATOR ',') AS meter_ids,
                mm.*
            FROM
                master_meter mm
                left join master_equipment_meter mem on mem.meter_id = mm.meter_id
                left join master_equipment_calculations mec on mec.equipment_id = mem.equipment_id
                inner join master_equipment me on me.equipment_id = mm.equipment_id
                inner join master_plant mp on mp.plant_id = mm.plant_id
            where mm.meter_type = 'Primary' and mm.status = 'active'  and me.status = 'active'
            {where} 
            GROUP BY
                mm.meter_id
            order by 
                mm.meter_order,
                mm.meter_state_condition1 asc
        ''')

        # Execute the main query
        data = await cnx.execute(main_query)
        data = data.fetchall()      

        result = []

        for row in data:
            meter_ids = row["meter_ids"]
            meter_id_list = meter_ids.split(",")
            meter_dtl = []

            for meter_id in meter_id_list:
                sub_query = text(f'''
                    SELECT meter_name
                    FROM master_meter
                    WHERE meter_id = {meter_id}
                ''')

                sub_data = await cnx.execute(sub_query)
                sub_data = sub_data.fetchall()

                for sub_row in sub_data:
                    meter_dtl.append(sub_row['meter_name'])

            new_row = dict(row)
            new_row["meter_dtl"] = '\n'.join(meter_dtl)
            result.append(new_row)

        return result
    
async def polling_timeentry(cnx,meter_id,meter_state_condition1,meter_state_condition2,meter_state_condition3,meter_state_condition4,meter_state_condition5,meter_state_condition6):

        
        obj_data = json.loads(meter_id)
        for row in obj_data:
            meter_id = row["meter_id"]
            query = text(f''' 
                update 
                    ems_v1.master_meter 
                set 
                    meter_state_condition1 = {meter_state_condition1},
                    meter_state_condition2 = {meter_state_condition2},
                    meter_state_condition3 = {meter_state_condition3},
                    meter_state_condition4 = {meter_state_condition4},
                    meter_state_condition5 = {meter_state_condition5},
                    meter_state_condition6 = {meter_state_condition6}
                where meter_id = {meter_id}
                ''')
            await cnx.execute(query)
            await cnx.commit()
            print(query)

        
