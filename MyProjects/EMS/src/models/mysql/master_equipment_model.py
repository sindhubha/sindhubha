from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
import datetime
from sqlalchemy import text
from datetime import datetime,timedelta,date

async def get_machine_dtl(cnx, plc_ip_address):
    where = ""
    orderby = ""
    
    if plc_ip_address != "":
        where += f"and mt.tab_ip_address = '{plc_ip_address}'"
    orderby += "mt.equipment_id"

    query=f''' SELECT
			mc.company_code,
			mc.company_name,
			CONCAT(mc.company_code, ' - ', mc.company_name) AS company_actual,
			mb.bu_code,
			mb.bu_name,
			CONCAT(mb.bu_code, ' - ', mb.bu_name) AS bu_actual,
			mp.plant_code,
			mp.plant_name,
			CONCAT(mp.plant_code, ' - ', mp.plant_name) AS plant_actual,
			md.plant_department_id AS department_id,
			md.plant_department_code AS department_code,
			md.plant_department_name AS department_name,
			CONCAT(md.plant_department_code, ' - ', md.plant_department_name) AS department_actual,
			meg.equipment_group_code,
			meg.equipment_group_name,
			CONCAT(meg.equipment_group_code, ' - ', meg.equipment_group_name) AS equipment_group_actual,
			mec.equipment_class_code,
			mec.equipment_class_name,
			CONCAT(mec.equipment_class_code, ' - ', mec.equipment_class_name) AS equipment_class_actual,
			mpt.mfg_process_type_code,
			mpt.mfg_process_type_name,
			CONCAT(mpt.mfg_process_type_code, ' - ', mpt.mfg_process_type_name) AS mfg_actual,
			mt.ip_address AS plc_ip_address,
			mt.tab_ip_address AS tablet_ip_address,
            mp.is_subcategory,
			mt.*
		FROM
			master_equipment mt
			INNER JOIN master_company mc ON mc.company_id = mt.company_id
			INNER JOIN master_business_unit mb ON mb.bu_id = mt.bu_id
			INNER JOIN master_plant mp ON mp.plant_id = mt.plant_id
			INNER JOIN master_plant_wise_department md ON md.plant_department_id=mt.plant_department_id
			INNER JOIN master_equipment_group meg ON meg.equipment_group_id=mt.equipment_group_id
			INNER JOIN master_equipment_class mec ON mec.equipment_class_id=mt.equipment_class_id
			INNER JOIN master_mfg_process_type mpt ON mpt.mfg_process_type_id=mt.mfg_process_type_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def equipment_Lists(cnx, equipment_id, bu_id = '', company_id = '', plant_id = ''):

    # DATE_FORMAT(mt.shift1_start_time,'%H:%i') AS a_shift_start_time,
    #         DATE_FORMAT(mt.shift2_start_time,'%H:%i') AS b_shift_start_time,
    #         DATE_FORMAT(mt.shift3_start_time,'%H:%i') AS c_shift_start_time,
    #         DATE_FORMAT(mt.shift1_end_time,'%H:%i') AS a_shift_end_time,
    #         DATE_FORMAT(mt.shift2_end_time,'%H:%i') AS b_shift_end_time,
    #         DATE_FORMAT(mt.shift3_end_time,'%H:%i') AS c_shift_end_time,
    where = ""
    orderby = ""
    
    if equipment_id != "" and equipment_id != "0":
        where += f"and mt.equipment_id = '{equipment_id}'"

    if company_id != "" and company_id != "0":
        where += f''' and mt.company_id = '{company_id}' '''

    if bu_id != "" and bu_id != "0":
        where += f''' and mt.bu_id = '{bu_id}' '''
        
    if plant_id != "" and plant_id != "0":
        where += f''' and mt.plant_id = '{plant_id}' '''

    orderby += "mt.equipment_id"

    query=f''' SELECT
			mt.*,
			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name,
            IFNULL(CONCAT(mpd.plant_department_code, '-', mpd.plant_department_name), '') AS department_name,
            IFNULL(CONCAT(meg.equipment_group_code, '-', meg.equipment_group_name), '') AS equipment_group_name,
			mec.equipment_class_code
		FROM
            master_equipment mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			INNER JOIN master_company mc on mc.company_id = mt.company_id
            INNER JOIN master_business_unit mb on mb.bu_id = mt.bu_id
            INNER JOIN master_plant mp on mp.plant_id = mt.plant_id
			INNER JOIN master_equipment_class mec ON mec.equipment_class_id=mt.equipment_class_id
            inner join master_plant_wise_department mpd on mpd.plant_department_id=mt.plant_department_id
            inner join master_equipment_group meg on meg.equipment_group_id=mt.equipment_group_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    # print("e_list")
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def getequipmentdtl(cnx, equipment_id, company_name, bu_name, plant_name, equipment_code, equipment_name):
    where=""

    if equipment_id != "":
        where += f"and equipment_id <> '{equipment_id}' "
      
    query=f'''select * from master_equipment where 1=1 and status<>'delete' and company_id = '{company_name}' and bu_id = '{bu_name}' and plant_id = '{plant_name}' and equipment_code='{equipment_code}' {where}'''
    print(query)
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def gettabletip(cnx, equipment_id, tablet_ip):
    where=""

    if equipment_id != "":
        where += f"and equipment_id <> '{equipment_id}' "
      
    query=f'''select * from master_equipment where 1=1 and status<>'delete' and tab_ip_address='{tablet_ip}' {where}'''

    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def getiotplcip(cnx, equipment_id, iot_plc_ip):
    where=""

    if equipment_id != "":
        where += f"and equipment_id <> '{equipment_id}' "
      
    query=f'''select * from master_equipment where 1=1 and status<>'delete' and ip_address='{iot_plc_ip}' {where}'''

    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def save_equipment(cnx, equipment_code, equipment_name, company_name, bu_name, plant_name, department_name, equipment_group_name, equipment_class_name, processtype_name, integrated_tablet_ip, tablet_ip, tablet_ip_1, integrated_line_name, iot_plc_ip, mc_capacity, mc_max_load,is_configuration, user_login_id):
    query= f'''insert into master_equipment(equipment_code,equipment_name,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,equipment_class_id,mfg_process_type_id,integrated_tab_ip_address,tab_ip_address,tab_ip_1_address,integrated_line_name,ip_address,mc_capacity,mc_max_load,created_on,created_by)
               values('{equipment_code}','{equipment_name}','{company_name}','{bu_name}','{plant_name}','{department_name}','{equipment_group_name}','{equipment_class_name}','{processtype_name}','{integrated_tablet_ip}','{tablet_ip}','{tablet_ip_1}','{integrated_line_name}','{iot_plc_ip}','{mc_capacity}','{mc_max_load}',now(),'{user_login_id}')
      '''
    await cnx.execute(text(query))
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()
    
    query1=f'''select * from master_shifts where company_id = '{company_name}' and bu_id = '{bu_name}' and plant_id = '{plant_name}' '''
    shifts = await cnx.execute(text(query1))
    shifts = shifts.fetchall()

    mill_date = date.today()
    mill_shift = '1'

    for row in shifts:
        mill_date = row["mill_date"]
        mill_shift = row["mill_shift"]
    
    if mill_date != "" and mill_shift != "":
        query2=f''' select * from current_production where machine_id='{insert_id}' '''
        result = await cnx.execute(text(query2))
        result = result.fetchall()

        if len(result) == 0 :
            query3=f''' insert into current_production(company_id,department_id,machine_id,product_id,supervisor_id,operator1_id,machine_status,mill_date,mill_shift,product_end_time,created_on,created_by)
                        values('{company_name}','{department_name}','{insert_id}','50','7','6','1','{mill_date}','{mill_shift}',now(),now(),'{user_login_id}')'''

            await cnx.execute(text(query3))
            await cnx.commit()

            query4=f''' update master_equipment set equipment_order=equipment_id where equipment_id='{insert_id}' '''
            await cnx.execute(text(query4))
            await cnx.commit()
            
			
    await update_plant_wise_sync(cnx, 'master_equipment')
    return insert_id

async def update_equipment(cnx, equipment_id, equipment_code, equipment_name, company_name, bu_name, plant_name, department_name, equipment_group_name, equipment_class_name, processtype_name, integrated_tablet_ip, tablet_ip, tablet_ip_1, integrated_line_name, iot_plc_ip, mc_capacity, mc_max_load, user_login_id):
    query=f''' UPDATE 
                    master_equipment
                set 
                    equipment_code = '{equipment_code}',
                    equipment_name = '{equipment_name}',
                    company_id = '{company_name}',
                    bu_id = '{bu_name}',
                    plant_id = '{plant_name}',
                    plant_department_id = '{department_name}',
                    equipment_group_id = '{equipment_group_name}',
                    equipment_class_id = '{equipment_class_name}',
                    mfg_process_type_id = '{processtype_name}',
                    integrated_tab_ip_address = '{integrated_tablet_ip}',
                    tab_ip_address = '{tablet_ip}',
                    tab_ip_1_address = '{tablet_ip_1}',
                    integrated_line_name = '{integrated_line_name}',
                    ip_address = '{iot_plc_ip}',
                    mc_capacity = '{mc_capacity}',
                    mc_max_load = '{mc_max_load}',
                    sync_status = 'update',
                    modified_on = NOW(),
                    modified_by = '{user_login_id}' 
                WHERE
                    equipment_id = '{equipment_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    insert_id = equipment_id

    query1=f'''select * from master_shifts where company_id = '{company_name}' and bu_id = '{bu_name}' and plant_id = '{plant_name}' '''
    shifts = await cnx.execute(text(query1))
    shifts = shifts.fetchall()

    mill_date = date.today()
    mill_shift = '1'

    for row in shifts:
        mill_date = row["mill_date"]
        mill_shift = row["mill_shift"]
    
    if mill_date != "" and mill_shift != "":
        query2=f''' select * from current_production where machine_id='{insert_id}' '''
        result = await cnx.execute(text(query2))
        result = result.fetchall()

        if len(result) == 0 :
            query3=f''' insert into current_production(company_id,department_id,machine_id,product_id,supervisor_id,operator1_id,machine_status,mill_date,mill_shift,product_end_time,created_on,created_by)
                        values('{company_name}','{department_name}','{insert_id}','50','7','6','1','{mill_date}','{mill_shift}',now(),now(),'{user_login_id}')'''

            await cnx.execute(text(query3))
            await cnx.commit()

            query4=f''' updaate master_equipment set equipment_order=equipment_id where equipment_id='{insert_id}' '''
            await cnx.execute(query4)
            await cnx.commit()
            
    await update_plant_wise_sync(cnx, 'master_equipment')


async def update_equipmentStatus(cnx, equipment_id, status='delete'):
    
      query=f''' Update master_equipment Set sync_status = 'update',status = '{status}' Where equipment_id='{equipment_id}' '''
      
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_equipment')

async def changestatus_equipment(cnx, equipment_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_equipment Set sync_status = 'update',status = '{status}' Where equipment_id='{equipment_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_equipment')

async def get_equipment_name(cnx, bu_id, company_id, plant_id):
    
    query = f''' select * from master_equipment where 1=1 and status = 'active'  '''

    if company_id != "":
        query += f''' and company_id = '{company_id}' '''
    if bu_id != "":
        query += f''' and bu_id = '{bu_id}' '''
    if plant_id != "":
        query += f''' and plant_id = '{plant_id}' '''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def equipment_interlock_update(cnx, equipment_id, interlock_status, col_name='m_interlock_status'):
    if interlock_status == 'yes':
        interlock_status = 'no'
    elif interlock_status == 'no':
        interlock_status = 'yes'
    
    query = f''' Update master_equipment Set sync_status = 'update', {col_name} = '{interlock_status}',m_interlock_write='1' Where equipment_id='{equipment_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    
    await update_plant_wise_sync(cnx, 'master_equipment')

async def equipment_hb_ht_mt_update(cnx, equipment_id, heart_beat_time, handling_time, minor_stoppage_time, communicate_type,kep_id, run_bit_tagname, error_bit_tagname, interlock_bit_name, iiot_bypass_bit_name, product_start_status):
    query=f''' Update master_equipment Set sync_status = 'update',tab_refresh_rate='5000',m_heart_beat_time = '{heart_beat_time}',m_handling_time = '{handling_time}',m_minor_stoppage_time = '{minor_stoppage_time}',communicate_type = '{communicate_type}',kep_id = '{kep_id}',run_bit_tagname = '{run_bit_tagname}',error_bit_tagname = '{error_bit_tagname}',interlock_bit_name='{interlock_bit_name}',iiot_bypass_bit_name='{iiot_bypass_bit_name}',m_time_write='1',product_start_status='{product_start_status}' Where equipment_id='{equipment_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    
    await update_plant_wise_sync(cnx, 'master_equipment')

    if communicate_type == "KEP":
        query1=f''' select * from master_equipment_linespeed where machine_id='{equipment_id}' '''
        result = await cnx.execute(text(query1))
        result = result.fetchall()

        if len(result) == 0:
            result1=equipment_Lists(equipment_id)
            plant_id = 0
            for row in result1:
                plant_id = row[plant_id]
            query2=f''' insert into master_equipment_linespeed(plant_id,machine_id,kep_id)
                        values('{plant_id}','{equipment_id}','{kep_id}')'''
            await cnx.execute(text(query2))
            await cnx.commit()
            await update_plant_wise_sync(cnx, 'master_equipment_linespeed')

async def equipment_linespeed_Lists(cnx, equipment_id):
    where = ""

    if equipment_id != "":
        where+=f''' and me.equipment_id = '{equipment_id}' '''
    
    query = f''' SELECT
					IFNULL(CONCAT(mc.company_code,'-',mc.company_name),'') AS company_name,
					IFNULL(CONCAT(mb.bu_code,'-',mb.bu_name),'') AS bu_name,
					IFNULL(CONCAT(mp.plant_code,'-',mp.plant_name),'') AS plant_name,
					me.equipment_id,me.equipment_code,me.equipment_name,
					mel.id,
					mel.select_parameters,
					mel.where_parameters
				FROM
					master_equipment_linespeed mel
					INNER JOIN master_equipment me ON me.equipment_id=mel.machine_id
					INNER JOIN master_company mc ON mc.company_id = me.company_id
					INNER JOIN master_business_unit mb ON mb.bu_id = me.bu_id
					INNER JOIN master_plant mp ON mp.plant_id = me.plant_id
				WHERE 1=1 {where}
				ORDER BY me.company_id,me.bu_id,me.plant_id,me.equipment_order  '''
    
    data=await cnx.execute(query)
    data=data.fetchall()
    
    return data

async def equipment_is_configured_update(cnx, id, interlock_status, col_name='is_configured'):
    if interlock_status == 'yes':
        interlock_status = 'no'
    elif interlock_status == 'no':
        interlock_status = 'yes'


    query = f''' Update master_equipment_linespeed Set sync_status = 'update',{col_name} = '{interlock_status}' Where id='{id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_equipment')


async def equipment_linespeed_update(cnx, id, select_parameters, where_parameters):
    
    query = f''' Update master_equipment_linespeed Set sync_status = 'update',select_parameters = '{select_parameters}',where_parameters = '{where_parameters}' Where id='{id}' '''
    await cnx.execute(text(query))
    await cnx.commit()

    


