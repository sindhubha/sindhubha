from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text

async def shift_Lists(cnx, shift_id,plant_id, bu_id, company_id):
    where = ""
    orderby = ""
    
    if shift_id != "":
        where += f"and mt.shift_id = '{shift_id}' "
    if bu_id != "" and bu_id != "0":
        where += f''' and mt.bu_id = '{bu_id}' '''
    if company_id != "" and company_id != "0":
        where += f''' and mt.company_id = '{company_id}' '''
    if plant_id != "" and plant_id != "0":
        where += f''' and mt.plant_id = '{plant_id}' '''
    
    orderby += "mt.shift_id"

    query=f''' SELECT
			mt.*, DATE_FORMAT(mt.shift1_start_time,'%H:%i') AS a_shift_start_time,
			DATE_FORMAT(mt.shift2_start_time,'%H:%i') AS b_shift_start_time,
			DATE_FORMAT(mt.shift3_start_time,'%H:%i') AS c_shift_start_time,
			DATE_FORMAT(mt.shift1_end_time,'%H:%i') AS a_shift_end_time,
			DATE_FORMAT(mt.shift2_end_time,'%H:%i') AS b_shift_end_time,
			DATE_FORMAT(mt.shift3_end_time,'%H:%i') AS c_shift_end_time,

            DATE_FORMAT(mt.mill_date,'%d-%m-%Y') as shiftmilldate,
            DATE_FORMAT(mt.shift1_start_time,'%h:%i %p') AS shift1_start_time_d,
            DATE_FORMAT(mt.shift2_start_time,'%h:%i %p') AS shift2_start_time_d,
            DATE_FORMAT(mt.shift3_start_time,'%h:%i %p') AS shift3_start_time_d,

			ifnull(concat(cu.employee_code,'-',cu.employee_name),'') as created_user,
			ifnull(concat(mu.employee_code,'-',mu.employee_name),'') as modified_user,
			ifnull(concat(mc.company_code,'-',mc.company_name),'') as company_name,
            ifnull(concat(mb.bu_code,'-',mb.bu_name),'') as bu_name,
            ifnull(concat(mp.plant_code,'-',mp.plant_name),'') as plant_name
		FROM
			master_shifts mt
			left join master_employee cu on cu.employee_id=mt.created_by
			left join master_employee mu on mu.employee_id=mt.modified_by
			left join master_company mc on mc.company_id = mt.company_id
            left join master_business_unit mb on mb.bu_id = mt.bu_id
            left join master_plant mp on mp.plant_id = mt.plant_id
		WHERE mt.status <> 'delete' {where} 
        ORDER BY {orderby}'''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()
    # print(result)
    return result

async def getshiftdtl(cnx, shift_id, plant_name, company_name, bu_name):
    where=""

    if shift_id != "":
        where += f"and shift_id <> '{shift_id}' "      
    query=f'''select * from master_shifts where 1=1 and status<>'delete' and company_id ='{company_name}' AND plant_id ='{plant_name}' AND bu_id ='{bu_name}' '''
    result = await cnx.execute(text(query))
    result = result.fetchall()
    
    return result

async def save_shift(cnx, plant_name, no_of_shifts, company_name, bu_name, a_shift_start_time, b_shift_start_time, c_shift_start_time, a_shift_end_time, b_shift_end_time, c_shift_end_time, user_login_id):
    
    shift_dtl = ''
    shift_dtl_rec = ''

    if no_of_shifts == "3":
        shift_dtl =  f"shift1_start_time,shift2_start_time,shift3_start_time,shift1_end_time,shift2_end_time,shift3_end_time," 
        shift_dtl_rec =  f"CONCAT('1900-01-01  {a_shift_start_time}:00'),CONCAT('1900-01-01 {b_shift_start_time}:00'),CONCAT('1900-01-01 {c_shift_start_time}:00'),CONCAT('1900-01-01 {a_shift_end_time}:00'),CONCAT('1900-01-01 {b_shift_end_time}:00'),CONCAT('1900-01-01 {c_shift_end_time}:00')," 
    
    if no_of_shifts == "2":
        shift_dtl =  f"shift1_start_time,shift2_start_time,shift1_end_time,shift2_end_time," 
        shift_dtl_rec =  f"CONCAT('1900-01-01  {a_shift_start_time}:00'),CONCAT('1900-01-01 {b_shift_start_time}:00'),CONCAT('1900-01-01 {a_shift_end_time}:00'),CONCAT('1900-01-01 {b_shift_end_time}:00')," 
    
    if no_of_shifts == "1":
        shift_dtl =  f"shift1_start_time,shift1_end_time," 
        shift_dtl_rec =  f"CONCAT('1900-01-01  {a_shift_start_time}:00'),CONCAT('1900-01-01 {a_shift_end_time}:00')," 
    
    query= f'''insert into master_shifts(plant_id,no_of_shifts,company_id,bu_id,{shift_dtl} created_on,created_by)
             values('{plant_name}','{no_of_shifts}','{company_name}','{bu_name}',{shift_dtl_rec} now(),'{user_login_id}')
    '''
    print(query)
    await cnx.execute(text(query))
    insert_id = await cnx.execute(text("SELECT LAST_INSERT_ID()"))
    insert_id = insert_id.first()[0]
    await cnx.commit()
    
    await update_plant_wise_sync(cnx, 'master_shifts')
    return insert_id

async def update_shift(cnx, shift_id, plant_name, no_of_shifts, company_name, bu_name, a_shift_start_time, b_shift_start_time, c_shift_start_time, a_shift_end_time, b_shift_end_time, c_shift_end_time,user_login_id):
    shift_dtl = ''

    if no_of_shifts == "3":
        shift_dtl = f'''shift1_start_time = CONCAT('1900-01-01 {a_shift_start_time}:00'),
                    shift2_start_time = CONCAT('1900-01-01 {b_shift_start_time}:00'),
                    shift3_start_time = CONCAT('1900-01-01 {c_shift_start_time}:00'),
                    shift1_end_time = CONCAT('1900-01-01  {a_shift_end_time}:00'),
                    shift2_end_time = CONCAT('1900-01-01 {b_shift_end_time}:00'),
                    shift3_end_time = CONCAT('1900-01-01 {c_shift_end_time}:00'),'''
        
    if no_of_shifts == "2":
        shift_dtl = f'''shift1_start_time = CONCAT('1900-01-01 {a_shift_start_time}:00'),
                    shift2_start_time = CONCAT('1900-01-01 {b_shift_start_time}:00'),
                    shift1_end_time = CONCAT('1900-01-01  {a_shift_end_time}:00'),
                    shift2_end_time = CONCAT('1900-01-01 {b_shift_end_time}:00'),
                    '''
    if no_of_shifts == "1":
        shift_dtl = f'''shift1_start_time = CONCAT('1900-01-01 {a_shift_start_time}:00'),                    
                    shift1_end_time = CONCAT('1900-01-01  {a_shift_end_time}:00'),                    
                    '''

    query=f''' update 
                    master_shifts
                set 
                    plant_id = '{plant_name}',
                    no_of_shifts = '{no_of_shifts}',
                    company_id = '{company_name}',
                    bu_id = '{bu_name}',
                    {shift_dtl}
                    sync_status = 'update',
                    modified_on = now(),
                    modified_by = '{user_login_id}' 
                where 
                    shift_id = '{shift_id}' '''
      
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_shifts')

async def remove_shift(cnx, shift_id, status='delete'):
    
      query=f''' Update master_shifts Set sync_status = 'update',status = '{status}' Where shift_id='{shift_id}' '''
      
      await cnx.execute(text(query))
      await cnx.commit()
      await update_plant_wise_sync(cnx, 'master_shifts')

async def changestatus_shift(cnx, shift_id, active_status):
    status = ''

    if active_status == 'inactive':
        status = 'active'
    elif active_status == 'active':
        status = 'inactive'
    
    query = f''' Update master_shifts Set sync_status = 'update',status = '{status}' Where shift_id='{shift_id}' '''
    await cnx.execute(text(query))
    await cnx.commit()
    await update_plant_wise_sync(cnx, 'master_shifts')

async def get_shift_dtl_display(cnx, plant_id, bu_id, company_id):
    where = ""
    
    if bu_id != "" :
        where += f''' and bu_id = '{bu_id}' '''
    if company_id != "":
        where += f''' and company_id = '{company_id}' '''
    if plant_id != "" :
        where += f''' and plant_id = '{plant_id}' '''
    
    query = f''' select
			s.*,
			DATE_FORMAT(s.shift1_start_time, '%h:%i %p') AS shift1_start_time_d,
			DATE_FORMAT(s.shift1_end_time, '%h:%i %p') AS shift1_end_time_d,
			DATE_FORMAT(s.shift2_start_time, '%h:%i %p') AS shift2_start_time_d,
			DATE_FORMAT(s.shift2_end_time, '%h:%i %p') AS shift2_end_time_d,
			DATE_FORMAT(s.shift3_start_time, '%h:%i %p') AS shift3_start_time_d,
			DATE_FORMAT(s.shift3_end_time, '%h:%i %p') AS shift3_end_time_d
		from
			master_shifts as s
		where 1=1 and s.status<>'delete' {where} '''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()

    return result
