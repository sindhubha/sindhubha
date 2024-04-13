# from src.models.mysql.plant_wise_sync_and_is_assign import update_plant_wise_sync
from sqlalchemy import text
from src.models.mysql.master_plant_model import plant_Lists
from src.models.mysql.master_equipment_model import equipment_Lists


async def getGroupDetailsReport_code(cnx,company_id,bu_id,plant_id,department_id,equipment_group_id,equipment_class_id=''):

    sql = ''' SELECT
			mt.*
		FROM
            master_equipment mt
		WHERE mt.status <> 'delete' '''

    if company_id != "":
        sql += f" and mt.company_id =  '{company_id}' "
		
    if bu_id != "" :
        sql += f"and mt.bu_id =  '{bu_id}' "

    if plant_id != "" :
        sql += f"and mt.plant_id =  '{plant_id}' "

    if department_id != "" :
        sql += f"and mt.plant_department_id =  '{department_id}' "

    if equipment_group_id != "" :
        sql += f"and mt.equipment_group_id =  '{equipment_group_id}' "

    if equipment_class_id != "" :
        sql += f"and mt.equipment_class_id =  '{equipment_class_id}' "

    sql += '''Order By mt.equipment_id '''

    result = await cnx.execute(text(sql))
    result = result.fetchall()

    return result
