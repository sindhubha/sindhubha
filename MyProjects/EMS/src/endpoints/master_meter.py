from fastapi import APIRouter
from fastapi import Form,Depends,UploadFile
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from sqlalchemy.sql import text
import tempfile
import pandas as pd
import os
from datetime import datetime,date, timedelta
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_meter_model import meter_Lists,getmeterdtl,save_meter,update_meter,update_meterStatus,meter_historylist,check_import_meter,save_excel_meter,checkdemanddtl,checkconverterlimt,checkslaveid,save_transformer_meter,transformer_meter_list,transformer_submeter_list,save_subtransformer_meter
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

# @router.post("/get_meter_list/{meter_id}", tags=["Master Meter"])
@router.post("/get_meter_list/", tags=["Master Meter"])
async def meter_list(company_id : int = Form(''),
                     meter_id: str = Form(''),
                     type_value: str = Form(''),
                     type_id : str = Form(''), 
                     is_critical : str = Form(''),
                     model_name: str = Form(''),
                     bu_id : str = Form(''),
                     plant_id : str = Form(''),
                     plant_department_id : str = Form(''),
                     campus_id : str = Form(''),
                     function_id : int = Form(''),
                     function2_id :str = Form(''),
                     holiday :str = Form(''),
                     selected : str = Form(''),
                     equipment_id : str = Form(""),
                     equipment_group_id : str = Form(""),
                     meter: str = Form(""),
                     meter_type: str = Form(""),
                     selected_meter: str = Form(""),
                     ip_address: str = Form(""),
                     source: str = Form(""),
                     cnx: AsyncSession = Depends(get_db)): 
    try: 
        createFolder("Log/","query:"+str(meter_id))
        result = await meter_Lists(cnx, company_id,bu_id,meter_id,type_value,type_id,is_critical,model_name,plant_id,plant_department_id,function_id,function2_id,holiday,selected,campus_id,meter,equipment_id,equipment_group_id,meter_type,selected_meter,ip_address,source)
        where = ''
            
        createFolder("Log/","Query executed successfully ......")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result["data"],
            "data2": result["data2"]
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_meter_details/", tags=["Master Meter"])
async def save_meter_details(company_id: int = Form(''),
                             bu_id:int = Form(''),
                             plant_id: int = Form(''),
                             plant_department_id: int = Form(''), 
                             campus_id: int = Form(''),
                             function_id:int = Form(''),
                             converter_id: int = Form(''),
                             meter_id : int = Form(''),
                             meter_code: str = Form(''),
                             meter_name: str = Form(''),
                             ip_address : str = Form(''),
                             port :int = Form(''),
                             major_nonmajor : str = Form(''), 
                             model_name : str = Form(''),                                                
                             IMEI : str = Form(''),  
                             user_login_id : int = Form(''), 
                             function2_id : int = Form(''), 
                             address : int = Form(''),                   
                             parameter : int = Form(''),                   
                             sub_parameter : int = Form(''),  
                             meter_type : str = Form(''),                         
                             source : str = Form(''),                                          
                             physical_location : str = Form(''),                     
                             max_demand : str = Form(''),                     
                             max_pf : str = Form(''),                     
                             main_demand_meter: str = Form(''),                     
                             consumption_type : str = Form(''),                     
                             is_poll_meter : str = Form(''),                     
                             meter :str = Form(""), 
                             equipment_group_id : str = Form(''), 
                             equipment_id : str = Form(''), 
                             mac : str = Form(""),                   
                             cnx: AsyncSession = Depends(get_db)):
    try:
        if company_id == '':  
            return _getErrorResponseJson(" Company ID is Required") 
        
        if bu_id == '':  
            return _getErrorResponseJson(" Bu ID is Required")    
        
        if plant_id == '':  
            return _getErrorResponseJson("Plant ID is Required")   
        
        if plant_department_id == '':  
            return _getErrorResponseJson(" Plant Department ID is Required")
        
        if campus_id == '':  
            return _getErrorResponseJson(" Campus ID is Required")    
        
        if converter_id == '':  
            return _getErrorResponseJson(" Converter ID is Required") 
        
        if meter_name == '':
            return _getErrorResponseJson(" Meter Name is Required")
        
        if meter_code == '':  
            return _getErrorResponseJson(" Meter Code is Required") 
        
        if ip_address == '':  
            return _getErrorResponseJson(" ip_address is required") 
        
        if address == '':  
            return _getErrorResponseJson(" Slave ID Is Required") 
        
        if port == '':  
            return _getErrorResponseJson(" Port is required")
            
        if major_nonmajor == '':  
            return _getErrorResponseJson(" major_nomajor is required")
        
        if model_name == '':  
            return _getErrorResponseJson(" Mode  is Required")
        
        if meter == '':  
            return _getErrorResponseJson(" Meter is Required")
        
        if meter == 'equipment' or meter == 'Equipment':
            if equipment_group_id == '':
                return _getErrorResponseJson(" Equipment Group ID is Required")
            if equipment_id == '':
                return _getErrorResponseJson(" Equipment ID is Required")
        
        if is_poll_meter == "":
            is_poll_meter = 'yes'
        
        if meter_id == "":
            result = await getmeterdtl(cnx, meter_code)
            if len(result)>0:
               return _getErrorResponseJson("Given Meter Code Already Exists...")
            
            if main_demand_meter == 'yes':
                data1 = await checkdemanddtl(cnx,campus_id,main_demand_meter,meter_id)
                if len(data1)>0:
                    demand_meter_cound = data1["demand_meter_cound"]
                    if int(data1["demand_meter_limit"])<=int(data1["demand_meter_cound"]):
                        return _getErrorResponseJson(f"The maximum demand of a given campus is already enter from another{demand_meter_cound} meter...")
            
            data = await checkslaveid(cnx,ip_address,address,meter_id)
            if len(data)>0:
                return _getErrorResponseJson("Given Slave ID For This Converter Already Exists...")
            
            res = await checkconverterlimt(cnx,converter_id,meter_id)
            if res["meter_limit"]<=res["count"]:
                return _getErrorResponseJson("Meter Limit Reached For This Converter...")
            
            await save_meter(cnx, company_id,bu_id,plant_id,plant_department_id,campus_id,function_id,converter_id,meter_id,meter_code,meter_name,ip_address,port,major_nonmajor,model_name,IMEI,user_login_id,function2_id,address,parameter, sub_parameter,meter_type,source,physical_location,max_demand,max_pf,main_demand_meter,consumption_type,is_poll_meter,meter,equipment_group_id,equipment_id,mac)
            createFolder("Log/","Query executed successfully for save plant meter")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            if main_demand_meter == 'yes':
                data1 = await checkdemanddtl(cnx,campus_id,main_demand_meter,meter_id)
                if len(data1)>0:
                    demand_meter_cound = data1["demand_meter_cound"]
                    if int(data1["demand_meter_limit"])<=int(data1["demand_meter_cound"]):
                        return _getErrorResponseJson(f"The maximum demand of a given campus is already enter from another{demand_meter_cound} meter...")
            
            data = await checkslaveid(cnx,ip_address,address,meter_id)
            if len(data)>0:
                return _getErrorResponseJson("Given Slave ID For This Converter Already Exists...")
            
            res = await checkconverterlimt(cnx,converter_id,meter_id)
            if res["meter_limit"]<=(int(res["count"])):
                return _getErrorResponseJson("Meter Limit Reached For This Converter...")
            
            await update_meter(cnx, company_id,bu_id,plant_id,plant_department_id,campus_id,function_id,converter_id,meter_id,meter_code,meter_name,ip_address,port,major_nonmajor,model_name,IMEI,user_login_id,function2_id,address,parameter, sub_parameter,meter_type,source,physical_location,max_demand,max_pf,main_demand_meter,consumption_type,is_poll_meter,meter,equipment_group_id,equipment_id,mac)
            createFolder("Log/","Query executed successfully for update plant meter")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_meter/", tags=["Master Meter"])
async def remove_meter(meter_id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if meter_id == "":
        return _getErrorResponseJson("Meter ID Is Required")
    
    try:

        await update_meterStatus(cnx, meter_id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Seleted Successfully.")

    except Exception as e:
        return get_exception_response(e)
    
@router.post("/meter_history_list/", tags=["Master Meter"])
async def meter_history_list(meter_id: str = Form(''), 
                             company_id: str = Form(''),                             
                             plant_id: str = Form(''),                             
                             plant_department_id: str = Form(''),                             
                             campus_id: str = Form(''),                             
                             cnx: AsyncSession = Depends(get_db)):

    try: 
        
        result = await meter_historylist(cnx,meter_id,company_id,plant_id,plant_department_id,campus_id)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/import_meter_order", tags=["Master Meter"])
async def upload_excel_meter_order(excel_file: UploadFile, user_login_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        equipment_id = 0
        equipment_group_id = 0
        meter_count = 0
        meter_limit = 0
        del_query=f'''DELETE FROM master_meter_temp '''
        await cnx.execute(del_query)
        await cnx.commit()

        createFolder('Log/',f" Received Excel file: {excel_file.filename}")

        if not excel_file.filename.endswith((".xls", ".xlsx")):
            return _getErrorResponseJson( "File format not supported. Please upload an Excel file")
        
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(excel_file.file.read())
            temp_file_path = temp_file.name
            
        # Read the Excel file using Pandas
        df = pd.read_excel(temp_file_path)
        createFolder('Log/',f" Excel details: {df}")

        required_columns = ["Plant Code","Department","IP Address","Slave Id","Meter Code", "Meter Name","Model","Meter's Communication"]
        missing_columns = [col for col in required_columns if col not in df.columns]

        if missing_columns:
            missing_columns_str = ', '.join(missing_columns)
            return _getErrorResponseJson(f"Required columns are missing in the Excel file: {missing_columns_str}")
        
        column_order = ["Plant Code","Department","IP Address","Source","Meter Type","Slave Id","Meter Code", "Meter Name","Model","Physical Location","IMEI","Is Critical","Meter's Communication","Equipment Group Name","Equipment Name","Maximum Demand","Maximum PF"]
     
        expected_columns = df.columns[:len(column_order)]
        if not all(expected_columns[i] == column_order[i] for i in range(len(column_order))):
            return _getErrorResponseJson("Columns are not in the expected order.")
        
        data = df.to_dict(orient="records") 
        createFolder('Log/',f" Excel details: {data}")

        for row in data:
            meter_code = row["Meter Code"]
            meter_name = row["Meter Name"]
            plant_department_name = row["Department"]
            ip_address = row["IP Address"]
            address = row["Slave Id"]
            model_name = row["Model"]
            physical_location = row["Physical Location"]
            IMEI = row["IMEI"]
            major_nonmajor = row["Is Critical"]
            source = row["Source"]
            meter_type = row["Meter Type"]
            meter = row["Meter's Communication"]
            equipment_group_name = row["Equipment Group Name"]
            print("equipment_group_name",equipment_group_name)
            equipment_name = row["Equipment Name"]
            plant_code = row["Plant Code"]
            max_demand = row["Maximum Demand"]
            max_pf = row["Maximum PF"]
            equipment_id = 0
            equipment_group_id = 0
            query = f" select * from master_converter_detail where ip_address = '{ip_address}'"
            c_data = await cnx.execute(query)
            c_data = c_data.fetchall()

            if len(c_data) == 0:
                return _getErrorResponseJson(f"Given Converter IP Address ({ip_address}) not available")
            
            for row in c_data:
                converter_id = row["converter_id"]
                port = row["port_no"]
                mac = row["mac"]
            
            query = text(f''' select * from master_converter_detail where converter_id = '{converter_id}' ''')
            result = await cnx.execute(query)
            result = result.fetchone()
            meter_r = ''
            query2 = text(f'''select count(converter_id) as meter_count from master_meter where converter_id = '{converter_id}' and status <> 'delete' group by converter_id ''')
            meter_r =await cnx.execute(query2)
            meter_r =meter_r.fetchall()
          
            if len(meter_r)>0:
                for meter in meter_r:
                    meter_count = meter["meter_count"]
                    count = result["meter_limit"]

                if int(count)  < (int(meter_count)+1): 
                    return _getErrorResponseJson(f"Meter limit reached for this converter-({converter_id})")
                
            if meter == 'Equipment' or meter == 'equipment':
                if equipment_group_name == '':
                    return _getErrorResponseJson("Equipment Group Name is Missing")
                if equipment_name == '':
                    return _getErrorResponseJson("Equipment Name is Missing")
                
                if equipment_group_name != '':
                    query = text(f" select equipment_group_id from master_equipment_group where equipment_group_name = '{equipment_group_name}'")
                    print(query)
                    data =await cnx.execute(query)
                    data = data.fetchall()
                    print(data)
                    if len(data) == 0:
                        return _getErrorResponseJson(f"Given Equipment Group({equipment_group_name}) not available")
                    for row in data :
                        equipment_group_id = row["equipment_group_id"]
                
                if equipment_name != '':
                    query = text(f" select equipment_id from master_equipment where equipment_name = '{equipment_name}'")
                    data =await cnx.execute(query)
                    data = data.fetchall()
                    if len(data) == 0:
                        return _getErrorResponseJson(f"Given Equipment({equipment_name}) not available")
                    for row in data :
                        equipment_id = row["equipment_id"]
                    
            query = text(f" select model_id from master_model where model_name = '{model_name}'")
            model = await cnx.execute(query)
            model = model.fetchall()
            if len(model) == 0:
                return _getErrorResponseJson(f"Given Model({model_name}) not available")
            
            for row in model:
                model_id  = row["model_id"]
            query = text(f" select * from master_plant where plant_code = '{plant_code}'")
            plant = await cnx.execute(query)
            plant = plant.fetchall()
            if len(plant) == 0:
                return _getErrorResponseJson(f"Given Plant({plant_code}) not available")
            
            for i in plant:
                plant_id = i["plant_id"]

            query = f'''
            select 
                md.*,
                mp.campus_id 
            from 
                master_plant_wise_department md
                inner join master_plant mp on mp.plant_id = md.plant_id
            where 
                md.plant_department_name = '{plant_department_name}' and md.plant_id = {plant_id}'''
            
            get_data = await cnx.execute(text(query))
            get_data = get_data.fetchall()

            if len(get_data) == 0:
                return _getErrorResponseJson(f"Given Department({plant_department_name}) not available")
            
            for row in get_data:
                plant_department_id = row["plant_department_id"]
                company_id = row["company_id"]
                bu_id = row["bu_id"]
                campus_id = row["campus_id"]

            query = text(f" select * from master_meter where ip_address = '{ip_address}' and address = '{address}'")
            data1 = await cnx.execute(query)
            data1 = data1.fetchall()
            if len(data1)> 0:
                return _getErrorResponseJson(f"Given Slave ID For This Converter({ip_address}) Already Exists...")
            
            sql = text(f"""
                INSERT INTO ems_v1.master_meter_temp (
                    company_id, meter_name, meter_code, bu_id, plant_id, plant_department_id, converter_id, campus_id,
                    ip_address, port, created_on, created_by, major_nonmajor, model_name,  IMEI,  
                    address,meter_type,source,physical_location,mac, meter, equipment_group_id, equipment_id,max_demand,max_pf
                )
                VALUES (
                    {company_id},'{meter_name}', '{meter_code}', {bu_id}, {plant_id}, {plant_department_id}, {converter_id}, 
                    {campus_id}, '{ip_address}',{port}, NOW(), '{user_login_id}', '{major_nonmajor}', '{model_id}', '{IMEI}',
                    '{address}','{meter_type}','{source}','{physical_location}','{mac}','{meter}','{equipment_group_id}','{equipment_id}','{max_demand}','{max_pf}')
            """)
            await cnx.execute(sql)
            await cnx.commit()

        get_list = await check_import_meter(cnx)
        result = {
            "iserror": False,
            "message": "Data Return Successfully",
            "res": get_list
        }
        return result
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_import_meter_order", tags=["Master Meter"])
async def save_upload_excel_meter_order(user_login_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        await save_excel_meter(cnx,user_login_id)
        return _getSuccessResponseJson("Save Data Successfully")
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_main_transformer_meter_list", tags=["Master Meter"])
async def get_main_transformer_meter_list(campus_id :int = Form(""),cnx:AsyncSession=Depends(get_db)):
    try:
        get_list = await transformer_meter_list(cnx,campus_id)
        result = {
            "iserror": False,
            "message": "Data Return Successfully",
            "res": get_list
        }
        return result
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_main_transformer_meter", tags=["Master Meter"])
async def save_main_transformer_meter(campus_id :int = Form(""),meter_ids:str = Form(''),user_login_id : str = Form(''),cnx:AsyncSession=Depends(get_db)):
    try:
        await save_transformer_meter(cnx,campus_id,meter_ids,user_login_id)
        return _getSuccessResponseJson("Save Data Successfully")
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_sub_transformer_meter_list", tags=["Master Meter"])
async def get_sub_transformer_meter_list(campus_id :int = Form(""),main_transformer_meter_id:int = Form(""),cnx:AsyncSession=Depends(get_db)):
    try:
        get_list = await transformer_submeter_list(cnx,campus_id,main_transformer_meter_id)
        result = {
            "iserror": False,
            "message": "Data Return Successfully",
            "res": get_list
        }
        return result
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_sub_transformer_meter", tags=["Master Meter"])
async def save_sub_transformer_meter(main_transformer_meter_id :int = Form(''),meter_ids:str = Form(''),user_login_id : str = Form(''),cnx:AsyncSession=Depends(get_db)):
    try:
        await save_subtransformer_meter(cnx,main_transformer_meter_id,meter_ids,user_login_id)
        return _getSuccessResponseJson("Save Data Successfully")
    
    except Exception as e:
        return get_exception_response(e)
    

    
