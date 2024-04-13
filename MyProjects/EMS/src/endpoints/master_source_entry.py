from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from pathlib import Path
from fastapi.requests import Request 
from fastapi import FastAPI, Depends, Form, File, UploadFile,Body
import os
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime
from src.models.parse_date import parse_date
from datetime import datetime, timedelta

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))


# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_source_entry_model import source_entry_list,save_source_entry,update_source_entry,update_sourceentryStatus,source_entry_data,save_source_entry_data
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"

# @router.post("/get_source_entry_list/{source_id}",tags=["Master Source Entry"])
@router.post("/get_source_entry_list/",tags=["Master Source Entry"])
async def source_entrylist(id:int =Form('') ,  
                           period_type :str = Form(''),                     
                           source_type :str = Form(''),                     
                           cnx:AsyncSession = Depends(get_db)):
    try:

        result = await source_entry_list(cnx,id,period_type,source_type)

        createFolder("Log/","Query executed successfully for plant meter_source list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_source_entry/",tags=["Master Source Entry"])
async def savesourceentry(id: int = Form(""),                                
                          campus_id:str= Form(""),
                          period_type:str= Form(""),
                          energy_source_name:str= Form(""),
                          user_login_id : str = Form(""),                             
                          source_type : str = Form(""),                             
                          cnx: AsyncSession = Depends(get_db)):
  
    try:
                
        if campus_id == "":
            return _getErrorResponseJson(" Campus ID is required")
        
        if energy_source_name == "":
            return _getErrorResponseJson(" Energy Source Name is required")
        
        if user_login_id == "":
            return _getErrorResponseJson(" User Login ID is required")
                
        if source_type == "":
            return _getErrorResponseJson(" source_type is required")
                
        if id == '':
            await save_source_entry(cnx, campus_id,period_type,energy_source_name,user_login_id,source_type)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_source_entry(cnx, id,campus_id,period_type,energy_source_name,user_login_id,source_type)
            createFolder("Log/","Query executed successfully for update plant meter_source")
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/remove_source_entry/",tags=["Master Source Entry"])
async def remove_sourceentry(id:str=Form(""),status : str = Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if id == "":
        return _getErrorResponseJson("Id is required")
    
    try:

        await update_sourceentryStatus(cnx, id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_source_entry_data_dtl/",tags=["Master Source Entry"])
async def source_entrydata(campus_id:int =Form('') ,  
                           mill_date :str = Form(''),                     
                           period_type :str = Form(''),                     
                           cnx:AsyncSession = Depends(get_db)):
    try:
        mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}

        if mill_date == "":
            return _getErrorResponseJson("Mill Date is Required")
        if period_type == "":
            return _getErrorResponseJson("Period Type is Required")
        
        campus = ''
        if campus_id != '' and campus_id != 0:
            campus = f" and msed.campus_id = {campus_id}"
        mill_date = await parse_date(mill_date)
        error_message = ''
        error_messages = []
        if period_type == 'date':
            if mill_date.day != 1:
                for day in range(1, mill_date.day):  # Adding 1 to include the last day
                    formatted_day = str(day).zfill(2)  # Adding leading zeros if necessary
                    formatted_date = f"{mill_date.year}-{mill_date.month:02d}-{formatted_day}"
                    
                    sql = f'''select msed.*, mse.id
                            from master_source_entry_date msed
                            inner join master_source_entry mse on mse.id = msed.energy_source_id where msed.mill_date = '{formatted_date}'
                            {campus} and mse.period_type = 'date' '''
                    createFolder("Log/",f"sql-{sql}")
                    data = await cnx.execute(sql)
                    datas = data.fetchall()             
                    if len(datas) == 0:
                        error_messages.append(f"{formatted_date}")

                error_data = ",".join(error_messages)
                if error_messages:
                    error_message = f"Kindly Enter The consumption for this days-({error_data})"
        if period_type == 'month':
            month_year=f"""{mill_month[mill_date.month]}-{str(mill_date.year)}"""
            mill_date_s = mill_date.date() - timedelta(days=1)
            sql = f'''select msed.*, mse.id
                        from master_source_entry_date msed
                        inner join master_source_entry mse on mse.id = msed.energy_source_id where msed.mill_date = '{mill_date_s}'
                        {campus} and mse.period_type = 'month' '''
            data = await cnx.execute(sql)
            datas = data.fetchall()  
            if len(datas)== 0:
                error_message = f"Kindly Enter The consumption for this month-({month_year})"
                  

        result = await source_entry_data(cnx,campus_id,mill_date,period_type)

        createFolder("Log/","Query executed successfully for plant meter_source list")
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result,
            "alert_message":error_message
            
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_src_entry_data/",tags=["Master Source Entry"])
async def save_src_entry_data(campus_id :str = Form(""),
                              mill_date :str = Form(""),
                              obj:str= Form(""),
                              obj2:str= Form(""),
                              mill_date_month:str= Form(""),
                              user_login_id : str = Form(""),                             
                              cnx: AsyncSession = Depends(get_db)):
  
    try:
                
        if obj == "":
            return _getErrorResponseJson(" Obj is Required")
        
        if obj2 == "":
            return _getErrorResponseJson(" Obj2 is Required")
        
        if campus_id == "":
            return _getErrorResponseJson(" Campus ID is Required")
        
        if mill_date == "":
            return _getErrorResponseJson(" Mill Date is Required")
        
        if user_login_id == "":
            return _getErrorResponseJson(" User Login ID is Required")     

        await save_source_entry_data(cnx, obj,obj2,mill_date_month,campus_id,mill_date,user_login_id)
        return _getSuccessResponseJson("Saved Successfully...")          
        
    except Exception as e:
        return get_exception_response(e)
    