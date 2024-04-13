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

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_budget_entry_model import budget_entry_list,savebudget,updatebudget,update_budget_status,getbudgetentryname,budget_rate,savebudget_rate
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"
   
@router.post("/get_budget_entry_list/", tags=["Master Budget Entry"])
async def get_budget_entry_list(reporting_department_id: int = Form(''),
                                plant_id : int = Form(''),
                                cnx: AsyncSession = Depends(get_db)):
    try:
        

        result = await budget_entry_list(cnx,reporting_department_id,plant_id)        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)

@router.post("/save_budget/", tags=["Master Budget Entry"])
async def save_budget(reporting_department_id : int = Form(''),
                      campus_id : str = Form(''),
                      company_id : str = Form(''),
                      bu_id : str = Form(''),
                      plant_id : str = Form(''),
                      reporting_department : str = Form(''),
                      department_ids : str = Form(''),
                      utility_department_ids : str = Form(''),
                      is_corporate : str = Form(''),
                      financial_year : str = Form(''),
                      budget : str = Form(''),
                      user_login_id : int = Form(''),
                      cnx:AsyncSession=Depends(get_db)): 
  
    try:
        
        if reporting_department == "":
            return _getErrorResponseJson(" Reporting Department is Required")   
        
        if campus_id == "":
            return _getErrorResponseJson(" campus_id is required") 
          
        # if bu_id == "":
        #     return _getErrorResponseJson(" bu_id is required")   
        
        # if plant_id == "" and plant_id != "0":
        #     return _getErrorResponseJson(" plant_id is required") 
          
        if department_ids == "":
            return _getErrorResponseJson(" department_ids is required")   
        
        if financial_year == "":
            return _getErrorResponseJson(" financial_year is required")   
        
        if budget == "":
            return _getErrorResponseJson(" budget is required")   
        
        if user_login_id == "":
            return _getErrorResponseJson(" user_login_id is required")   
        
        if reporting_department_id == '':  
            res = await getbudgetentryname(cnx,reporting_department,plant_id)
            if len(res)>0:
                return _getErrorResponseJson("Given Reporting Departement already exists !")
            await savebudget(cnx,campus_id,company_id,bu_id,plant_id,reporting_department, department_ids,utility_department_ids,is_corporate,financial_year,budget,user_login_id)
            return _getSuccessResponseJson("saved successfully...")
        else :
            await updatebudget(cnx,campus_id,reporting_department_id,company_id,bu_id,plant_id,reporting_department, department_ids,utility_department_ids,is_corporate,financial_year,budget,user_login_id)
            return _getSuccessResponseJson("updated successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_budget_entry_status/", tags=["Master Budget Entry"])
async def update_budget_entry_status(reporting_department_id: str = Form(''),     
                                     status : str = Form(''),                        
                                     cnx: AsyncSession = Depends(get_db)):
    
    try:
        await update_budget_status(cnx, reporting_department_id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_budget_rate/", tags=["Master Budget Rate"])
async def get_budget_rate(id: str = Form(''),     
                          campus_id : str = Form(''),                        
                          month : str = Form(''),                        
                          is_year_wise : str = Form(''),                        
                          source_type : str = Form(''),                        
                          cnx: AsyncSession = Depends(get_db)):
    
    try:

        if campus_id == "":
            return _getErrorResponseJson(" Campus ID is required") 
        
        if month == "":
            return _getErrorResponseJson(" Month is required") 
        
        if source_type == "":
            return _getErrorResponseJson(" Source Type is required") 
        
        result = await budget_rate(cnx, id,campus_id, month,is_year_wise,source_type)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_budget_rate/", tags=["Master Budget Rate"])
async def save_budget_rate(campus_id : str = Form(''),
                           obj : str = Form(''),                    
                           month : str = Form(''),                    
                           source_type : str = Form(''),
                           is_year_wise : str = Form(''),
                           user_login_id : int = Form(''),
                           cnx:AsyncSession=Depends(get_db)): 
  
    try:
        
          
        
        if campus_id == "":
            return _getErrorResponseJson(" campus_id is required") 
          
        if obj == "":
            return _getErrorResponseJson(" obj is required")   
        
        if user_login_id == "":
            return _getErrorResponseJson(" user_login_id is required")   
        
        
        await savebudget_rate(cnx,campus_id,obj,month,source_type,user_login_id,is_year_wise)
        return _getSuccessResponseJson("saved successfully...")
        
    except Exception as e:
        return get_exception_response(e)
  