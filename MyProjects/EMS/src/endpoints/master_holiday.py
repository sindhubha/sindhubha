from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import os
from datetime import datetime,timedelta
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_holiday_model import holidaylist,save_holiday_dtl,upadte_holiday_dtl,upadte_holidaystatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.master_holiday_model import holidaylist,save_holiday_dtl,upadte_holiday_dtl,upadte_holidaystatus
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/holiday_list/", tags=["Master Holiday"])
async def holiday_list(id :str = Form(""),
                       company_id : int = Form(''),
                       bu_id : int = Form(''),
                       plant_id : int = Form(''),
                       plant_department_id : int = Form(''),
                       equipment_group_id : int = Form(''),
                       holiday_type : str = Form(''),
                       cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await holidaylist(cnx, id,company_id,bu_id,plant_id,plant_department_id,equipment_group_id,holiday_type)
        
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result["data"],
            "data2": result["data2"]
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_holiday_details/", tags=["Master Holiday"])
async def save_holiday_details(year :str = Form(""),
                               company_id :str = Form(""),
                               bu_id :str = Form(""),
                               equipment_id :str = Form(""),
                               plant_id :str = Form(""),
                               plant_department_id :str = Form(""),
                               equipment_group_id :str = Form(""),
                               obj :str = Form(""),
                               obj2 :str = Form(""),
                               id :str = Form(""),
                               user_login_id = Form(""),
                               cnx: AsyncSession = Depends(get_db)):
    try:
        if year == "":
            return _getErrorResponseJson("year is required...")
        
        if equipment_id == "":
            return _getErrorResponseJson("equipment_id is required...")
        
        if user_login_id == "":
            return _getErrorResponseJson("user_login_id is required...")
        
        if obj == "":
            return _getErrorResponseJson("obj is required...")
        
        if id == "":            
            await save_holiday_dtl(cnx, year ,company_id,bu_id,equipment_id ,plant_id ,plant_department_id ,equipment_group_id ,obj ,obj2 ,user_login_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await upadte_holiday_dtl(cnx, year ,company_id,bu_id,equipment_id ,plant_id ,plant_department_id ,equipment_group_id ,obj ,obj2,id ,user_login_id)
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_holiday_status/", tags=["Master Holiday"])
async def update_holiday_status(id: int = Form(''),
                                status : str = Form(''),
                                cnx: AsyncSession = Depends(get_db)):
    if id == "":
        return _getErrorResponseJson("id id is required")
    
    try:

        await upadte_holidaystatus(cnx, id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/get_days_list/", tags=["Master Holiday"])
async def get_days_list(year: int = Form(''), day: str = Form('')):
    try:
        if year == '':
            return _getSuccessResponseJson("year is required")

        if day not in ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]:
            return _getSuccessResponseJson("Invalid day specified")

        target_day = {
            "Sunday": 6,
            "Monday": 0,
            "Tuesday": 1,
            "Wednesday": 2,
            "Thursday": 3,
            "Friday": 4,
            "Saturday": 5
        }[day]

        dates = []
        date = datetime(year, 1, 1)

        while date.year == year:
            if date.weekday() == target_day:
                dates.append({"date": date.strftime("%d-%m-%Y"), "holiday_type": "Weekend","day":day})
            date += timedelta(days=1)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": dates
            
        }
        return response
    except Exception as e:
        return get_exception_response(e)
