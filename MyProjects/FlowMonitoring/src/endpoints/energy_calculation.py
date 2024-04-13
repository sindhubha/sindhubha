from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.energy_calculation_model import energy_calculationlist,save_energycalculation,energy_calculationlist2,save_energycalculation2
    elif content == 'MSSQL':
        from mssql_connection import get_db
        from src.models.mssql.energy_calculation_model import energy_calculationlist,save_energycalculation,energy_calculationlist2,save_energycalculation2
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")



router = APIRouter()

@router.post("/energy_calculation_list/")
async def energy_calculation_list(cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await energy_calculationlist(cnx)

        createFolder("Log/","Query executed successfully for plant energycalculation list")
        
        response = {
            "iserror": False,
            "message": "data returned successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_energy_calculation/")
async def save_energy_calculation(obj :str = Form(""),
                                  cnx: AsyncSession = Depends(get_db)):
    try:
        if obj == '':  
            return _getErrorResponseJson(" obj is required") 
        
        await save_energycalculation(cnx, obj)
            
        return _getSuccessResponseJson("data save successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/energy_calculation_list2/")
async def energy_calculation_list2(cnx: AsyncSession = Depends(get_db)):
    try: 

        result = await energy_calculationlist2(cnx)

        createFolder("Log/","Query executed successfully for plant energycalculation list2")
        
        response = {
            "iserror": False,
            "message": "data returned duccessfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/save_energy_calculation2/")
async def save_energy_calculation2(obj :str = Form(""),
                                  cnx: AsyncSession = Depends(get_db)):
    try:
        if obj == '':  
            return _getErrorResponseJson(" obj is required") 
        
        await save_energycalculation2(cnx, obj)
            
        return _getSuccessResponseJson("data save successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
