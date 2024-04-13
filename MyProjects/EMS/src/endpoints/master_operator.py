from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_operator_model import operator_Lists, save_operator, update_operator, changestatus_operator, update_operatorStatus, reset_password, getoperatordtl
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_operator_model import operator_Lists, save_operator, update_operator, changestatus_operator, update_operatorStatus, reset_password, getoperatordtl

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()


@router.post("/operator_Lists/", tags=["Master Operator"])
async def operator_Lists_api(operator_id:str=Form(""),plant_id:str=Form(""),company_id:str=Form(""),bu_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        result = await operator_Lists(cnx, operator_id,plant_id,company_id,bu_id)
        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "operator_id": operator_id,
            "operator_Lists": result
        }
        return response
    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/save_operator/", tags=["Master Operator"])
async def save_operator_api(operator_id:str=Form(""), operator_code:str=Form(""), operator_name:str=Form(""), company_name:str=Form(""), bu_name:str=Form(""), plant_name:str=Form(""), password:str=Form(""), user_login_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        print()
        if operator_code == "" or operator_name == "":
            return _getErrorResponseJson("Fields Missing")
        result = await getoperatordtl(cnx, operator_id, operator_code, operator_name)
        if len(result)>0:
            return _getErrorResponseJson("Enter already exist")
        if operator_id == "":
            await save_operator(operator_code, operator_name, company_name, bu_name, plant_name, password, user_login_id, cnx)
            createFolder("Log/", "Query executed successfully for save operator data")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_operator(operator_id, operator_code, operator_name, company_name, bu_name, plant_name, password, user_login_id, cnx)
            createFolder("Log/", "Query executed successfully for Update operator data")
            return _getSuccessResponseJson("Update Successfully...")
    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/remove_operator/", tags=["Master Operator"])
async def remove_operator_api(operator_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        if operator_id == "":
            return _getErrorResponseJson("Field is required")
        await update_operatorStatus(operator_id, cnx)
        createFolder("Log/", "Query executed successfully for delete operator data")
        return _getSuccessResponseJson("Delete Successfully...")
    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/changestatus_operator/", tags=["Master Operator"])
async def changestatus_operator_api(operator_id:str=Form(""), active_status:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        if operator_id == "":
            return _getErrorResponseJson("Field is required")
        await changestatus_operator(operator_id, active_status, cnx)
        createFolder("Log/", "Query executed successfully for Change operator status data")
        return _getSuccessResponseJson("Change status Successfully...")
    except Exception as e:
        return _getErrorResponseJson(str(e))

@router.post("/reset_password/", tags=["Master Operator"])
async def reset_password_api(operator_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        if operator_id == "":
            return _getErrorResponseJson("Field is required")
        await reset_password(operator_id, cnx)
        createFolder("Log/", "Query executed successfully for Change password")
        return _getSuccessResponseJson("Change password Successfully...")
    except Exception as e:
        return _getErrorResponseJson(str(e))
