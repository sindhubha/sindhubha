from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_user_level_model import user_levelLists, getuser_leveldtl,saveuser_level, updateuser_level, updateuser_levelStatus, changestatus_user_level, get_user_level
from sqlalchemy.ext.asyncio import AsyncSession
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))


# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_user_level_model import user_levelLists, getuser_leveldtl,saveuser_level, updateuser_level, updateuser_levelStatus, changestatus_user_level, get_user_level
    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")


router = APIRouter()


@router.post("/user_levelLists/", tags=["Master User Level"])
async def user_levelLists_api(user_level_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        result = await user_levelLists(user_level_id, cnx)
        createFolder("Log/","Query executed successfully for user level list")

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "user_level_id": user_level_id,
            "user_levelLists": result
        }
        return response
    except Exception as e:
        return _getErrorResponseJson(str(e))

@router.post("/saveuser_level/", tags=["Master User Level"])
async def saveuser_level_api(user_level_id:str=Form(""), user_level_code:str=Form(""), user_level_name:str=Form(""), user_login_id:int=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        if user_level_code == "" or user_level_name == "":
            return _getErrorResponseJson("Filed is missing")
        if user_level_id == "":
            result = await getuser_leveldtl(user_level_id,user_level_code,cnx)
            if len(result)>0:
                return _getErrorResponseJson("Entry already exist")
            await saveuser_level(user_level_code, user_level_name, user_login_id, cnx)
            createFolder("Log/", "Query executed successfully for save user data")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await updateuser_level(user_level_id,user_level_code,user_level_name,user_login_id,cnx)
            createFolder("Log/", "Query executed successfully for update user data")
            return _getSuccessResponseJson("Update user data Successfully")

    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/removeuser_list/", tags=["Master User Level"])
async def removeuser_level_api(user_level_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if user_level_id == "":
        return _getErrorResponseJson("user id is required")
    try:
        await updateuser_levelStatus(user_level_id, cnx)
        createFolder("Log/", "Query executed successfully for remove user data ")
        return _getSuccessResponseJson("Deleted Successfully.")
    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/changestatus_user_level/", tags=["Master User Level"])
async def changestatus_user_level_api(user_level_id:int=Form(""), active_status:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if user_level_id == "":
        return _getErrorResponseJson("user id is required")
    try:
        await changestatus_user_level(user_level_id, active_status, cnx)
        createFolder("Log/", "Query executed successfully for user status change ")
        return _getSuccessResponseJson("Change status Successfully.")
    except Exception as e:
        return _getErrorResponseJson(str(e))

@router.post("/get_user_level/", tags=["Master User Level"])
async def get_user_level_api(user_level_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        result = await get_user_level(user_level_id, cnx)
        createFolder("Log/","Query executed successfully for get user details ")

        return _getReturnResponseJson(jsonable_encoder(result))

    except Exception as e:
        return get_exception_response(e)