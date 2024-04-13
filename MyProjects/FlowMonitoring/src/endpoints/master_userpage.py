from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from fastapi.encoders import jsonable_encoder
from log_file import createFolder
from typing import List, Dict
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_userpage_model import userpagelist, getuserpagedtl, check_email, saveuserpage, updateuserpage, updateuserpageStatus, changestatus_userpage, reset_password, get_rights_userLists, getallMenus, getUserAccessMenus, getUserMenus, save_user_rights
from src.models.mysql.master_operator_model import operator_Lists
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
import os

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))


# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_userpage_model import userpagelist, getuserpagedtl, check_email, saveuserpage, updateuserpage, updateuserpageStatus, changestatus_userpage, reset_password, get_rights_userLists, getallMenus, getUserAccessMenus, getUserMenus, save_user_rights

    elif content == 'MSSQL':
        from mssql_connection import get_db
        
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")


router = APIRouter()


@router.post("/userpageLists/", tags=["Master Userpage"])
async def userpageLists_api(userpage_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        result = await userpagelist(userpage_id, cnx)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "userpage_id": userpage_id,
            "userpageLists": result
        }

        return response
    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/saveuserpage/", tags=["Master Userpage"])
async def saveuserpage_api(userpage_id:str=Form(""), userpage_code:str=Form(""), userpage_name:str=Form(""), company_name:str=Form(""), bu_name:str=Form(""), plant_name:str=Form(""), mobile_no:str=Form(""), email_id:str=Form(""), user_designation:str=Form(""), password:str=Form(""), user_level_name:int=Form(""), user_login_id:str=Form(""), is_campus :str = Form(''),cnx:AsyncSession=Depends(get_db)):
    try:
        if userpage_code == "" or userpage_name == "":
            return _getErrorResponseJson("Fields Missing")
        user_exist = await getuserpagedtl(userpage_id, userpage_code, userpage_name, cnx)
        if len(user_exist) > 0:
            return _getErrorResponseJson("User code is already exist")
        email_check = await check_email(userpage_id, userpage_code, email_id, cnx)
        if len(email_check) > 0:
            return _getErrorResponseJson("This email id is already exist")

        if userpage_id == "":
            result = await saveuserpage(userpage_code, userpage_name, company_name, bu_name, plant_name, mobile_no, email_id, user_designation, password, user_level_name, user_login_id, is_campus,cnx)
            createFolder("Log/", "Query executed successfully for User page data")
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            result = await updateuserpage(userpage_id, userpage_code, userpage_name, company_name, bu_name, plant_name, mobile_no, email_id, user_designation, password, user_level_name, user_login_id, is_campus,cnx)
            createFolder("Log/", "Query executed successfully for User page data")
            return _getSuccessResponseJson("Update Successfully...")
    except Exception as e:
        return _getErrorResponseJson(str(e))


@router.post("/removeuserpage/", tags=["Master Userpage"])
async def removeuserpage_api(userpage_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    try:
        if userpage_id == "":
            return _getErrorResponseJson("Fields is required")

        await updateuserpageStatus(userpage_id, cnx)
        createFolder("Log/", "Query executed successfully for User page data")
        return _getSuccessResponseJson("Delete Successfully...")
    except Exception as e:
        return _getErrorResponseJson(str(e))

@router.post("/changestatus_userpage/", tags=["Master Userpage"])
async def changestatus_userpage_api(userpage_id:str=Form(""), active_status:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if userpage_id == "":
        return _getErrorResponseJson("Fields is required")
    await changestatus_userpage(userpage_id, active_status, cnx)
    createFolder("Log/", "Query executed successfully for User page data")
    return _getSuccessResponseJson("Status change Successfully...")


@router.post("/reset_passwords/", tags=["Master Userpage"])
async def reset_password_api(userpage_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if userpage_id == "":
        return _getErrorResponseJson("Fields is required")
    await reset_password(userpage_id, cnx)
    createFolder("Log/", "Query executed successfully for User page data")
    return _getSuccessResponseJson("Change password Successfully...")


@router.post("/get_rights_userLists/", tags=["Master Userpage"])
async def get_rights_userLists_api(userpage_id:str=Form(""), is_login:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    # if userpage_id == "":
    #     return _getErrorResponseJson("Fields is required")

    result = await get_rights_userLists(cnx, userpage_id, is_login)
    return _getReturnResponseJson(jsonable_encoder(result))


@router.post("/get_rights_user_menuLists/", tags=["Master Userpage"])
async def get_rights_user_menuLists_api(employee_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if employee_id == "":
        return _getErrorResponseJson("Fields is required")

    result = await getUserMenus(employee_id, cnx)
    return _getReturnResponseJson(jsonable_encoder(result))


@router.post("/save_user_menu/", tags=["Master Userpage"])
async def save_user_menu_api(obj:str=Form(""), employee_id:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if employee_id == "" or obj == "":
        return _getErrorResponseJson("Fields is required")
    print("save")
    await save_user_rights(employee_id, obj, cnx)
    createFolder("Log/", "Query executed successfully for User menu page data")

    return _getSuccessResponseJson("Save Successfully...")


@router.post("/get_master_access/", tags=["Master Userpage"])
async def get_master_access_api(employee_id:str=Form(""), menu_id:int=Form(""), employee_type:str=Form(""), cnx:AsyncSession=Depends(get_db)):
    if employee_id == "" or employee_type == "" or menu_id == "":
        return _getErrorResponseJson("Fields is required")
    menu = list()
    if employee_type == 'Admin':
        menu = await getallMenus(cnx)
    else:
        menu = await getUserAccessMenus(employee_id, cnx)
    master_aed = dict()
    
    for data in menu:
        master_aed[data['menu_id']] = {"a": data["add_op"], "e": data["edit_op"], "d": data["delete_op"]}
    print(master_aed)
    return _getReturnResponseJson(jsonable_encoder(master_aed[menu_id]))


@router.post("/get_menu_access/", tags=["Master Userpage"])
async def get_menu_access_api(user_login_id:str=Form(""),cnx:AsyncSession=Depends(get_db)):
    if user_login_id == "" :
        return _getErrorResponseJson("Fields is required")
    query = f'''select * from master_employee where employee_id = '{user_login_id}' '''
    
    result = await cnx.execute(text(query))
    result = result.fetchall()
    print(result)
    if len(result) >0:
        for i in result:
            employee_type = i['employee_type']

        
        if employee_type == 'Admin':
            menu = await getallMenus(cnx)
        else:
            menu = await getUserAccessMenus(user_login_id, cnx)
        print(menu)
        return _getReturnResponseJson(jsonable_encoder(menu))

    


