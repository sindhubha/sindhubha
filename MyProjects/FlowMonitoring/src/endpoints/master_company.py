from fastapi import APIRouter
from fastapi import Form,Depends,File,UploadFile,Request
from sqlalchemy.orm import Session
from log_file import createFolder
from pathlib import Path
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from mysql_connection import get_db
from src.models.mysql.master_company_model import companyLists,getcompanydtl,savecompany,updatecompany,updatecompanyStatus,changestatus_company,get_company_name
import os
from sqlalchemy.ext.asyncio import AsyncSession

file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_company_model import companyLists,getcompanydtl,savecompany,updatecompany,updatecompanyStatus,changestatus_company,get_company_name

    elif content == 'MSSQL':
        from mssql_connection import get_db
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()
static_dir = Path(__file__).parent / "attachments"
@router.post("/companyLists/", tags=["Master Comapany"])
async def companyLists_api(request: Request,company_id:str=Form(""),for_android:str=Form(""),campus_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    try:
        base_url = request.url._url
        base_path = base_url.split("/")
        base_path.pop()
        base_path.pop()
        base_path = "/".join(base_path)+"/"

        result = await companyLists(cnx, base_path, company_id,campus_id)

        createFolder("Log/","Query executed successfully for  company list")
        if for_android == 'yes':
            response = [{
                "iserror": False,
                "message": "Data Returned Successfully.",
                "company_id": company_id,
                "companyLists": result
            }]
        else:
            response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "company_id": company_id,
            "companyLists": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)

@router.post("/savecompany/", tags=["Master Comapany"])
async def savecompany_api(company_id:str=Form(""),company_code:str=Form(""),company_name:str=Form(""),oracle_id:str=Form(""),ramco_id:str=Form(""),group_logo_old:str=Form(""),company_logo_old:str=Form(""), pdf_attach_old:str=Form(""), group_logo:UploadFile=File(""),company_logo:UploadFile=File(""), pdf_attach:UploadFile=File(""), user_login_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    try:

        if company_code == "" or company_name == "":
            return _getErrorResponseJson("Fields Missing...")
        
        if company_id == "":
            result = await getcompanydtl(cnx, company_id, company_code, company_name)
            if len(result)>0:
                return _getErrorResponseJson("Given Company Code is Already Exists...")
            
            await savecompany(cnx,company_code, company_name, oracle_id, ramco_id, group_logo_old, company_logo_old, pdf_attach_old, group_logo, company_logo, pdf_attach, user_login_id, static_dir)
            createFolder("Log/","Query executed successfully for save company")    
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await updatecompany(cnx, company_id, company_code, company_name, oracle_id, ramco_id, group_logo_old, company_logo_old, pdf_attach_old, group_logo, company_logo, pdf_attach, user_login_id, static_dir)
            createFolder("Log/","Query executed successfully for update company")
            return _getSuccessResponseJson("Updated Successfully...")
            
    except Exception as e:
        return get_exception_response(e)
        
@router.post("/removecompany/", tags=["Master Comapany"])
async def removecompany_api(company_id:str=Form(""),cnx: AsyncSession = Depends(get_db)):
    
    if company_id == "":
        return _getErrorResponseJson("company id is required")
    
    try:

        await updatecompanyStatus(cnx, company_id)
        createFolder("Log/","Query executed successfully for remove company ")
        return _getSuccessResponseJson("Deleted Successfully.")

    except Exception as e:
        return get_exception_response(e)

@router.post("/changestatus_company/", tags=["Master Comapany"])
async def changestatus_company_api(company_id:str=Form(""),active_status:str=Form(""),cnx: AsyncSession = Depends(get_db)):

    if company_id == "":
       return _getErrorResponseJson("company id is required")
    
    try:

        await changestatus_company(cnx, company_id, active_status)
        createFolder("Log/","Query executed successfully for change company status ")
        return _getSuccessResponseJson("Status Changed Successfully.")

    except Exception as e:
        return get_exception_response(e)

@router.post("/get_company_name/", tags=["Master Comapany"])
async def get_company_name_api(cnx: AsyncSession = Depends(get_db)):

    try:

        result = await get_company_name(cnx)
        createFolder("Log/","Query executed successfully for get company ")
        
        return _getReturnResponseJson(result)
    
    except Exception as e:
        return get_exception_response(e)


    
    
    




        
