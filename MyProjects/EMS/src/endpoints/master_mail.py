from fastapi import APIRouter
from fastapi import Form,Depends
from sqlalchemy.orm import Session
from log_file import createFolder
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from starlette.responses import JSONResponse
import smtplib
from os.path import basename
import httpx
import asyncio
from fastapi import Request
from pathlib import Path
import os
from sqlalchemy.ext.asyncio import AsyncSession

base_path = Path(__file__).resolve().parent / "attachments"
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..","..", "..", "..", "database.txt"))
createFolder("Log/","file_path"+str(file_path))

# Check if the file exists
if os.path.exists(file_path):
    with open(file_path, "r") as file:
        content = file.read().strip()  # Remove leading/trailing whitespace
   
    if content == 'MySQL':
        from mysql_connection import get_db
        from src.models.mysql.master_mail_model import mail_list,save_mail,update_mail,update_mailstatus
    elif content == 'MSSQL':
        from mssql_connection import get_db
        # from src.models.mssql.model_model import model_lists,getmodeldtl,save_model,update_model,update_modelstatus
    else:
        raise Exception("Database is not configured or 'database.txt' contains an unexpected value.")
else:
    raise Exception("The 'database.txt' file does not exist in the specified location.")

router = APIRouter()

@router.post("/get_mail_details/")
async def get_mail_details(mail_id :str = Form(""),
                           cnx: AsyncSession = Depends(get_db)):

    try: 

        result = await mail_list(cnx,mail_id)

        response = {
            "iserror": False,
            "message": "Data Returned Successfully.",
            "data": result
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
    

@router.post("/save_mail_details/")
async def save_mail_details(mail_id : int = Form(""),
                            from_mail :str = Form(""),
                            to_mail :str = Form(""),
                            pwd :str = Form(""),
                            cc_mail :str = Form(""),
                            bcc_mail :str = Form(""),
                            subject :str = Form(""),
                            compose_textarea :str = Form(""),
                            report :str = Form(""),
                            mail_type :str = Form(""),
                            send_at :str = Form(""),
                            send_day :str = Form(""),
                            campus_id :str = Form(""),
                            user_login_id :str = Form(""),
                            cnx: AsyncSession = Depends(get_db)):
    try:
        if from_mail == "" :
            return _getErrorResponseJson("from_mail is required...")
        
        if to_mail == "" :
            return _getErrorResponseJson("to_mail is required...")
        
        if pwd == "" :
            return _getErrorResponseJson("pwd is required...")
        
        if report == "" :
            return _getErrorResponseJson("report is required...")
        
        if user_login_id == "" :
            return _getErrorResponseJson("user_login_id is required...")
        
        if mail_id == "":            
            await save_mail(cnx,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_day,user_login_id,campus_id)
            return _getSuccessResponseJson("Saved Successfully...")
        else:
            await update_mail(cnx,mail_id,from_mail,to_mail,pwd,cc_mail,bcc_mail,subject,compose_textarea,report,mail_type,send_at,send_day,user_login_id,campus_id)
            return _getSuccessResponseJson("Updated Successfully...")
        
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/update_mail_status/")
async def update_mail_status(mail_id : int = Form(""),
                             status : str = Form(""),
                             cnx: AsyncSession = Depends(get_db)):
    try:
        if mail_id == '':
            return _getErrorResponseJson("mail_id is required...")
        await update_mailstatus(cnx, mail_id, status)
        if status !='':
            return _getSuccessResponseJson("Status Updated Successfully.")
        else:
            return _getSuccessResponseJson("Deleted Successfully.")
    except Exception as e:
        return get_exception_response(e)
    
@router.post("/send_manual_email/")
async def send_manual_email(from_mail : str = Form(''),
                            password : str = Form(''),
                            to_mail : str = Form(''),
                            subject : str = Form(''),
                            compose_textarea : str = Form(''),
                            attachments : str = Form(''),
                            date :str = Form(''),
                            report_for : str = Form(''),
                            report_type : str = Form(''),
                            cnx: AsyncSession = Depends(get_db)):  
    try:
        
        report_api_urls = []
        attachment_files = []
            
        if "custom_daily_report" in attachments:
            data = {"date": date, "report_for": report_for}
            report_api_urls.append(("http://{request.headers['host']}/custom_daily_report/", data))
            attachment_files.append(f"\DailyReport - {date}.xlsx")
        
        if "month_wise_report" in attachments:
            month_year = date[3:]
            data = {"month_year": month_year, "report_for": report_for, "report_type": report_type}
            report_api_urls.append(("http://{request.headers['host']}/performance_report/", data))
            attachment_files.append(f"\MonthWiseReport-{month_year}.xlsx")
        
        if "year_wise_report" in attachments:
            year = date[6:]
            next_year = int(year) + 1
            data = {"year": year, "report_type": report_for}
            report_api_urls.append(("http://{request.headers['host']}/year_wise_report/", data))
            attachment_files.append(f"\YearWiseReport-{year}-{next_year}.xlsx")

        if "year_report" in attachments:
            year = date[6:]
            next_year = int(year) + 1
            data = {"year": year, "report_type": report_for}
            report_api_urls.append(("http://{request.headers['host']}/year_report/", data))
            attachment_files.append(f"\YearWiseReport-{year}-{next_year}.xlsx")

        async with httpx.AsyncClient() as client:
            responses = await asyncio.gather(*[client.post(url, data=data, timeout=60) for url, data in report_api_urls])
            # responses = await asyncio.gather(*[client.post(url, data=data) for url, data in report_api_urls])
        
        message = MIMEMultipart()
        message['From'] = from_mail
        message['Subject'] = subject
        for attachment in attachment_files:
            with open(f'{base_path}{attachment}', 'rb') as file:
                attachment_part = MIMEApplication(file.read())
                attachment_part.add_header('Content-Disposition', 'attachment', filename=attachment)
                message.attach(attachment_part)

        message.attach(MIMEText(compose_textarea, 'html'))
        smtp_server = "smtpout.secureserver.net"
        smtp_port = 587
        with smtplib.SMTP(smtp_server, smtp_port) as smtp:
            smtp.starttls()
            smtp.set_debuglevel(1)  

            smtp.login(from_mail, password)
            addr = to_mail.split(',')
            message['To'] = ", ".join(addr)
            smtp.send_message(message)
            
        print("Success!")
        response = {
            "iserror": False,
            "message": "Email sent successfully.",
            "data": ''
        }

        return response
    
    except Exception as e:
        return get_exception_response(e)
       