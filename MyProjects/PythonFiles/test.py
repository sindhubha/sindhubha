import sys
import os
import datetime
from  datetime import date,timedelta
import shutil
import time
import pymysql
import pytz
import json
import psutil
import smtplib
from os.path import basename
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import requests




shift_end_flag=0
Logfile_name = 'Log/'

def createFolder(directory, data):
    date_time = datetime.datetime.now()
    curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2 = date_time.strftime("%d-%m-%Y")

    try:
        # Get the path of the current script
        base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

        # Create the directory inside the user's file directory
        directory = os.path.join(base_path, directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Remove log files older than 5 days
        five_days_ago = date_time - timedelta(days=3)
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                file_date_str = filename.split('.')[0]
                file_date = datetime.datetime.strptime(file_date_str, "%d-%m-%Y")
                if file_date < five_days_ago:
                    os.remove(file_path)

        # Create the log file inside the directory
        file_path = os.path.join(directory, f"{curtime2}.txt")
        with open(file_path, "a+") as f:
            f.write(f"{curtime1} {data}\r\n")
    except OSError as e:
        print(f"Error: Creating directory. {directory} - {e}")


def auto_mail(db,cursor):
    try:
        current_time = datetime.datetime.now()
        current_date = date.today()
        formatted_date = current_time.strftime("%Y-%m-%d %H:%M")
        print(formatted_date)
        from_date = current_date.replace(day=1)
        to_date = current_date - timedelta(days=1) 

        if from_date == current_date:                
            from_date = current_date - timedelta(days=1)
            from_date = from_date.replace(day=1)
            to_date = current_date - timedelta(days=1)  
        
        
        sql = f'''SELECT  * FROM ems_v1.master_mail '''
        cursor.execute(sql)
        automail_list = cursor.fetchall()
       

        from_mail = ''
        password = ''
        to_mail = ''
        subject = ''
        compose_textarea = ''
        current_time = datetime.datetime.now().strftime('%H:%M')
        if len(automail_list)>0:
            for rows in automail_list:
                from_mail = rows["from_mail"]
                password = rows["pwd"]
                to_mail = rows["to_mail"]
                cc_mail = rows["cc_mail"]
                bcc_mail = rows["bcc_mail"]
                subject = rows["subject"]
                compose_textarea = rows["compose_textarea"]
                compose_textarea1 = rows["compose_textarea1"]
                compose_textarea2 = rows["compose_textarea2"]
                location = rows["report_location"] 
                api_url = rows["api_url"]
                campus_id = rows["campus_id"]
                send_at = rows["send_at"]
                send_date = rows["send_date"]
                report = rows["report"]
            
                api_params  = ''
                if report == 'Energy Statement':
                    if current_time == send_at:
                        api_params = {
                                "campus_id":campus_id,
                                "from_date":'2024-03-01',
                                "to_date":"2024-03-29",
                                "report_for":"pdf",
                                "user_login_id":1
                            }
                elif report == 'Energy Statement With Tariff':
                    new_send_date = send_date + timedelta(minutes=5)
                    send_date = new_send_date.strftime("%Y-%m-%d %H:%M")
                    if formatted_date == send_date:
                        api_params = {
                                "campus_id":campus_id,
                                "month":from_date,
                                "report_for":"pdf",
                                "report_type":"with_rate",
                                "user_login_id":1
                            }
                if api_params != '':
                    createFolder('Log/',f" API inputs - {api_params} ")
                    api_url = f"{api_url}"
                    createFolder('Log/',f" API url - {api_url} ")
                    
                    response = requests.post(api_url, data=api_params)
                    createFolder('Log/',f" API raw data - {response.text} ")
                    api_response = response.json()
                    print(api_response)
                    if api_response!=None:
                        if api_response["iserror"] == False:
                            mill_month={1:"01",2:"02",3:"03",4:"04",5:"05",6:"06",7:"07",8:"08",9:"09",10:"10",11:"11",12:"12"}                
                            
                            try:
                                # Create a multipart message container
                                message = MIMEMultipart()
                            
                                message['From'] = from_mail
                                message['Subject'] = subject
                                print(from_mail)
                                attachment = location+'availability_report.pdf'
                                attachment_name = os.path.basename(attachment)
                                with open(attachment, 'rb') as file:
                                    attachment_part = MIMEApplication(file.read())
                                    attachment_part.add_header('Content-Disposition', 'attachment', filename=attachment_name)
                                    message.attach(attachment_part)
                                
                                email_content = compose_textarea + '<br><br>'  +compose_textarea1 + '<br><br>'+ compose_textarea2

                            
                                html_part = MIMEText(email_content, 'html')
                                message.attach(html_part)

                                # Connect to the SMTP server
                                smtp_server =  "smtpout.secureserver.net"
                                smtp_port = 587
                        
                                with smtplib.SMTP(smtp_server, smtp_port) as smtp:
                                    smtp.starttls()
                                    smtp.login(from_mail, password)
                                    message['To'] = to_mail
                                    message['Cc'] = cc_mail
                                    recipients = to_mail.split(',') + cc_mail.split(',')
                                    smtp.send_message(message, from_mail, recipients)
                                createFolder(f"Log/",f"Mail Send Sucessfully!")  
                            except Exception as e:
                                createFolder("ErrorLog/", "Issue in Sending Mail " + str(e))
                        else:
                            message = MIMEMultipart()
                            
                            message['From'] = from_mail
                            message['Subject'] = subject
                            
                            email_content = compose_textarea + '<br><br>' + api_response['message'] + '<br><br>' +compose_textarea2

                            
                            html_part = MIMEText(email_content, 'html')
                            message.attach(html_part)

                            # Connect to the SMTP server
                            smtp_server = "smtpout.secureserver.net"
                            smtp_port = 587
                    
                            with smtplib.SMTP(smtp_server, smtp_port) as smtp:
                                smtp.starttls()
                                smtp.login(from_mail, password)
                                message['To'] = to_mail
                                message['Cc'] = cc_mail
                                message.add_header('Bcc', bcc_mail)
                                recipients = to_mail.split(',') + cc_mail.split(',')
                                smtp.send_message(message, from_mail, recipients)
                            createFolder(f"Log/",f"Mail Send Sucessfully..")  
                    else:
                        createFolder(f"Log/",f"API response is empty")  
                
    except Exception as e:
        createFolder(f"ErrorLog/",f"Error in AutoMail Function :{e}")

def checkIfProcessRunning(processName):
    process_count = 0
    for proc in psutil.process_iter():
        try:
            if processName.lower() in proc.name().lower():
                process_count = process_count + 1
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            pass
    return process_count

if getattr(sys, 'frozen', False):
    File_Name = os.path.basename(sys.executable)
elif __file__:
    File_Name = os.path.basename(__file__)

process_count = checkIfProcessRunning(File_Name)
if process_count > 2:
    createFolder('Process_log/', f"Process {File_Name} is Already Running  !! ")
    sys.exit()

else:
    while True:

        try:
            createFolder('log/', f"Calling Power Data  !! ")
            db = pymysql.connect(host="localhost", user="root",passwd="", db="ems_v1" , port= 3306)
            cursor = db.cursor(pymysql.cursors.DictCursor)
            auto_mail(db,cursor)
            if db:
                db.close()
            createFolder('log/', f"Mail Function execution completed !! ")
            time.sleep(60)
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            createFolder(f"{Logfile_name}main_loop_error/", f"Error In While Loop -->> Error: {e} , Error_type: {exc_type} , File_name: {fname} , Error_line: {exc_tb.tb_lineno} .")