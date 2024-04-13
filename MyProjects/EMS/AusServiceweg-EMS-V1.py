import os
import sys
from multiprocessing import freeze_support
from subprocess import Popen, PIPE

import psutil
import servicemanager
import traceback
import win32event
import win32service
import win32serviceutil
from datetime import datetime
from pathlib import Path

def createFolder(directory, data):
    date_time = datetime.now()
    curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2 = date_time.strftime("%d-%m-%Y")

    try:
        # Get the path of the current script
        base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

        # Create the directory inside the user's file directory
        directory = os.path.join(base_path, directory)
        if not os.path.exists(directory):
            os.makedirs(directory)

        # Create the log file inside the directory
        file_path = os.path.join(directory, f"{curtime2}.txt")
        with open(file_path, "a+") as f:
            f.write(f"{curtime1} {data}\r\n")
    except OSError as e:
        print(f"Error: Creating directory. {directory} - {e}")

static_dir = Path(__file__).resolve().parent
createFolder("Log/",f"{static_dir}")

isFile = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "service.txt"))

if os.path.exists(isFile):  
    with open(isFile, "r") as f:
        lines = f.readlines()  
        if lines:
            service_name = lines[0].strip()
            # Check if there is a second line and display it
            if len(lines) > 1:
                service_description = lines[1].strip()
else:
    print("File not found:", isFile)

def kill_proc_tree(pid):
    parent = psutil.Process(pid)
    children = parent.children(recursive=True)
    for child in children:
        child.kill()

class PythonWindowsService(win32serviceutil.ServiceFramework):
    _svc_name_ = f"{service_name}"  # service name
    _svc_display_name_ = f"{service_name}"  # service display name
    _svc_description_ = f"{service_description}"  # service description

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        kill_proc_tree(self.process.pid)
        self.process.terminate()
        win32event.SetEvent(self.hWaitStop)
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

    def SvcDoRun(self):
        # Create a separate process to allow server stopping when service
        # is stopped

        server_path = os.path.join(sys._MEIPASS, "AusDataProvian-EMS-V1\\AusDataProvian-EMS-V1.exe")
        self.process = Popen(server_path, stdout=PIPE, stderr=PIPE)
        stdout, stderr = self.process.communicate()
        
        if self.process.returncode != 0:
            error_message = f"AusDataProvian-EMS-V1.exe failed to run. Error: {stderr.decode('utf-8').strip()}"
            createFolder("Log/", error_message)

        win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

if __name__ == '__main__':
    try:
        freeze_support()  # Needed for pyinstaller for multiprocessing on WindowsOS
        if len(sys.argv) == 1:
            servicemanager.Initialize()
            servicemanager.PrepareToHostSingle(PythonWindowsService)
            servicemanager.StartServiceCtrlDispatcher()
        else:
        
            win32serviceutil.HandleCommandLine(PythonWindowsService)
        createFolder("Log/", "Service installed...")

    except Exception as e:
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/", "Issue in service" + error_message)


# import os
# import sys
# from multiprocessing import freeze_support
# from subprocess import Popen, PIPE
# import psutil
# import servicemanager
# import traceback
# import win32event
# import win32service
# import win32serviceutil
# from datetime import datetime
# import threading
# import time
# from sqlalchemy.sql import text
# from mysql_connection import get_db
# from sqlalchemy.orm import Session
# from fastapi import FastAPI,Request,Form,Body,Depends,HTTPException

# import os
# import sys
# from subprocess import Popen, PIPE
# import psutil
# import servicemanager
# import traceback
# import win32event
# import win32service
# import win32serviceutil
# from datetime import datetime
# import threading
# from sqlalchemy.sql import text
# from mysql_connection import get_db
# from sqlalchemy.orm import Session
# from fastapi import Depends

# MAX_CONNECTIONS_THRESHOLD = 5  # Adjust as needed

# def createFolder(directory, data):
#     date_time = datetime.now()
#     curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
#     curtime2 = date_time.strftime("%d-%m-%Y")

#     try:
#         base_path = os.path.abspath(os.path.dirname(sys.argv[0]))
#         directory = os.path.join(base_path, directory)
#         if not os.path.exists(directory):
#             os.makedirs(directory)

#         file_path = os.path.join(directory, f"{curtime2}.txt")
#         with open(file_path, "a+") as f:
#             f.write(f"{curtime1} {data}\r\n")
#     except OSError as e:
#         print(f"Error: Creating directory. {directory} - {e}")

# def kill_proc_tree(pid):
#     parent = psutil.Process(pid)
#     children = parent.children(recursive=True)
#     for child in children:
#         child.kill()

# def restart_fastapi():
#     server_path = os.path.join(sys._MEIPASS, "AusDataProvian-EMS-V1\\AusDataProvian-EMS-V1.exe")
#     process = Popen(server_path, stdout=PIPE, stderr=PIPE)
#     stdout, stderr = process.communicate()

#     if process.returncode != 0:
#         error_message = f"AusDataProvian-EMS-V1.exe failed to run. Error: {stderr.decode('utf-8').strip()}"
#         createFolder("Log/", error_message)

# def connection_monitor(cnx: Session = Depends(get_db)):
#     sql = text(f'SELECT COUNT(*) as connection_count FROM information_schema.processlist')
#     data = cnx.execute(sql).fetchone()
#     return data["connection_count"]

# class PythonWindowsService(win32serviceutil.ServiceFramework):
#     _svc_name_ = "FastAPI Uvicorn Server EMS Service"
#     _svc_display_name_ = "FastAPI Uvicorn Server EMS Service"
#     _svc_description_ = "This EMS service starts up the uvicorn server."

#     def __init__(self, args):
#         win32serviceutil.ServiceFramework.__init__(self, args)
#         self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

#     def SvcStop(self):
#         self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
#         kill_proc_tree(self.process.pid)
#         self.process.terminate()
#         win32event.SetEvent(self.hWaitStop)
#         self.ReportServiceStatus(win32service.SERVICE_STOPPED)

#     def SvcDoRun(self):
#         try:
#             while True:
#                 # Check the current connection count before restarting
#                 if connection_monitor() >= MAX_CONNECTIONS_THRESHOLD:
#                     print(f"Connection limit ({MAX_CONNECTIONS_THRESHOLD}) reached. Restarting FastAPI service.")
#                     restart_fastapi()

#         except Exception as e:
#             error_type = type(e).__name__
#             error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
#             error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
#             error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
#             createFolder("Log/", "Issue in service" + error_message)

# if __name__ == '__main__':
#     try:
#         freeze_support()
#         if len(sys.argv) == 1:
#             servicemanager.Initialize()
#             servicemanager.PrepareToHostSingle(PythonWindowsService)
#             servicemanager.StartServiceCtrlDispatcher()
#         else:
#             win32serviceutil.HandleCommandLine(PythonWindowsService)
#         createFolder("Log/", "Service installed...")

#     except Exception as e:
#         error_type = type(e).__name__
#         error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
#         error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
#         error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
#         createFolder("Log/", "Issue in service" + error_message)
