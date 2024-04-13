import uvicorn
from multiprocessing import cpu_count, freeze_support
import traceback
import os
import sys
from datetime import datetime

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

try:
    def start_server(host="0.0.0.0",
                     port=5102,
                     num_workers=4,
                     loop="asyncio",
                     reload=False):
        uvicorn.run("main:app",
                    host=host,
                    port=port,
                    workers=num_workers,
                    loop=loop,
                    reload=reload)
    
    if __name__ == "__main__":       
        try:
            freeze_support()  # Needed for pyinstaller for multiprocessing on WindowsOS
            num_workers = int(cpu_count() * 0.1)
            createFolder("Log/", " service started...")
            start_server(num_workers=num_workers)

        except Exception as e:
            error_type = type(e).__name__
            error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
            error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
            error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
            createFolder("Log/", "Issue in start service" + error_message)
            
except Exception as e:
        error_type = type(e).__name__
        error_line = traceback.extract_tb(e.__traceback__)[-1].lineno
        error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[-1].filename)
        error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
        createFolder("Log/","Issue in provian "+error_message)

