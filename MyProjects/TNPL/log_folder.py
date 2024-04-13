# import os
# import sys
# import logging
# from datetime import datetime

# def createFolder(directory, data):
#     date_time = datetime.now()
#     curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
#     curtime2 = date_time.strftime("%d-%m-%Y")

#     try:
#         # Get the path of the current script
#         base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

#         # Create the directory inside the user's file directory
#         directory = os.path.join(base_path, directory)
#         if not os.path.exists(directory):
#             os.makedirs(directory)

#         # Create the log file inside the directory
#         file_path = os.path.join(directory, f"{curtime2}.txt")
#         with open(file_path, "a+") as f:
#             f.write(f"{curtime1} {data}\r\n")
#     except OSError as e:
#         print(f"Error: Creating directory. {directory} - {e}")


# def createFolder(directory, data):
#     date_time = datetime.now()
#     curtime1 = date_time.strftime("%d/%m/%Y %H:%M:%S")
#     curtime2 = date_time.strftime("%d-%m-%Y")

#     try:
#         # Get the path of the current script
#         base_path = os.path.abspath(os.path.dirname(sys.argv[0]))

#         # Create the directory inside the user's file directory
#         directory = os.path.join(base_path, directory)
#         if not os.path.exists(directory):
#             os.makedirs(directory)

#         # Create the log file inside the directory
#         log_file_name = f"{curtime2}.txt"
#         file_path = os.path.join(directory, log_file_name)

#         # Configure logging
#         logging.basicConfig(filename=file_path, level=logging.INFO, format="%(asctime)s %(message)s")

#         # Log the data
#         logging.info(f"{curtime1} {data}")
#     except OSError as e:
#         print(f"Error: Creating directory or logging. {directory}/{log_file_name} - {e}")

import os
import sys
from datetime import datetime, timedelta

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

        # Remove log files older than 5 days
        five_days_ago = date_time - timedelta(days=5)
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            if os.path.isfile(file_path):
                file_date_str = filename.split('.')[0]
                file_date = datetime.strptime(file_date_str, "%d-%m-%Y")
                if file_date < five_days_ago:
                    os.remove(file_path)

        # Create the log file inside the directory
        file_path = os.path.join(directory, f"{curtime2}.txt")
        with open(file_path, "a+") as f:
            f.write(f"{curtime1} {data}\r\n")
    except OSError as e:
        print(f"Error: Creating directory. {directory} - {e}")


