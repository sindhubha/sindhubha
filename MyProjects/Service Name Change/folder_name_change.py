import os
from datetime import datetime,timedelta
import sys
import shutil
import datetime

live = 'Live'

def createFolder(directory,file_name,data):
    
    date_time=datetime.datetime.now()
    curtime1=date_time.strftime("%d/%m/%Y %H:%M:%S")
    curtime2=date_time.strftime("%d-%m-%Y")
    directory = directory + str(curtime2) + '/'
    # print(directory)

    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        f= open(directory+str(file_name)+".txt","a+")      
        f.write(curtime1 +" "+ str(data) +"\r\n")
        f.close()

        # deleting old log files
        old_date = (datetime.datetime.now() + datetime.timedelta(days=-5)).date()
        file_list = os.listdir('Log')
        for file in file_list:
            try:
                file_date = datetime.datetime.strptime(file, '%d-%m-%Y').date()
            except:
                shutil.rmtree('Log/'+file)
            if file_date <= old_date:
                shutil.rmtree('Log/'+file)

    except OSError:
        print ('Error: Creating directory. ' +  directory)



isFile = os.path.isfile("folder_change.txt")
if isFile:
    f = open("folder_change.txt","r")
    change = f.read()
if change:
    replace_value = change
    print(change)
    print(type(change))
else :
    print(f"The given name is : {change} " )

# replace_value = 'V1'
replace_value = '-' + replace_value

if getattr(sys, 'frozen', False):  # Check if the script is run as a frozen (compiled) executable
    file_path = os.path.abspath(sys.executable)
else:
    file_path = os.path.abspath(__file__)
# file_path = os.path.abspath(__file__)
print(file_path)
file_path = file_path.replace("folder_name_change.exe", "")
file_path = file_path.replace("\\", "/")
createFolder("Log/",live,"file_path "+file_path)
print(file_path)

def rename_directory(old_name, new_name):

    old_name = str(file_path) + str(old_name)
    
    new_name = str(file_path) + str(new_name)

    os.rename(old_name, new_name)

old_name1, new_name1 = ['dist/GatewayApiS-Prod-AMB', 'dist/GatewayApiS-Prod-AMB']
old_name2, new_name2 = ['dist/GatewayApiS-Prod-AMB/GatewayApiS-Prod-AMB.exe', 'dist/GatewayApiS-Prod-AMB/GatewayApiS-Prod-AMB.exe']
old_name3, new_name3 = ['dist/GatewayApiS-Prod-AMB/GatewayApi-Prod-AMB', 'dist/GatewayApiS-Prod-AMB/GatewayApi-Prod-AMB']
old_name4, new_name4 = ['dist/GatewayApiS-Prod-AMB/GatewayApi-Prod-AMB/GatewayApi-Prod-AMB.exe', 'dist/GatewayApiS-Prod-AMB/GatewayApi-Prod-AMB/GatewayApi-Prod-AMB.exe']
old_name5, new_name5 = ['dist/GatewayApi-Prod-AMB', 'dist/GatewayApi-Prod-AMB']
old_name6, new_name6 = ['dist/GatewayApi-Prod-AMB/GatewayApi-Prod-AMB.exe', 'dist/GatewayApi-Prod-AMB/GatewayApi-Prod-AMB.exe']

#old_name1 = old_name1.replace("-AMB", replace_value)
new_name1 = new_name1.replace("-AMB", replace_value)

old_name2 = old_name2.replace("-AMB/", replace_value+'/')
new_name2 = new_name2.replace("-AMB", replace_value)

old_name3 = old_name3.replace("-AMB/", replace_value+'/')
new_name3 = new_name3.replace("-AMB", replace_value)

old_name4 = old_name4.replace("-AMB/", replace_value+'/')
new_name4 = new_name4.replace("-AMB", replace_value)

old_name5 = old_name5.replace("-AMB/", replace_value+'/')
new_name5 = new_name5.replace("-AMB", replace_value)

old_name6 = old_name6.replace("-AMB/", replace_value+'/')
new_name6 = new_name6.replace("-AMB", replace_value)

createFolder("Log/",live,f"GatewayApiS folder - {old_name1},{new_name1}")
rename_directory(old_name1, new_name1)# GatewayApiS folder
createFolder("Log/",live,f"GatewayApiS exe - {old_name2},{new_name2}")
rename_directory(old_name2, new_name2)# GatewayApiS exe
createFolder("Log/",live,f"GatewayApiS/GatewayApi folder - {old_name3},{new_name3}")
rename_directory(old_name3, new_name3)# GatewayApiS/GatewayApi folder
createFolder("Log/",live,f"GatewayApiS/GatewayApi exe - {old_name4},{new_name4}")
rename_directory(old_name4, new_name4)# GatewayApiS/GatewayApi exe
createFolder("Log/",live,f"GatewayApi folder - {old_name5},{new_name5}")
rename_directory(old_name5, new_name5)# GatewayApi folder
createFolder("Log/",live,f"GatewayApi exe - {old_name6},{new_name6}")
rename_directory(old_name6, new_name6)# GatewayApi exe