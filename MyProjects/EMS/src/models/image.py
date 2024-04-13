import os
import datetime
import random
import shutil
from dateutil import parser

async def save_image(image, dir_name):
    random_number = random.randint(1, 100000)
    now = datetime.datetime.now()
    extension = os.path.splitext(image.filename)[1].lower()
    image_file_name = f"{random_number}_{now.strftime('%Y_%m_%d_%H_%M_%S')}{extension}"
    
    if not os.path.exists(f"{dir_name}"):
        os.makedirs(dir_name)

    with open(os.path.join(f"{dir_name}", image_file_name), "wb") as buffer:
        shutil.copyfileobj(image.file, buffer)

    return image_file_name

async def id(meter_id):
    if meter_id !='':
        value = meter_id.split(",")
        if len(value) > 1:
            if  "all" in value:
                meter_id = 'all'
            else:
                values = tuple(value)
                meter_id = ",".join(values)
        else:
            meter_id = value[0]
    return meter_id