import os
import datetime
import random
import shutil

async def save_pdf_file(bdf, dir_name):
    random_number = random.randint(1, 100000)
    now = datetime.datetime.now()
    extension = os.path.splitext(bdf.filename)[1].lower()
    pdf_file_name = f"{random_number}_{now.strftime('%Y_%m_%d_%H_%M_%S')}{extension}"
    
    if not os.path.exists(f"{dir_name}"):
        os.makedirs(dir_name)

    with open(os.path.join(f"{dir_name}", pdf_file_name), "wb") as buffer:
        shutil.copyfileobj(bdf.file, buffer)

    return pdf_file_name
