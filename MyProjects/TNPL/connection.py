import os
import uvicorn 
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
app = FastAPI()
 
def get_connection():    
   
    app = None       
       
    if os.path.isfile('database.txt'): 
        with open('database.txt', 'r') as f:        
            db_type = f.read().strip()
            if db_type == "MSSQL":   
                app = 'main_mssql'
            elif db_type == "MySQL":                     
                app = 'main_mysql'      
                   
            else:                 
                raise ValueError("Invalid database type")        
    else:
        raise ValueError("database.txt file not found")
               
    return app

   
if __name__ == "__main__":
    app = get_connection()   
    uvicorn.run(f"{app}:app",host="0.0.0.0", port=4002, reload = True)
     
  
       
     




    







    


