import uvicorn
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI,Request,Form,Body,Depends,HTTPException
# from connection import get_connection
from routes.api import router as api_router
from fastapi.responses import FileResponse
import os
from pathlib import Path
from fastapi.staticfiles import StaticFiles
from multiprocessing import cpu_count, freeze_support  
import uvicorn
from fastapi.openapi.docs import get_swagger_ui_html,get_swagger_ui_oauth2_redirect_html
from log_file import createFolder
from middleware import setup_middleware
from src.endpoints.response_json import _getReturnResponseJson,_getSuccessResponseJson,_getErrorResponseJson,get_exception_response
# from mysql_connection import get_db,AsyncGenerator
from mysql_connection import get_db
from sqlalchemy.orm import Session
import sys
import pytz
import datetime
from log_file import createFolder
from sqlalchemy.sql import text
from fastapi.middleware.trustedhost import TrustedHostMiddleware
import asyncio
import time

time_zone = pytz.timezone("Asia/Kolkata")

app = FastAPI(docs_url=None)
# app = FastAPI()
setup_middleware(app)

dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "static"))

app.mount("/static", StaticFiles(directory=str(dir)), name="static")
# app.mount("/static", StaticFiles(directory="static"), name="static")

static_dir = Path(__file__).resolve().parent / "src" / "endpoints" / "attachments"
if not os.path.exists(static_dir):
    os.makedirs(static_dir) 

app.mount("/attachments", StaticFiles(directory=str(static_dir)), name=  "attachments")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)



@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
    )

@app.get(app.swagger_ui_oauth2_redirect_url, include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(
        openapi_url=app.openapi_url,
        title=app.title + " - Swagger UI",
        oauth2_redirect_url="/docs/oauth2-redirect",
        swagger_js_url="/static/swagger-ui-bundle.js",
        swagger_css_url="/static/swagger-ui.css",
    )

@app.get("/docs/oauth2-redirect", include_in_schema=False)
async def swagger_ui_redirect():
    return get_swagger_ui_oauth2_redirect_html()

# async def run_with_timeout(coro, timeout):
#     try:
#         return await asyncio.wait_for(coro, timeout)
#     except asyncio.TimeoutError:
#         raise TimeoutError("Function execution timed out.")

app.include_router(api_router)
           
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=5003, reload=True)
   
