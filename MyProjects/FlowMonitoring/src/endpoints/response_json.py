from fastapi.responses import JSONResponse
from fastapi.encoders import jsonable_encoder
import os
from log_file import createFolder
import traceback
import sys 

def _getReturnResponseJson(datas):
    resarray={}
    resarray["iserror"] = False
    resarray["message"] = "Data Received Successfully."
    resarray["data"] =datas
    return JSONResponse(jsonable_encoder(resarray))

def _getSuccessResponseJson(datas):
    resarray = {}
    resarray["iserror"] = False
    resarray["message"] = datas
    return JSONResponse (resarray)


def _getErrorResponseJson(datas):
    resarray={}
    resarray["iserror"] = True
    resarray["message"] = datas
    print(resarray)
    return JSONResponse(resarray)


def get_exception_response(e: Exception):
    error_type = type(e).__name__
    error_line = traceback.extract_tb(e.__traceback__)[0].lineno
    error_filename = os.path.basename(traceback.extract_tb(e.__traceback__)[0].filename)
    error_message = f"{error_type} occurred in file {error_filename}, line {error_line}: {str(e)}"
    createFolder("Log/","Issue in returning data "+error_message)
    resarray={}
    resarray["iserror"] = True
    resarray["message"] = "Error Exception"
    resarray["error"] = error_message
    
    return JSONResponse(resarray)
