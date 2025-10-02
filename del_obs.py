

#from pymongo import MongoClient
import pandas as pd
import os
#import numpy as np 
#import json as J
#import requests as R
from mictlanx.logger.log import Log
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse
from concurrent.futures import as_completed, wait
from option import Result,Ok,Err
#from dotenv import load_dotenv
from typing import List,Dict,Any,Awaitable
#from db.index import getMongoClientSemarnatReader,emisoras_aggreation
from oca.client import OCAClient,Observatory,Catalog,Product,LevelCatalog,Level
from nanoid import generate as nanoid
#import string 
import unicodedata
#import magic

pd.options.mode.copy_on_write = True

#OCA definitions
oca_client = OCAClient(
    hostname=os.environ.get("OCA_API_HOSTNAME","apix.tamps.cinvestav.mx/onca/api/v1"),
    port= int(os.environ.get("OCA_API_PORT","-1")),
)

L = Log(
    name     = "csv_files_metadata",
    path     = "log/",
    console_handler_filter=lambda record: True
)


MICTLANX_BUCKET_ID = "c910_test12"


NODE_ID = os.environ.get("NODE_ID","risk-calculator-observatory-0")
BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",MICTLANX_BUCKET_ID) #inem
catalog_ids = os.environ.get("OBSERVATORY_CATALOGS","").split(';')
routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:apix.tamps.cinvestav.mx/mictlanx:-1")
#routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:alpha.tamps.cinvestav.mx/v0/mictlanx/router:-1")
OBSERVATORY_INDEX  = int(os.environ.get("OBSERVATORY_INDEX",0))
OBSERVATORY_ID     = os.environ.get("OBSERVATORY_ID",MICTLANX_BUCKET_ID)
OBSERVATORY_DESC = os.environ.get("OBSERVATORY_DESCRIPTION","Pruebas RETC-NOM")
OBSERVATORY_IMAGE_URL = os.environ.get("OBSERVATORY_IMAGE_URL","")
OBSERVATORY_TITLE = os.environ.get("OBSERVATORY_TITLE","Observatory Pruebas RETC-NOM")
#MICTLANX_URL       = os.environ.get("MICTLANX_URL","https://alpha.tamps.cinvestav.mx/v0/mictlanx/router/api/v4/buckets")
MICTLANX_URL       = os.environ.get("MICTLANX_URL","https://apix.tamps.cinvestav.mx/mictlanx/api/v4/buckets")
MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","https")
OUTPUT_PATH:str    = os.environ.get("OUTPUT_PATH","outs_csv/")
L.debug({
    "event":"RETC_IARC_STARTED",
    "bucket_id":BUCKET_ID,
    "catalog_ids":catalog_ids,
    "routers_str":routers_str
})

routers     = list(Utils.routers_from_str(routers_str,protocol=MICTLANX_PROTOCOL))
c = Client(
    # Unique identifier of the client
    client_id   = os.environ.get("MICTLANX_CLIENT_ID","risk-calculator-0"),
    # Storage peers
    routers     = routers,
    # Number of threads to perform I/O operations
    max_workers = int(os.environ.get("MICTLANX_MAX_WORKERS","2")),
    # This parameters are optionals only set to True if you want to see some basic metrics ( this options increase little bit the overhead please take into account).
    debug       = True,
    log_output_path= os.environ.get("MICTLANX_LOG_OUTPUT_PATH","log/"),
    bucket_id=BUCKET_ID
)

def main():
    save_csv           = bool(int(os.environ.get("SAVE_CSV","1")))
    #BUCKET_ID          = os.environ.get("MICTLANX_BUCKET_ID","inem")
    if not os.path.exists(OUTPUT_PATH):
        L.debug("MAKE_DIR {}".format(OUTPUT_PATH))
        os.makedirs(OUTPUT_PATH)
    futures:List[Awaitable[Result[PutResponse,Exception]]] = []
    future = c.delete_bucket_async(bucket_id=BUCKET_ID)
    futures.append(future)        
    
main()