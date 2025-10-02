import pandas as pd
import onca_utils as ou
import os
import onca_products as op
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import time
# Dependencias de MictlanX
from mictlanx.logger.log import Log
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse
from concurrent.futures import as_completed
from option import Result,Ok,Err
from typing import List,Dict,Any,Awaitable
from client import OCAClient



L = Log(
    name     = "upload_metadata",
    path     = "logs/",
    console_handler_filter=lambda record: True
)

MICTLANX_BUCKET_ID = "c910_test14" # productos en espanol

NODE_ID = os.environ.get("NODE_ID","risk-calculator-observatory-0")
BUCKET_ID       = os.environ.get("MICTLANX_BUCKET_ID",MICTLANX_BUCKET_ID) #pruebas
catalog_ids = os.environ.get("OBSERVATORY_CATALOGS","").split(';')
routers_str = os.environ.get("MICTLANX_ROUTERS","mictlanx-router-0:apix.tamps.cinvestav.mx/mictlanx:-1") #

OBSERVATORY_ID     = os.environ.get("OBSERVATORY_ID",MICTLANX_BUCKET_ID)
MICTLANX_URL       = os.environ.get("MICTLANX_URL","https://apix.tamps.cinvestav.mx/mictlanx/api/v4/buckets")


MICTLANX_PROTOCOL  = os.environ.get("MICTLANX_PROTOCOL","https")
OUTPUT_PATH:str    = os.environ.get("OUTPUT_PATH","outs_csv/")
L.debug({
    "event":"RETC_IARC_STARTED",
    "bucket_id":BUCKET_ID,
    "catalog_ids":catalog_ids,
    "routers_str":routers_str
})

print("Iniciando el cliente de MictlanX")
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
    log_output_path= os.environ.get("MICTLANX_LOG_OUTPUT_PATH","logs/"),
    bucket_id=BUCKET_ID
)


input_path = "/data/onca_products/C910_outputs/maps/"

import os 
from uuid import uuid4
bucket_id = "cubeton"

for (root, dirs, files) in os.walk(input_path):
    for file in files:
        file_path = os.path.join(root, file)
        key = uuid4().hex.replace("-", "") 
        print(f"Uploading {file_path} - {bucket_id} {key} ")
        with open(file_path, "rb") as f:
            res = c.put_async(
                bucket_id=bucket_id,
                key=key,
                value=f.read()
            )
            result = res.result()
            print(result)
          