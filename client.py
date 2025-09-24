from typing import List,Dict,Optional
from pydantic import BaseModel,validator
import requests as R
from nanoid import generate as nanoid
import string
import os
from option import Result,Ok,Err
import json as J
class LevelCatalog(BaseModel):
    level: int
    cid: str

class Observatory(BaseModel):
    obid:str=""
    title: str="Observatory"
    image_url:str=""
    description:str=""
    catalogs:List[LevelCatalog]=[]
    disabled:bool = False

class CatalogItem(BaseModel):
    value:str
    display_name:str
    code:int
    description:str
    metadata:Dict[str,str]

class Catalog(BaseModel):
    cid:str = ""
    display_name:str = ""
    items: List[CatalogItem] = []
    kind:str = ""
    @validator("display_name",pre=True)
    def remove_double_spaces(cls,value):
        x = " ".join(value.split())
        return x
    @validator("items",pre=True,each_item=True)
    def remove_double_spaces_in_items(cls,item):
        if type(item) ==dict:
            item = CatalogItem(**item)
        item.display_name = " ".join(item.display_name.split())
        return item
    
    @staticmethod
    def from_json( path:str)->'Catalog':
        with open(path,"rb") as f:
            data = J.loads(f.read())
            catalog = Catalog(**data)
            return catalog

class InequalityFilter(BaseModel):
    gt: Optional[int] = None  # Greater than
    lt: Optional[int] = None  # Less than
    eq: Optional[int] = None  # Equal to

    @validator('*', pre=True, always=True)
    def empty_str_to_none(cls, v):
        return v if v != "" else None

class InterestFilter(BaseModel):
    # Allow either a simple value (str) or an inequality filter
    value: Optional[str] = None
    inequality: Optional[InequalityFilter] = None

    # Ensure either value or inequality is provided, but not both
    @validator('inequality', always=True)
    def check_exclusivity(cls, v, values):
        if v and values.get('value'):
            raise ValueError('Provide either a value or an inequality, not both')
        if not v and not values.get('value'):
            raise ValueError('Provide at least a value or an inequality')
        return v
    
class  TemporalFilter(BaseModel):
    low: int
    high: int

class SpatialFilter(BaseModel):
    country: str
    state: str
    municipality: str
    def make_regex(self):
        x = "^"
        if self.country =="*":
            x +="*"
        else:
            x += "{}".format(self.country)
        if self.state =="*":
            x +="\.*"
        else:
            x += "\.{}".format(self.state)
        if self.municipality =="*":
            x +="\.*"
        else:
            x += "\.{}".format(self.municipality)
        return x.upper()
        

        # return "{}".format()



class ProductFilter(BaseModel):
    temporal: Optional[TemporalFilter] = None
    spatial: Optional[SpatialFilter] = None
    interest: List[InterestFilter]=[]

class Level(BaseModel):
    index:int
    cid:str
    value:str
    kind:str =""

class Product(BaseModel):
    pid:str=""
    description:str=""
    levels:List[Level]=[]
    product_type: str=""
    level_path:str=""
    profile:str=""
    product_name: str=""
    tags:List[str]=[]
    url:str =""
    data_url:str =""



OBSERVATORY_ID_SIZE = int(os.environ.get("OBSERVATORY_ID_SIZE","12"))
OBSERVATORY_ID_ALPHABET  = string.ascii_lowercase+string.digits 

class OCAClient(object):
    def __init__(self,hostname:str, port:int=-1):
        self.base_url = "https://{}".format(hostname) if port == -1 else "http://{}:{}".format(hostname,port)
        self.observatories_url = "{}/observatories".format(self.base_url)
        self.catalogs_url = "{}/catalogs".format(self.base_url)
        self.products_url = "{}/products".format(self.base_url)
    def create_observatory(self, observatory:Observatory)->Result[str,Exception]:
        try:
            if observatory.image_url == "":
                observatory.image_url = "https://ivoice.live/wp-content/uploads/2019/12/no-image-1.jpg"
            if observatory.obid == "":
                observatory.obid = nanoid(alphabet=OBSERVATORY_ID_ALPHABET,size=OBSERVATORY_ID_SIZE)
            response = R.post(self.observatories_url,json=observatory.model_dump())
            response.raise_for_status()
            return Ok(observatory.obid)
        except Exception as e:
            return Err(e)

    def delete_observatory(self,obid:str)->Result[str,Exception]:
        url = "{}/{}".format(self.observatories_url,obid)
        try:
            response = R.delete(url=url)
            response.raise_for_status()
            return Ok(obid)
        except Exception as e:
            return Err(e)
    def update_observatory_catalogs(self,obid:str, catalogs:List[LevelCatalog]=[])->Result[str,Exception]:
        try:
            url = "{}/{}".format(self.observatories_url,obid)
            _catalogs = list(map(lambda x: x.model_dump() , catalogs))
            response = R.post(url=url, json=_catalogs )
            response.raise_for_status()
            return Ok(obid)
        except Exception as e:
            return Err(e)
        
        
    def get_observatory(self,obid:str)->Result[Observatory, Exception]:
        url = "{}/{}".format(self.observatories_url,obid)
        try:
            response = R.get(url=url)
            response.raise_for_status()
            data = response.json()
            print(data)
            return Ok(Observatory(
                obid= data["obid"],
                title= data["title"],
                catalogs=list(map(lambda x: LevelCatalog(**x),data["catalogs"])),
                description=data["description"],
                image_url=data["image_url"]
            ))
        except Exception as e:
            return Err(e)
    def get_observatories(self,skip:int=0,limit:int=10)->Result[List[Observatory],Exception]:
        try:
            url = "{}?skip={}&limit={}".format(self.observatories_url,skip,limit)
            response = R.get(url=url)
            response.raise_for_status()
            data = response.json()
            print(data)
            observatories = list(map(lambda x: Observatory(**x), data))
            return Ok(observatories)
        except Exception as e:
            return Err(e)
        
    def create_catalog(self,catalog:Catalog)->Result[str,Exception]:
        try:
            if catalog.cid == "":
                catalog.cid = nanoid(alphabet=OBSERVATORY_ID_ALPHABET, size=OBSERVATORY_ID_SIZE)
            data = catalog.model_dump()
            response = R.post(url=self.catalogs_url,json=data)
            response.raise_for_status()
            return Ok(catalog.cid)
        except Exception as e:
            return Err(e)
        
    def delete_catalog(self,cid:str)->Result[str,Exception]:
        try:
            url = "{}/{}".format(self.catalogs_url,cid)
            response = R.delete(url=url)
            response.raise_for_status()
            return Ok(cid)
        except Exception as e:
            return Err(e)
    def get_catalog(self,cid:str)->Result[Catalog,Exception]:
        try:
            url = "{}/{}".format(self.catalogs_url,cid)
            response = R.get(url=url)
            response.raise_for_status()
            data = response.json()
            return Ok(Catalog(**data))
        except Exception as e:
            return Err(e)
    def get_catalogs(self)->Result[List[Catalog],Exception]:
        try:
            response = R.get(url=self.catalogs_url)
            response.raise_for_status()
            data = response.json()
            catalogs = list(map(lambda x: Catalog(**x), data))
            return Ok(catalogs)
        except Exception as e:
            return Err(e)
    def get_products(self,skip:int = 0, limit:int = 10)->Result[List[Product],Exception]:
        try:
            url = "{}?skip={}&limit={}".format(self.products_url,skip,limit)
            response = R.get(url=url)
            response.raise_for_status()
            data = response.json()
            products = list(map(lambda x : Product(**x), data))
            return Ok(products)
        except Exception as e:
            return Err(e)
    def query_products(self,obid:str, filter:ProductFilter ,skip:int = 0, limit:int = 100 ):
        try:
            url = "{}/{}/products/nid".format(self.observatories_url,obid)
            response = R.post(url=url, json= filter.model_dump())
            response.raise_for_status()
            data = response.json()
            print(data)
            products = list(map(lambda x : Product(**x), data))
            return Ok(products)
        except Exception as e:
            return Err(e)
    def create_products(self,products:List[Product]=[])->Result[bool, Exception]:
        try:
            _products = list(map(lambda x : x.model_dump(),products))
            response = R.post(url=self.products_url,json=_products)
            response.raise_for_status()
            return Ok(True)
        except Exception as e:
            return Err(e)
    def delete_product(self,pid:str)->Result[str,Exception]:
        try:
            url = "{}/{}".format(self.products_url,pid)
            response = R.delete(url=url)
            response.raise_for_status()
            return Ok(pid)
        except Exception as e:
            return Err(e)