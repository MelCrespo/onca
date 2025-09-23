import pandas as pd
import os
import unicodedata
from client import Product,Level

class MortalityStandardizer:
    def __init__(self, file_path:str, std_name:str, age_groups:list) -> None:
        self.__std_pop = pd.read_csv(file_path)
        self.__age_groups = age_groups
        self.__std_pop["W"] = self.__std_pop["W"]/100
        
        if self.__std_pop["Age"].shape[0] != len(age_groups):
            self.__std_pop = self.__std_pop[self.__std_pop["Age"].isin(age_groups)].copy()
            self.__std_pop["W"] = self.__std_pop["W"]/self.__std_pop["W"].sum()
        
        self.__std_name = std_name
        
    @property
    def std_pop(self) -> pd.DataFrame:
        return self.__std_pop
        
    @property
    def age_groups(self) -> set:
        return self.__age_groups
        
    def compute_ASR(self, df:pd.DataFrame, age_column:str, rate_column:str, scale:str) -> pd.DataFrame:
        df = df.merge(self.__std_pop, left_on=age_column, right_on="Age").drop(columns=["Age"])
        asr_name = f"ASR({self.__std_name})_" + scale
        df[asr_name] = df[rate_column] * df["W"]
        df = df.drop(columns=[age_column, rate_column,"W"])
        group_columns = df.columns.drop(asr_name).to_list()
        df = df.groupby(group_columns).agg({asr_name:"sum"}).reset_index()
        return df

class MortalityCalculator:
    def compute_raw_mortality_rate(self, deaths:pd.DataFrame, population:pd.DataFrame, group_columns:list) -> pd.DataFrame:
        mortality_rates = deaths.groupby(group_columns).size().reset_index(name="DEFUNCIONES")
        population = population.groupby(group_columns) \
            .agg({'POBLACION_ESTRATO':['sum']})['POBLACION_ESTRATO'].reset_index().rename(columns={'sum':'POBLACION_ESTRATO'})
        mortality_rates = mortality_rates.merge(population)
        raw_ratio = mortality_rates.DEFUNCIONES / mortality_rates.POBLACION_ESTRATO
        mortality_rates['TASA_CRUDA_1K'] = raw_ratio * 1000
        mortality_rates['TASA_CRUDA_10K'] = raw_ratio * 10000
        mortality_rates['TASA_CRUDA_100K'] = raw_ratio * 100000
        return mortality_rates

class CatalogLoader:
    def load_conapo_populations(self, file_path:str) -> pd.DataFrame:
        pe = pd.read_csv(file_path).drop_duplicates()
        pe = pe.melt(['ANIO_REGIS', 'cve_ent_mun', 'ENT_OCURR', 'MUN_OCURR', 'SEXO'], ['00_04', '05_09',
        '10_14', '15_19', '20_24', '25_29', '30_34', '35_39', '40_44', '45_49',
        '50_54', '55_59', '60_64', '65_69', '70_74', '75_79', '80_84', '>85'], var_name="RANGO_EDAD", value_name="POBLACION_ESTRATO")
        pe = pe[pe.SEXO!='Total']
        pe = pe.sort_values(['ANIO_REGIS', 'ENT_OCURR', 'MUN_OCURR', 'RANGO_EDAD', 'SEXO']).reset_index(drop=True).drop(columns=['cve_ent_mun'])
        pe = pe.rename(columns={'ENT_OCURR':'ENT_CVE', 'MUN_OCURR':'MUN_CVE'})
        pe = pe.replace({'Hombres':1,'Mujeres':2})    
        return pe
    
    def load_oms_populations(self, file_path:str) -> pd.DataFrame:
        return pd.read_csv(file_path)
    
    def load_inegi_populations(self, file_path:str) -> pd.DataFrame:
        return pd.read_csv(file_path)
    
    def load_states(self, file_path:str) -> pd.DataFrame:
        cat_entidades = pd.read_csv(file_path).rename(columns={'ENT_OCURR':'ENT_CVE','ENT_NAME':'ENT_NOMBRE'})
        return cat_entidades
    
    def load_municipalities(self, file_path:str) -> pd.DataFrame:
        cat_municipios = pd.read_csv(file_path, encoding='utf-8', usecols=['CVE_ENT','CVE_MUN','nombre municipio']) \
            .drop_duplicates()  \
            .rename(columns={'CVE_ENT':'ENT_CVE',
                            'CVE_MUN':'MUN_CVE',
                            'nombre municipio':'MUN_NOMBRE'})
        return cat_municipios
    
    def load_ages(self, file_path:str) -> pd.DataFrame:
        return pd.read_csv(file_path)
    
class DeathRegistryLoader:
    def load_deaths(self, input_mortality_folder:str, cat_edades:pd.DataFrame, cie10:str):
        print(f"Realizando extracción de registros para {cie10}")
        files_names = os.listdir(input_mortality_folder)
        files_names.sort()
        deaths = pd.DataFrame()

        for file_name in files_names:
            print("Procesando archivo {}".format(file_name))
            raw_data = self.__preprocess_deaths_data(input_mortality_folder + file_name, cat_edades, cie10)            
            deaths = pd.concat([deaths,raw_data], ignore_index=True)
        return deaths
    
    def __preprocess_deaths_data(self, input_path: str, cat_edades: pd.DataFrame, cie10: str) -> pd.DataFrame:
        deaths = pd.read_csv(input_path)
        deaths = deaths.rename(columns=str.upper)
        deaths = deaths[['ANIO_OCUR', 'ENT_OCURR', 'MUN_OCURR', 'CAUSA_DEF', 'SEXO' ,'EDAD']]
        deaths = deaths.rename(columns={'ANIO_OCUR':'ANIO_REGIS'})
        deaths['CAUSA_DEF'] = deaths.CAUSA_DEF.str[:len(cie10)] # Reasignacion de etiquetas CIE10
        deaths = deaths[deaths.CAUSA_DEF==cie10] # Seleccionamos solo la causa de defuncion especifica
        deaths = deaths.merge(cat_edades, left_on='EDAD', right_on='CVE') \
            .drop(columns=['EDAD', 'CVE', 'DESCRIP']) \
            .rename(columns={'ENT_OCURR':'ENT_CVE', 'MUN_OCURR':'MUN_CVE'}) # Agregamos de columna de RANGO_EDAD
        return deaths

def read_data_bytes(file_path):
    with open(file_path, "rb") as file:
        data_bytes = file.read()
    return data_bytes

def prepare_indexing(product_type, cie10, anio, ent_cve, mun_cve, sex_id, rate_type, response: dict, futures, products, MICTLANX_URL, BUCKET_ID, OBSERVATORY_ID, mictlanx_client):
    name_metadata = f"{response["fname"]}.csv"
    name_producct = f"{response["fname"]}.html"

    line = {
        "mcrespo_tipos_productos": product_type,
        "mcrespo_cie10": cie10,
        "mcrespo_temporal": anio,
        "mcrespo_estados": ent_cve,
        "mcrespo_municipios": mun_cve,
        "mcrespo_sexo": str(sex_id),
        "mcrespo_rango_edad": response["rango_edad"],
        "mcrespo_tipo_tasa": rate_type,
        "name_metadata": name_metadata,
        "name_producct": name_producct,
        "description": response["description"]
    }

    print("Cargando productos")
    print("\tMetadata")
    url_data = ""
    file_id = "{}_{}_{}_{}_{}_{}_{}".format(
        line["mcrespo_tipos_productos"], 
        line["mcrespo_cie10"],
        line["mcrespo_temporal"], 
        line["mcrespo_estados"], 
        line["mcrespo_sexo"], 
        line["mcrespo_rango_edad"], 
        line["mcrespo_tipo_tasa"])
    
    print(f"\t{file_id}")
    
    csv_data = read_data_bytes(line["name_metadata"])
    product_data = read_data_bytes(line["name_producct"])
    
    profile = "{}_{}_{}_{}_{}_{}_{}".format(
        line["mcrespo_tipos_productos"], 
        line["mcrespo_cie10"],
        line["mcrespo_temporal"], 
        line["mcrespo_estados"], 
        line["mcrespo_sexo"], 
        line["mcrespo_rango_edad"], 
        line["mcrespo_tipo_tasa"])    

    description      = line["description"]
    level_path       = "PRODUCTO.CIE10.PERIODO.CVE_ENT.SEXO.RANGOEDAD.TASA"
    product_name     = file_id
    # product_type     = "Lineplot"
    # state_key        = str(line["mcrespo_estados"]).zfill(2)
    # state            = line["mcrespo_estados"]
    #url              = "{}/{}/dp_{}".format(MICTLANX_URL,BUCKET_ID,file_id)
    normalized       = unicodedata.normalize("NFD",file_id)
    file_id              = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
    file_id = file_id.replace(" ","").replace(".","").replace("-","_").replace("@","")
    file_id = file_id.lower()
    url              = "{}/{}/product_{}".format(MICTLANX_URL,BUCKET_ID,file_id)
    url_data              = "{}/{}/csv_{}?content_type=text/csv".format(MICTLANX_URL,BUCKET_ID,file_id)
    
    print("\t"+url)
    print("\t"+url_data)




    product_metadata = Product(
        pid        ="csv_{}".format(file_id),
        description= f"Metadata del producto: {description}",
        level_path=level_path,
        product_name=product_name,
        product_type="csv",
        tags=[OBSERVATORY_ID],
        profile=profile,
        url=url_data,
        data_url="",
        levels=[
            Level(
                index=0,
                cid ="mcrespo_tipos_productos", 
                value=str("csv"),
                kind="INTEREST"
            ), 
            
            Level(
                index=1,
                cid ="mcrespo_cie10",
                value=line["mcrespo_cie10"],
                kind="INTEREST",
            ),
            Level(
                index=2,
                cid ="mcrespo_temporal",
                value=str(line["mcrespo_temporal"]),
                kind="TEMPORAL",
            ),            
            Level(
                index=3,
                cid ="mcrespo_estados",
                value=str(line["mcrespo_estados"]),
                kind="SPATIAL",
            ),            
            Level(
                index=4,
                cid ="mcrespo_sexo",
                value=line["mcrespo_sexo"],
                kind="INTEREST",
            ),            
            Level(
                index=5,
                cid ="mcrespo_rango_edad",
                value=line["mcrespo_rango_edad"],
                kind="INTEREST",
            ),            
            Level(
                index=6,
                cid ="mcrespo_tipo_tasa",
                value=line["mcrespo_tipo_tasa"],
                kind="INTEREST",
            )
        ]   
    )
    products.append(product_metadata)

    my_type = "text/csv"


    future = mictlanx_client.put_async(
        #   Comment this line out to save using the checksum as key
        key   = "csv_{}".format(file_id),
        value = csv_data,
        

        #   MictlanX - metadata
        tags = {
            "iarc":"NA",
            "anio":line["mcrespo_temporal"],
            "estado": line["mcrespo_estados"],
            "cve_ent": line["mcrespo_estados"],
            "cve_mun": line["mcrespo_municipios"],
            "product_type":product_type,
            "product_name":product_name,
            "profile":profile,
            "content_type":my_type
        },

        bucket_id=BUCKET_ID,
        replication_factor=2
    )
    
    futures.append(future)

    product_html = Product(
        pid        ="product_{}".format(file_id),
        description= f"{description}",
        level_path=level_path,
        product_name=product_name,
        product_type=product_type,
        tags=[OBSERVATORY_ID],
        profile=profile,
        url=url,
        data_url=url_data,
        levels=[
            Level(
                index=0,
                cid ="mcrespo_tipos_productos", 
                value=str(line["mcrespo_tipos_productos"]),
                kind="INTEREST"
            ), 
            
            Level(
                index=1,
                cid ="mcrespo_cie10",
                value=line["mcrespo_cie10"],
                kind="INTEREST",
            ),
            Level(
                index=2,
                cid ="mcrespo_temporal",
                value=str(line["mcrespo_temporal"]),
                kind="TEMPORAL",
            ),            
            Level(
                index=3,
                cid ="mcrespo_estados",
                value=str(line["mcrespo_estados"]),
                kind="SPATIAL",
            ),            
            Level(
                index=4,
                cid ="mcrespo_sexo",
                value=line["mcrespo_sexo"],
                kind="INTEREST",
            ),            
            Level(
                index=5,
                cid ="mcrespo_rango_edad",
                value=line["mcrespo_rango_edad"],
                kind="INTEREST",
            ),            
            Level(
                index=6,
                cid ="mcrespo_tipo_tasa",
                value=line["mcrespo_tipo_tasa"],
                kind="INTEREST",
            )
    ]
    )
    my_type = "html"
    products.append(product_html)

    future = mictlanx_client.put_async(
        #   Comment this line out to save using the checksum as key
        key   = "product_{}".format(file_id),
        value = product_data,
        

        #   MictlanX - metadata
        tags = {
            "iarc":"NA",
            "anio":line["mcrespo_temporal"],
            "estado": line["mcrespo_estados"],
            "cve_ent": line["mcrespo_estados"],
            "cve_mun": line["mcrespo_municipios"],
            "product_type":product_type,
            "product_name":product_name,
            "profile":profile,
            "content_type":my_type
        },

        bucket_id=BUCKET_ID,
        replication_factor=1
    )
    
    futures.append(future)
    print(url)
    print(url_data)
    print("\tLineplot") 