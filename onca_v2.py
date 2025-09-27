import pandas as pd
import onca_utils as ou
import os
import onca_products as op
import numpy as np
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import time
import janitor
import sys
# Dependencias de MictlanX
from mictlanx.logger.log import Log
from mictlanx.v4.client import Client
from mictlanx.utils.index import Utils
from mictlanx.v4.interfaces.responses import PutResponse
from concurrent.futures import as_completed
from option import Result,Ok,Err
from typing import List,Dict,Any,Awaitable
from client import OCAClient
from nanoid import generate as nanoid


# I/O paths
input_conapo_poblaciones = "./requirements/poblaciones_group_quinq.csv"
input_who_poblaciones = './requirements/poblaciones_WHO.csv'
input_cat_entidades = "./requirements/entidades_fed.csv"
input_cat_municipios = "./requirements/municipios_geo.csv"
input_cat_edades = "./requirements/EDADES.csv"
input_mortality_folder = "./DATOS_CRUDOS/"
input_estados_geojson = "./requirements/estados.geojson"
cie10 = "C910"
workers = 24

output_path = f'/data/onca_products/{cie10}_outputs'
if not os.path.exists(output_path):
    os.mkdir(output_path)

# Lectura de catalogos y datos crudos
print("Cargando catalogos")
catalog_loader = ou.CatalogLoader()

conapo_populations = catalog_loader.load_conapo_populations(input_conapo_poblaciones)
cat_entidades = catalog_loader.load_states(input_cat_entidades)
cat_municipios = catalog_loader.load_municipalities(input_cat_municipios)
cat_edades = catalog_loader.load_ages(input_cat_edades)

del(catalog_loader)

print("Cargando registros de mortalidad")
deaths = ou.DeathRegistryLoader().load_deaths(input_mortality_folder, cat_edades, cie10)
deaths = deaths[(deaths.ANIO_REGIS >= 2000) & (deaths.ANIO_REGIS != 9999)]

if deaths.shape[0] == 0:
    raise Exception(f"No se encontraron registros de mortalidad para {cie10}")

mc = ou.MortalityCalculator()
pg = op.ProductGenerator()

# Variacion de rangos de edad
age_groups = np.array(['00_04', '05_09', '10_14', '15_19', '20_24', '25_29', '30_34',
       '35_39', '40_44', '45_49', '50_54', '55_59', '60_64', '65_69',
       '70_74', '75_79', '80_84', '>85'])
arr_l = age_groups.shape[0]

init_time = time.time()

#Definiendo las configuraciones de MictlanX
print("Definiendo las configuraciones de MictlanX")
oca_client = OCAClient(
    hostname=os.environ.get("OCA_API_HOSTNAME","apix.tamps.cinvestav.mx/onca/api/v1"),
    port= int(os.environ.get("OCA_API_PORT","-1")),
)

L = Log(
    name     = "upload_metadata",
    path     = "logs/",
    console_handler_filter=lambda record: True
)

MICTLANX_BUCKET_ID = "c910_test11" # solamente para mapas estatales
# MICTLANX_BUCKET_ID = "c910_test10" # solamente para mapas estatales (problema de memoria)
# MICTLANX_BUCKET_ID = "c910_test9" # prueba con conexion por ethernet solo boxplots
# MICTLANX_BUCKET_ID = "c910_test8" # Solo faltan mapas aqui
# MICTLANX_BUCKET_ID = "c910_test7" BORRADO
# MICTLANX_BUCKET_ID = "c910_test6" BORRADO
# MICTLANX_BUCKET_ID = "c910_test5" BORRADO
# MICTLANX_BUCKET_ID = "c910_test4" BORRADO

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


# #----------------LINEPLOTS----------------#
# print("Generando lineplots")

# products = []
# futures:List[Awaitable[Result[PutResponse,Exception]]] = []

# if not os.path.exists(output_path + '/lineplots'):
#     os.mkdir(output_path + '/lineplots')
# counter = 1
# for l in np.arange(arr_l) + 1:   
#     for i in np.arange(arr_l-l+1):
#         filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups[i:i+l])].copy()

#         for sex_id, sex in zip([1,2,3], ["Men","Women","Both sexes"]):
#             # for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
#             tasa = "TASA_CRUDA_100K"
#             escala = "100,000"
#             if sex_id == 3:
#                 df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'RANGO_EDAD'])
#             else:
#                 df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'SEXO', 'RANGO_EDAD'])
#                 df = df[df.SEXO == sex_id].drop(columns=["SEXO"])

#             response = pg.create_lineplot(
#                 data=df,
#                 x='ANIO_REGIS',
#                 y=tasa,
#                 color='RANGO_EDAD',
#                 output_path=output_path + '/lineplots',
#                 cie10=cie10,
#                 place='Mexico',
#                 scale=escala,
#                 hover_data= [tasa],
#                 cve_geo='00',
#                 sex=sex,
#             )
            
#             ou.prepare_indexing("Lineplot",
#                                 cie10,
#                                 "2000-2023",
#                                 "00",
#                                 "000",
#                                 sex_id,
#                                 tasa,
#                                 response,
#                                 futures,
#                                 products,
#                                 MICTLANX_URL,
#                                 BUCKET_ID,
#                                 OBSERVATORY_ID,
#                                 c)

#             print(f"Lineplot {counter}")#, end="\r")
#             counter+=1

# wait(futures)

# prod_res    = oca_client.create_products(
#         products = products
#     )
# print(prod_res)

# #----------------HEATMAPS----------------#
# print("Generando mapas de calor")

# products = []
# futures:List[Awaitable[Result[PutResponse,Exception]]] = []

# if not os.path.exists(output_path + '/heatmaps'):
#     os.mkdir(output_path + '/heatmaps')
# counter = 0
# for l in np.arange(arr_l) + 1:   
#     for i in np.arange(arr_l-l+1):
#         age_groups_range = age_groups[i:i+l]
#         filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups_range)].copy()
#         ages = f"{age_groups_range[0].split('_')[0]}-{age_groups_range[-1].split('_')[-1]}"

#         for sex_id, sex in zip([1,2,3], ["Men","Women","Both sexes"]):
#             # for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
#             tasa = "TASA_CRUDA_100K"
#             escala = "100,000"
#             if sex_id == 3:
#                 df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'RANGO_EDAD'])
#             else:
#                 df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
#                 df = df[df.SEXO == sex_id].drop(columns=["SEXO"])

#             df = df.merge(cat_entidades, on="ENT_CVE")
#             df = df.astype({'ENT_CVE':str})
#             df['ENT_CVE'] = df.ENT_CVE.str.zfill(2)


#             with ThreadPoolExecutor(max_workers=workers) as executor:
#                 fts = list()
#                 years = list()
#                 for year in df.ANIO_REGIS.unique():
#                     years.append(year)
#                     df_year = df[df.ANIO_REGIS == year].copy()
#                     df_cancer_c = df_year.complete("ENT_NOMBRE","RANGO_EDAD").fillna(0)
#                     df_cancer_c = df_cancer_c.sort_values(by=[tasa], ascending=False)
#                     counter+=1
                    
#                     fts.append(executor.submit(pg.create_age_specific_heatmap,
#                     data=df_cancer_c,
#                     x="ENT_NOMBRE",
#                     y="RANGO_EDAD",
#                     z=tasa,
#                     output_path=output_path + '/heatmaps',
#                     cie10=cie10,
#                     place='Mexico',
#                     rate="Age-specific MR",
#                     scale=escala,
#                     labels={"ENT_NOMBRE":"State", "RANGO_EDAD":"Age", tasa:"Age-specific rate"},
#                     cve_geo='00',
#                     sex=sex,
#                     ages=ages,
#                     year=year))
                    
#                     print(f"Heatmap {counter}")#, end="\r")

#                 for ft, year in zip(fts, years):
#                     response = ft.result()

#                     ou.prepare_indexing("Heatmap",
#                                         cie10,
#                                         year,
#                                         "00",
#                                         "000",
#                                         sex_id,
#                                         tasa,
#                                         response,
#                                         futures,
#                                         products,
#                                         MICTLANX_URL,
#                                         BUCKET_ID,
#                                         OBSERVATORY_ID,
#                                         c)

# wait(futures)

# prod_res    = oca_client.create_products(
#         products = products
#     )
# print(prod_res)

# #----------------BOXPLOTS----------------#
# print("Generando boxplots")

# products = []
# futures:List[Awaitable[Result[PutResponse,Exception]]] = []

# if not os.path.exists(output_path + '/boxplots'):
#     os.mkdir(output_path + '/boxplots')
# counter = 0
# for l in np.arange(arr_l) + 1:   
#     for i in np.arange(arr_l-l+1):
#         age_groups_range = age_groups[i:i+l]
#         filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups_range)].copy()
#         ages = f"{age_groups_range[0].split('_')[0]}-{age_groups_range[-1].split('_')[-1]}"

#         sex = "Both sexes"
#         # for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
#         tasa = "TASA_CRUDA_100K"
#         escala = "100,000"
#         df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
#         df = df.merge(cat_entidades, on="ENT_CVE")
#         df = df.astype({'ENT_CVE':str,'SEXO':str})
#         df.loc[df.SEXO=="1","SEXO"] = "Men"
#         df.loc[df.SEXO=="2","SEXO"] = "Women"
#         df['ENT_CVE'] = df.ENT_CVE.str.zfill(2)


#         with ThreadPoolExecutor(max_workers=workers) as executor:
#             fts = list()
#             years = list()
#             for year in df.ANIO_REGIS.unique():
#                 years.append(year)
#                 df_year = df[df.ANIO_REGIS == year].copy()
#                 df_year = df_year.sort_values(['RANGO_EDAD','SEXO'])
#                 counter+=1
                
#                 fts.append(executor.submit(pg.create_boxplot,
#                 data=df_year,
#                 x='RANGO_EDAD',
#                 y=tasa,
#                 color='SEXO',
#                 hover_data=['ENT_NOMBRE',tasa,'SEXO','RANGO_EDAD'],
#                 output_path=output_path + '/boxplots',
#                 cie10=cie10,
#                 place='Mexico',
#                 rate='Age-specific mortality rate',
#                 scale=escala,
#                 labels={'ENT_NOMBRE':'State',tasa:'Age-specific MR','SEXO':'Sex','RANGO_EDAD':'Age'},
#                 cve_geo='00',
#                 sex=sex,
#                 ages=ages,
#                 year=year))
                
#                 print(f"Boxplot {counter}")#, end="\r")

#             for ft, year in zip(fts, years):
#                 response = ft.result()

#                 ou.prepare_indexing("Boxplot",
#                                     cie10,
#                                     year,
#                                     "00",
#                                     "000",
#                                     3,
#                                     tasa,
#                                     response,
#                                     futures,
#                                     products,
#                                     MICTLANX_URL,
#                                     BUCKET_ID,
#                                     OBSERVATORY_ID,
#                                     c)

# wait(futures)

# prod_res    = oca_client.create_products(
#         products = products
#     )
# print(prod_res)

#-----------MAPAS ESTATALES---------------#
print("\nGenerando mapas estatales")

if not os.path.exists(output_path + '/maps'):
    os.mkdir(output_path + '/maps')
counter = 1

for l in np.arange(arr_l) + 1:   
    for i in np.arange(arr_l-l+1):

        products = []
        futures:List[Awaitable[Result[PutResponse,Exception]]] = []

        age_groups_range = age_groups[i:i+l]
        filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups_range)].copy()

        for sex_id, sex in zip([1,2,3], ["Men","Women","Both sexes"]):
            # for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
            tasa = "TASA_CRUDA_100K"
            escala = "100,000"
            who = ou.MortalityStandardizer(file_path=input_who_poblaciones, std_name='WHO', age_groups=age_groups_range)

            if sex_id == 3:
                df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'RANGO_EDAD'])
                
                df = who.compute_ASR(df=df[['ANIO_REGIS', 'ENT_CVE', 'RANGO_EDAD','TASA_CRUDA_100K']],
                    age_column="RANGO_EDAD",
                    rate_column="TASA_CRUDA_100K",
                    scale="100K")
                
            else:
                # df = mc.compute_raw_mortality_rate(filtered_deaths[filtered_deaths.SEXO == sex_id], conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
                df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
                df = df[df.SEXO == sex_id].drop(columns=["SEXO"])

                # df = who.compute_ASR(df=df[['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD','TASA_CRUDA_100K']],
                df = who.compute_ASR(df=df[['ANIO_REGIS', 'ENT_CVE', 'RANGO_EDAD','TASA_CRUDA_100K']],
                    age_column="RANGO_EDAD",
                    rate_column="TASA_CRUDA_100K",
                    scale="100K")

            df = df.merge(cat_entidades, on="ENT_CVE")
            df = df.astype({'ENT_CVE':str})
            df['ENT_CVE'] = df.ENT_CVE.str.zfill(2)
            ages = f"{age_groups_range[0].split("_")[0]}-{age_groups_range[-1].split("_")[-1]}"

            with ThreadPoolExecutor(max_workers=workers) as executor:
                fts = list()
                years = list()
                for year in df.ANIO_REGIS.unique():
                    years.append(year)
                    df_year = df[df.ANIO_REGIS == year].copy()
                    counter+=1

                    fts.append(executor.submit(pg.create_state_map,
                        data=df_year,
                        # data=df.query(f"ANIO_REGIS == {year}"),
                        geojson_file_path=input_estados_geojson,
                        x='ENT_CVE',
                        y='ASR(WHO)_100K',
                        output_path=output_path + '/maps',
                        cie10=cie10,
                        place='Mexico',
                        rate='ASR(WHO)',
                        scale='100,000',
                        hover_data=['ASR(WHO)_100K', 'ENT_NOMBRE'],
                        labels={'ASR(WHO)_100K':'ASMR(WHO)', 'ENT_NOMBRE':'State'},
                        cve_geo='00',
                        sex=sex,
                        ages=ages,
                        year=year))

                    print(f"State map {counter}", end="\r")

                for ft, year in zip(fts, years):
                    response = ft.result()

                    ou.prepare_indexing("Map",
                                        cie10,
                                        year,
                                        "00",
                                        "000",
                                        sex_id,
                                        tasa,
                                        response,
                                        futures,
                                        products,
                                        MICTLANX_URL,
                                        BUCKET_ID,
                                        OBSERVATORY_ID,
                                        c)

        wait(futures)

        prod_res    = oca_client.create_products(
                products = products
            )
        print(prod_res)
        del(products, futures)

print(f"\nProductos terminados en {round((time.time()-init_time)/60,2)} minutos")