import pandas as pd
import onca_utils as ou
import os
import onca_products as op
import numpy as np
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures as ft
import time
import janitor
import sys

# I/O paths
input_conapo_poblaciones = "./requirements/poblaciones_group_quinq.csv"
input_who_poblaciones = './requirements/poblaciones_WHO.csv'
input_cat_entidades = "./requirements/entidades_fed.csv"
input_cat_municipios = "./requirements/municipios_geo.csv"
input_cat_edades = "./requirements/EDADES.csv"
input_mortality_folder = "./DATOS_CRUDOS/"
input_estados_geojson = "./requirements/estados.geojson"
cie10 = "C910"
workers = 16

output_path = f'{cie10}_outputs'
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

#----------------LINEPLOTS----------------#
print("Generando lineplots")
if not os.path.exists(output_path + '/lineplots'):
    os.mkdir(output_path + '/lineplots')
counter = 1
for l in np.arange(arr_l) + 1:   
    for i in np.arange(arr_l-l+1):
        filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups[i:i+l])].copy()

        for sex_id, sex in zip([1,2,3], ["Men","Women","Both sexes"]):
            for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
                if sex_id == 3:
                    df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'RANGO_EDAD'])
                else:
                    df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'SEXO', 'RANGO_EDAD'])
                    df = df[df.SEXO == sex_id].drop(columns=["SEXO"])

                pg.create_lineplot(
                    data=df,
                    x='ANIO_REGIS',
                    y=tasa,
                    color='RANGO_EDAD',
                    output_path=output_path + '/lineplots',
                    cie10=cie10,
                    place='Mexico',
                    scale=escala,
                    hover_data= [tasa],
                    cve_geo='00',
                    sex=sex,
                )
                print(f"Lineplot {counter}", end="\r")
                counter+=1

#----------------HEATMAPS----------------#
print("Generando mapas de calor")
if not os.path.exists(output_path + '/heatmaps'):
    os.mkdir(output_path + '/heatmaps')
counter = 1
for l in np.arange(arr_l) + 1:   
    for i in np.arange(arr_l-l+1):
        age_groups_range = age_groups[i:i+l]
        filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups_range)].copy()
        ages = f"{age_groups_range[0].split('_')[0]}-{age_groups_range[-1].split('_')[-1]}"

        for sex_id, sex in zip([1,2,3], ["Men","Women","Both sexes"]):
            for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
                if sex_id == 3:
                    df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'RANGO_EDAD'])
                else:
                    df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
                    df = df[df.SEXO == sex_id].drop(columns=["SEXO"])

                df = df.merge(cat_entidades, on="ENT_CVE")
                df = df.astype({'ENT_CVE':str})
                df['ENT_CVE'] = df.ENT_CVE.str.zfill(2)


                with ThreadPoolExecutor(max_workers=workers) as executor:
                    futures = list()
                    for year in df.ANIO_REGIS.unique():
                        df_year = df[df.ANIO_REGIS == year].copy()
                        df_cancer_c = df_year.complete("ENT_NOMBRE","RANGO_EDAD").fillna(0, downcast='infer')
                        df_cancer_c = df_cancer_c.sort_values(by=[tasa], ascending=False)
                        counter+=1
                        
                        futures.append(executor.submit(pg.create_age_specific_heatmap,
                        data=df_cancer_c,
                        x="ENT_NOMBRE",
                        y="RANGO_EDAD",
                        z=tasa,
                        output_path=output_path + '/heatmaps',
                        cie10=cie10,
                        place='Mexico',
                        rate="Age-specific MR",
                        scale=escala,
                        labels={"ENT_NOMBRE":"State", "RANGO_EDAD":"Age", tasa:"Age-specific rate"},
                        cve_geo='00',
                        sex=sex,
                        ages=ages,
                        year=year))

                print(f"Heatmap {counter}", end="\r")

#----------------BOXPLOTS----------------#
print("Generando boxplots")
if not os.path.exists(output_path + '/boxplots'):
    os.mkdir(output_path + '/boxplots')
counter = 1
for l in np.arange(arr_l) + 1:   
    for i in np.arange(arr_l-l+1):
        age_groups_range = age_groups[i:i+l]
        filtered_deaths = deaths[deaths.RANGO_EDAD.isin(age_groups_range)].copy()
        ages = f"{age_groups_range[0].split('_')[0]}-{age_groups_range[-1].split('_')[-1]}"

        sex = "Both sexes"
        for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
            df = mc.compute_raw_mortality_rate(filtered_deaths, conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
            df = df.merge(cat_entidades, on="ENT_CVE")
            df = df.astype({'ENT_CVE':str,'SEXO':str})
            df.loc[df.SEXO=="1","SEXO"] = "Men"
            df.loc[df.SEXO=="2","SEXO"] = "Women"
            df['ENT_CVE'] = df.ENT_CVE.str.zfill(2)


            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = list()
                for year in df.ANIO_REGIS.unique():
                    df_year = df[df.ANIO_REGIS == year].copy()
                    df_year = df_year.sort_values(['RANGO_EDAD','SEXO'])
                    counter+=1
                    
                    futures.append(executor.submit(pg.create_boxplot,
                    data=df_year,
                    x='RANGO_EDAD',
                    y=tasa,
                    color='SEXO',
                    hover_data=['ENT_NOMBRE',tasa,'SEXO','RANGO_EDAD'],
                    output_path=output_path + '/boxplots',
                    cie10=cie10,
                    place='Mexico',
                    rate='Age-specific mortality rate',
                    scale=escala,
                    labels={'ENT_NOMBRE':'State',tasa:'Age-specific MR','SEXO':'Sex','RANGO_EDAD':'Age'},
                    cve_geo='00',
                    sex=sex,
                    ages=ages,
                    year=year))
                    
            print(f"Boxplot {counter}", end="\r")

#-----------MAPAS ESTATALES---------------#
print("\nGenerando mapas estatales")
if not os.path.exists(output_path + '/state_maps'):
    os.mkdir(output_path + '/state_maps')
counter = 0
for l in np.arange(arr_l) + 1:   
    for i in np.arange(arr_l-l+1):
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
                df = mc.compute_raw_mortality_rate(filtered_deaths[filtered_deaths.SEXO == sex_id], conapo_populations, ['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD'])
                
                df = who.compute_ASR(df=df[['ANIO_REGIS', 'ENT_CVE', 'SEXO', 'RANGO_EDAD','TASA_CRUDA_100K']],
                    age_column="RANGO_EDAD",
                    rate_column="TASA_CRUDA_100K",
                    scale="100K")

            df = df.merge(cat_entidades, on="ENT_CVE")
            df = df.astype({'ENT_CVE':str})
            df['ENT_CVE'] = df.ENT_CVE.str.zfill(2)
            ages = f"{age_groups_range[0].split("_")[0]}-{age_groups_range[-1].split("_")[-1]}"

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = list()
                for year in df.ANIO_REGIS.unique():
                    counter+=1
                    futures.append(executor.submit(pg.create_state_map,
                        data=df.query(f"ANIO_REGIS == {year}"),
                        geojson_file_path=input_estados_geojson,
                        x='ENT_CVE',
                        y='ASR(WHO)_100K',
                        output_path=output_path + '/state_maps',
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

                    # for future in ft.as_completed(futures):
                    #     print(f"State map {counter}", end="\r")
                    #     counter+=1
            print(f"State map {counter}", end="\r")

print(f"\nProductos terminados en {round((time.time()-init_time)/60,2)} minutos")