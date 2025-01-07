import pandas as pd
import onca_utils as ou
import os
import onca_products as op
import importlib

# I/O paths
input_conapo_poblaciones = "./requirements/poblaciones_group_quinq.csv"
input_cat_entidades = "./requirements/entidades_fed.csv"
input_cat_municipios = "./requirements/municipios_geo.csv"
input_cat_edades = "./requirements/EDADES.csv"
input_mortality_folder = "./DATOS_CRUDOS/"
cie10 = "C16"

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

mc = ou.MortalityCalculator()
pg = op.ProductGenerator()

print("Generando lineplots")
for sex_id, sex in zip([1,2,3], ["Hombres","Mujeres","ambos sexos"]):
    for tasa, escala in zip(["TASA_CRUDA_1K","TASA_CRUDA_10K","TASA_CRUDA_100K"], ["1000","10,000","100,000"]):
        # print(sex_id, sex, tasa, escala)
        if sex_id == 3:
            df = mc.compute_raw_mortality_rate(deaths, conapo_populations, ['ANIO_REGIS', 'RANGO_EDAD'])
        else:
            df = mc.compute_raw_mortality_rate(deaths[deaths.SEXO == sex_id], conapo_populations, ['ANIO_REGIS', 'RANGO_EDAD'])
        
        pg.create_lineplot(
            data=df,
            x='ANIO_REGIS',
            y=tasa,
            color='RANGO_EDAD',
            output_path=output_path,
            cie10=cie10,
            place='Mexico',
            scale=escala,
            hover_data= [tasa],
            cve_geo='00',
            sex=sex,
        )