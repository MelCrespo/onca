import pandas as pd
import os

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
        df = df.groupby(group_columns).agg({asr_name:sum}).reset_index()
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
        
