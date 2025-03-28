import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import json
import janitor

class ProductGenerator:
    def create_lineplot(self, data: pd.DataFrame, x: str, y: str, color: str, output_path: str,
                        cie10: str, place: str, scale: str, hover_data: list, cve_geo: str, sex: str) -> None:

        fig_title = f"{place} age-specific rate per {scale} inhabitants, {sex}"
        fig = px.line(data.sort_values([color,x]),
                        x=x,
                        y=y,
                        color=color,
                        hover_name=color,
                        hover_data=hover_data,
                        width=1080,
                        height=720,
                        markers=True)
        
        years = data.sort_values(x)[x].unique()
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title=f"Mortality rate per {scale} inhabitants",
            title_text=fig_title,
            legend_title="Age group",
            xaxis = dict(
                tickmode = 'array',
                tickvals = years,
                ticktext = years,
                tickangle = 45
            )
        )

        min_year = data[x].min()
        max_year = data[x].max()
        age_groups = data.sort_values(color)[color].unique()
        min_age = age_groups[0].split("_")[0]
        max_age = age_groups[-1].split("_")[-1]
        file_name = f"lineplot_{cie10}_" + f"[{min_year}-{max_year}]_" + f"{cve_geo}_" + sex.replace(" ","") + "_" + f"[{min_age}-{max_age}]_" + y.lower().replace('_', '')
        fig.write_html(output_path + "/" + file_name + ".html")
        data[[x,color,y]].to_csv(output_path + "/" + file_name + ".csv", index=False)
        
        self.__write_metadata(
                        name=output_path + "/" + file_name,
                        description="Histórico de tasas de mortalidad por edad.",
                        data_view_id=file_name + ".csv",
                        interest_var=f"CauseOfDeath({cie10}), Sex({sex.split(' ')[0]}), Age([{min_age}-{max_age}])",
                        observable_var=f"MortalityRate({y})",
                        info=f"CauseOfDeath({cie10}),Year([{min_year}-{max_year}]),Space({place}),Sex({sex.split(' ')[0]}),Age([{min_age}-{max_age}]),MortalityRate({y})",
                        product_type="Tendency",
                        space=self.__get_space_string(cve_geo),
                        temporal=f"Year([{min_year}-{max_year}])",    
                        function_id="plotly.express.line",
                        hue=color,
                        title=fig_title,
                        x_axis=x,
                        y_axis=y)
        
    def create_state_map(self, data: pd.DataFrame, geojson_file_path: str, x: str, y: str, output_path: str,
                        cie10: str, place: str, rate: str, scale: str, hover_data: list, labels: dict,
                        cve_geo: str, sex: str, ages: str, year: str) -> None:

        fig_title = f"{rate} per {scale} inhabitants, {place}, {sex}, age[{ages}], in {year}"
        
        geo = json.load(open(geojson_file_path,"r"))

        fig = px.choropleth_mapbox(data, geojson=geo, locations=x, 
                                featureidkey="properties.CVE_ENT",
                                color=y,
                                hover_data=hover_data,
                                labels=labels,
                                color_continuous_scale="YlOrRd",
                                mapbox_style="carto-positron",
                                zoom=4,
                                center={"lat":22.3969, "lon": -101.2833},
                                opacity=0.5,
                                title=fig_title)
        
        file_name = f"states_map_{cie10}_" + f"{year}_" + f"{cve_geo}_" + sex.replace(" ","") + "_" + f"[{ages}]_" + y.lower().replace('_', '')
        fig.write_html(output_path + "/" + file_name + ".html")
        data[hover_data].to_csv(output_path + "/" + file_name + ".csv", index=False)
        self.__write_metadata(
                        name=output_path + "/" + file_name,
                        description="Mapa de tasas de mortalidad por estado.",
                        data_view_id=file_name + ".csv",
                        interest_var=f"CauseOfDeath({cie10}), Sex({sex.split(' ')[0]}), Age([{ages}])",
                        observable_var=f"MortalityRate({y})",
                        info=f"CauseOfDeath({cie10}),Year({year}),Space({place}),Sex({sex.split(' ')[0]}),Age([{ages}]),MortalityRate({y})",
                        product_type="Map",
                        space=self.__get_space_string(cve_geo),
                        temporal=f"Year({year})",    
                        function_id="plotly.express.choropleth_mapbox",
                        title=fig_title,
                        x_axis=x,
                        y_axis=y)
        
    def create_municipality_map(self, data: pd.DataFrame, geojson_file_path: str, x: str, y: str, output_path: str,
                        cie10: str, place: str, rate: str, scale: str, hover_data: list, labels: dict,
                        cve_geo: str, sex: str, ages: str, year: str) -> None:

        fig_title = f"{rate} per {scale} inhabitants, {place}, {sex}, age[{ages}], in {year}"
        
        geo = json.load(open(geojson_file_path,"r"))

        fig = px.choropleth_mapbox(data, geojson=geo, locations=x, 
                                featureidkey="properties.CVEGEO",
                                color=y,
                                hover_data=hover_data,
                                labels=labels,
                                color_continuous_scale="YlOrRd",
                                mapbox_style="carto-positron",
                                zoom=4,
                                center={"lat":22.3969, "lon": -101.2833},
                                opacity=0.5,
                                title=fig_title)
        
        file_name = f"municipalities_map_{cie10}_" + f"{year}_" + f"{cve_geo}_" + \
            sex.replace(" ","") + "_" + y.lower().replace('_', '')
        fig.write_html(output_path + "/" + file_name + ".html")
        data[hover_data].to_csv(output_path + "/" + file_name + ".csv", index=False)
        self.__write_metadata(
                        name=output_path + "/" + file_name,
                        description="Mapa de tasas de mortalidad por municipio.",
                        data_view_id=file_name + ".csv",
                        interest_var=f"CauseOfDeath({cie10}), Sex({sex.split(' ')[0]}), Age([{ages}])",
                        observable_var=f"MortalityRate({y})",
                        info=f"CauseOfDeath({cie10}),Year({year}),Space({place}),Sex({sex.split(' ')[0]}),Age([{ages}]),MortalityRate({y})",
                        product_type="Map",
                        space=self.__get_space_string(cve_geo),
                        temporal=f"Year({year})",    
                        function_id="plotly.express.choropleth_mapbox",
                        title=fig_title,
                        x_axis=x,
                        y_axis=y)

    def create_age_state_heatmap(self, data: pd.DataFrame, x: str, y: str, z: str, output_path: str,
                        cie10: str, place: str, rate: str, scale: str, hover_data: list, labels: dict,
                        cve_geo: str, sex: str, ages: str, year: str) -> None:
        
        ######## hacer esto fuera de la funcion
        df_cancer_c = estados.complete("ENT_NOMBRE","RANGO_EDAD").fillna(0, downcast='infer')
        df_cancer_c = df_cancer_c.sort_values(by=["TASA_EST_2_1_100k"], ascending=False)
        ########

        fig_title = f"{rate} per {scale} inhabitants, {place}, {sex}, age[{ages}], in {year}"

        fig = px.density_heatmap(data.round(2), 
                                x=x, 
                                y=y, 
                                z=z,
                                width=1080, 
                                height=480,
                                text_auto=True
                                )
        fig.update_layout(
            title=fig_title,
            yaxis_title="Age",
            xaxis_title="State"
        )
        fig.update_yaxes(autorange="reversed")
        fig.update_layout(coloraxis_colorbar_title_text = 'SMR')
        fig.write_image("outputs/heatmap_tasas_estandarizadas_2022.pdf")
        fig.show()

    def __write_metadata(self,
                        name='Default',
                        description=None,
                        data_source_id=None,
                        data_view_id=None,
                        interest_var=None,
                        observable_var=None,
                        info=None,
                        product_type=None,
                        space=None,
                        temporal=None,
                        function_id=None,
                        hue=None,
                        title=None,
                        x_axis=None,
                        y_axis=None,
                        z_axis=None) -> None:
        meta_data = {
            "name": name.split("/")[1],
            "description": description,
            "data_source_id": data_source_id,
            "data_view_id": data_view_id,
            "content_vars": {
                "interest_var": interest_var,
                "observable_var": observable_var,
                "info": info
            },
            "ctx_vars": {
                "product_type": product_type,
                "spatial": space,
                "temporal": temporal
            },
            "plot_desc": {
                "function_id": function_id,
                "hue": hue,
                "title": title,
                "x_axis": x_axis,
                "y_axis": y_axis,
                "z_axis": z_axis
            }
        }

        with open(f"{name}.json", "w") as outfile:
            outfile.write(json.dumps(meta_data, indent=4))
            
    def __get_space_string(self, cve_geo: str) -> str:
        if cve_geo=="00":
            return "Country(Mexico)"
        elif len(cve_geo) == 2 and cve_geo != "00":
            return f"Country(Mexico)->State({cve_geo})"
        elif len(cve_geo) > 2:
            return f"State({cve_geo[:2]})->Municipality({cve_geo[2:]})"
