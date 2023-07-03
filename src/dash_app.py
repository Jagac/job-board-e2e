from dash import Dash, dcc, html
import plotly.express as px
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objects as go
import dask.dataframe as dd
import yaml
import os
import psutil
import math

with open("config.yaml", "r") as file:
    global_variables = yaml.safe_load(file)
    
csv_path = global_variables['paths_to_save']['csv_path']
ddf = dd.read_csv(f'{csv_path}\\*.csv')
ddf = ddf.drop_duplicates(subset=['ID']).compute()
ddf['Salary From'] = ddf['Salary From'].fillna(0)

app = Dash(
    external_stylesheets = [dbc.themes.DARKLY])

colors = {
    'black' : '#1A1B25',
    'red' : '#F8C271E',
    'white' : '#EFE9E7',
    'background' : '#333333',
    'text' : '#FFFFFF'
}


map_fig = px.scatter_mapbox(ddf, lat="Latitude", lon="Longitude", 
                        color="Skills", size="Salary From",
                  color_continuous_scale=px.colors.cyclical.IceFire,
                  mapbox_style= 'open-street-map', zoom=3, 
                  )


map_fig.update_layout(
    plot_bgcolor=colors['background'],
    paper_bgcolor=colors['background'],
    font_color=colors['text']
)


app.layout = html.Div(className = 'document', children=[
    html.H1(children = "Welcome to the app", className = "text-center p-2", style = {'color': '#EFE9E7'}),
    html.H3(children = "The home of the best IT job statistics", className = "text-center p-1 text-light "),
    html.Div([
            html.H6(children='Memory Consumed',
                    style={
                        'textAlign': 'center',
                        'color': '#EFE9E7'}
                    ),

            html.P(f"{math.trunc(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)} MB",
                style={
                    'textAlign': 'center',
                    'color': '#EFE9E7',
                    'fontSize': 30}
                )],
             
        
            
    )])
            


            



if __name__ == '__main__':
    app.run_server(debug=True)