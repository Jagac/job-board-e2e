import pandas as pd
from dash import Dash, dcc, html, Input, Output, callback
import plotly.express as px
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import math
from datetime import datetime
import os
import glob
import yaml
import psutil


with open("./config.yaml", "r") as file:
    global_variables = yaml.safe_load(file)
    
csv_path = global_variables['paths_to_save']['csv_path']

all_files = glob.glob(os.path.join(csv_path, '*.csv')) 
df_from_each_file = (pd.read_csv(f) for f in all_files)
ddf = pd.concat(df_from_each_file, ignore_index=True)
ddf = ddf.drop_duplicates(subset=['ID'])
ddf['Salary From'] = ddf['Salary From'].fillna(0)
    
app = Dash(__name__,meta_tags=[{"name": "viewport", "content": "width=device-width"}],
           external_stylesheets = [dbc.themes.DARKLY])

colors = {
    'black' : '#1A1B25',
    'red' : '#F8C271E',
    'white' : '#EFE9E7',
    'background' : '#333333',
    'text' : '#FFFFFF'
}

def create_map(ddf):
    map_fig = px.scatter_mapbox(ddf, lat="Latitude", lon="Longitude", 
                            color="Skills", size="Salary From",
                            color_continuous_scale=px.colors.cyclical.IceFire,
                            mapbox_style= 'open-street-map', zoom=3, 
                            title="Posting Locations")

    map_fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )

    return map_fig


def create_scatter(ddf):
    scatter = px.scatter(ddf, x = 'Salary From', y = 'Experience Level', size = 'Salary From', 
                    hover_name = 'Title', color = 'Title', 
                    color_discrete_sequence=px.colors.qualitative.Alphabet,
                    title='Experience Level VS Salary'
                    ).update_yaxes(categoryarray = ['junior', 'mid', 'senior'])

    scatter.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    
    return scatter


def create_hist(ddf):
    ddf = ddf.drop_duplicates(subset=['ID'])
    hist = px.histogram(ddf, x="Remote",color="Employment Types", title="Remote Job Frequency")
    
    hist.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )

    return hist


def create_hist_2(ddf):
    ddf['Title'] = ddf['Title'].str[:10]
    hist = px.histogram(ddf, x="Skills", color="Experience Level", title="Skills and Experience",
                        range_x=(0, 10)).update_xaxes(categoryorder='total descending')
    
    hist.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    hist.update_layout(bargap=0.2)
    
    return hist

job_map = create_map(ddf)
scatter_graph = create_scatter(ddf)
hist_graph = create_hist(ddf)
hist_graph_2 = create_hist_2(ddf)

app.layout = html.Div(className = 'document', children=[
    html.H1(children = "Welcome to the Job Board App", className = "text-center p-2", style = {'color': '#EFE9E7'}),
    html.H3(children = "Home of the Best IT Job Statistics", className = "text-center p-1 text-light "),
    html.Div([
        dbc.Row([
            dbc.Col([html.H6(children='Number of Unique Postings',
                    style={
                        'textAlign': 'center',
                        'color': '#EFE9E7'}
                    ),

                    html.P(f"{len(ddf['ID'].unique())}",
                        style={
                            'textAlign': 'center',
                            'color': '#EFE9E7',
                            'fontSize': 30}
                        )]),
            
            dbc.Col([html.H6(children='Current Memory Consumption',
                    style={
                        'textAlign': 'center',
                        'color': '#EFE9E7'}
                    ),
                     
                    html.P(f"{math.trunc(psutil.Process(os.getpid()).memory_info().rss / 1024 ** 2)} MB",
                        style={
                            'textAlign': 'center',
                            'color': '#EFE9E7',
                            'fontSize': 30}
                        )]),
            
            dbc.Col([html.H6(children='Current Time',
                    style={
                        'textAlign': 'center',
                        'color': '#EFE9E7'}
                    ),
                     
                    html.P(f"{datetime.now().strftime('%m/%d/%Y %H:%M:%S')}",
                        style={
                            'textAlign': 'center',
                            'color': '#EFE9E7',
                            'fontSize': 30}
                        )]),
            ])
        ]),
    
    html.Div([
        dbc.Row([
            dbc.Col([dcc.Graph(figure=scatter_graph)]),
        ])
    ]),
    
    html.Div([
        dbc.Row([
            dbc.Col([dcc.Graph(figure=hist_graph)]),
            dbc.Col([dcc.Graph(figure=hist_graph_2)])
        ])
    ]),
    
    html.Div([
        dbc.Row([
            dbc.Col([dcc.Graph(figure=job_map)]),
        ])
    ]),
    
])
 
            
if __name__ == '__main__':
    app.run_server(debug=True)