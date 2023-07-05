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
from datetime import datetime


with open("config.yaml", "r") as file:
    global_variables = yaml.safe_load(file)
    
csv_path = global_variables['paths_to_save']['csv_path']
ddf = dd.read_csv(f'{csv_path}\\*.csv')


app = Dash(__name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}], 
           external_stylesheets = [dbc.themes.DARKLY])

colors = {
    'black' : '#1A1B25',
    'red' : '#F8C271E',
    'white' : '#EFE9E7',
    'background' : '#333333',
    'text' : '#FFFFFF'
}


def create_map(ddf):
    ddf = ddf.drop_duplicates(subset=['ID']).compute()
    ddf['Salary From'] = ddf['Salary From'].fillna(0)

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

    return map_fig


def create_smt(ddf):
    ddf = ddf.drop_duplicates(subset=['ID']).compute()
    ddf['Salary From'] = ddf['Salary From'].fillna(0)
    ddf['Experience Level'] = ddf['Experience Level'].str[:10]
    
    test = px.scatter(ddf, x = 'Salary From', y = 'Experience Level', size = 'Salary From', 
            hover_name = 'Title', color = 'Title', 
            color_discrete_sequence=px.colors.qualitative.Alphabet,
            ).update_yaxes(categoryarray = ['mid', 'senior', 'junior'])

    test.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )
    
    return test


def create_hist(ddf):
    ddf = ddf.drop_duplicates(subset=['ID']).compute()
    fig = px.histogram(ddf, x="Remote")
    
    fig.update_layout(
        plot_bgcolor=colors['background'],
        paper_bgcolor=colors['background'],
        font_color=colors['text']
    )

    return fig



job_map = create_map(ddf)
test_graph = create_smt(ddf)
test_hist = create_hist(ddf)


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
            dbc.Col([dcc.Graph(figure=test_graph)]),
        ])
    ]),
    
    html.Div([
        dbc.Row([
            dbc.Col([dcc.Graph(figure=job_map)]),
        ])
    ])
])

            
if __name__ == '__main__':
    app.run_server(debug=True, use_reloader=True)