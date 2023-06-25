from dash import Dash, dcc, html
import plotly.express as px
import dask.dataframe as dd


ddf = dd.read_csv(r'C:\Users\jagos\Documents\GitHub\job-board-e2e\csv_data\*.csv')

