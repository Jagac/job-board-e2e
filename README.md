# IT Job Posting Data App

This project grabs data from a job posting website and uses Plotly/Dash for visualizations.

## Prerequisites

App needs a virtualenv library to be globally installed as it creates the environment by itself. Therefore simply run a:

    pip install virtualenv

## Getting Started

Paths should be configured in the config.yaml file. They dictate where the data is stored and read by the dash app. 

### Running

To run the app (python 3.11.4):

    python app.py

Specify your operating system and let the app do the rest. Then simply go to localhost:8050 in your browser.
The Web app may cause an timeout error but this is due to the map taking a long time to load (many marker locations). Just need some patience :)



