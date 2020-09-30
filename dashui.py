import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html

import argparse
import mysql.connector

import pandas as pd

app = dash.Dash('Business Online')

# db connection
CONN = None

app.layout = html.Div([
    dcc.Dropdown(
        id='my-dropdown',
        options=[
            {'label': 'Microsoft', 'value': 'Microsoft'},
            {'label': 'Tesla', 'value': 'Tesla'},
            {'label': 'Apple', 'value': 'Apple'}
        ],
        value='Microsoft'
    ),
    dcc.Graph(id='my-graph')
], style={'width': '500'})

@app.callback(Output('my-graph', 'figure'), [Input('my-dropdown', 'value')])
def update_graph(selected_dropdown_value):
    company_name = selected_dropdown_value

    cursor = CONN.cursor()
    cursor.execute("select date, metric_value from company_metrics where company_name=%s and metric_name = 'mentions' order by date asc", (company_name) )

    dates = []
    values = []

    for (date, value) in cursor:
        print("Result is {}, {}".format(date, value))
        dates.append(date)
        values.append(value)

    cursor.close()

    df = pd.DataFrame({ "date": dates, "mentions": values })

    return {
        'data': [{
            'x': df.date,
            'y': df.mentions
        }],
        'layout': {'margin': {'l': 40, 'r': 0, 't': 20, 'b': 30}}
    }

app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})

def parse_arguments():
    """ Returns the parsed arguments from the command line """

    arg_parser = argparse.ArgumentParser(prog="dashui",
                                        description="dashui",
                                        conflict_handler='resolve')
    arg_parser.add_argument("--endpoint", type=str, required=True,
                            help="MySQL endpoint")
    arg_parser.add_argument("--user", type=str, required=True,
                            help="User name")
    arg_parser.add_argument("--password", type=str, required=True,
                            help="Password")
    arg_parser.add_argument("--db", type=str, required=True,
                            help="Database name")

    args = arg_parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_arguments()
    CONN = mysql.connector.connect(user=args.user, password=args.password,
                                   host=args.endpoint,
                                   database=args.db)

    cursor = CONN.cursor()
    cursor.execute("select distinct company_name from company_metrics order by company_name asc")

    options = []
    for company in cursor:
        options.append({'label': company, 'value': company})
    cursor.close()

    app.layout = html.Div([
        dcc.Dropdown(
            id='my-dropdown',
            options=options,
            value='Microsoft'
        ),
        dcc.Graph(id='my-graph')
    ], style={'width': '500'})

    app.run_server()