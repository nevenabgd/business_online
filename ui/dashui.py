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

@app.callback(Output('my-graph', 'figure'), Output('my-graph2', 'figure'), [Input('my-dropdown', 'value')])
def update_graph(selected_dropdown_value):  
    companies = selected_dropdown_value
    print(companies)
    if not companies:
        companies = [""]

    data_mentions = []
    data_sentiment = []

    for company_name in companies:
        print(company_name)
        cursor = CONN.cursor()
        cursor.execute("select date, metric_value from company_metrics where company_name=%s and metric_name = 'mentions' order by date asc", (company_name,))
        dates = []
        values = []
        for (date, value) in cursor:
            print("Result is {}, {}".format(date, value))
            dates.append(date)
            values.append(value)
        df_mentions = pd.DataFrame({ "date": dates, "mentions": values })

        data_mentions.append(
            {
                'name': company_name,
                'x': df_mentions.date,
                'y': df_mentions.mentions,
                'type': 'bar'
            }
        )

        cursor.execute("select date, metric_value from company_metrics where company_name=%s and metric_name = 'sentiment' order by date asc", (company_name,))
        dates = []
        values = []
        for (date, value) in cursor:
            print("Result is {}, {}".format(date, value))
            dates.append(date)
            values.append(value)
        df_sentiment = pd.DataFrame({ "date": dates, "sentiment": values })

        data_sentiment.append(
            {
                'name': company_name,
                'x': df_sentiment.date,
                'y': df_sentiment.sentiment,
                'type': 'bar'
            }
        )

    cursor.close()

    return ({
        'data': data_mentions,
        'layout': {'margin': {'l': 100, 'r': 100, 't': 100, 'b': 100}, 'title': 'Mentions'}
    },
    {
        'data': data_sentiment,
        'layout': {'margin': {'l': 100, 'r': 100, 't': 100, 'b': 100}, 'title': 'Sentiment'}
    })

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
    for row in cursor:
        company = str(row[0])
        options.append({'label': company, 'value': company})
    cursor.close()

    app.layout = html.Div([
        html.Div([
            dcc.Dropdown(
                id='my-dropdown',
                options=options,
                multi=True,
                placeholder='Select companies...')
        ], style={'display': 'inline-block', 'height': '40px', 'width': '600px'}),
        html.Div([
            dcc.Graph(id='my-graph'),
            dcc.Graph(id='my-graph2')
        ])
    ], style={'width': '10'})

    app.run_server()