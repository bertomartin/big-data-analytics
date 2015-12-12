# this script hits the estimize api and brings back quaterly data
import requests
import json
from datetime import datetime, date, time
from operator import itemgetter
import csv
import os
import pdb

API_KEY = os.environ['ESTIMIZE_API_KEY']
headers = {'X-Estimize-Key': API_KEY, 'Accept': 'application/json'}

def eps_estimates(ticker):
    url = "http://api.estimize.com/companies/{0}/releases".format(ticker)
    req = requests.get(url, headers=headers)
    estimates = req.json()
    sorted_estimates = sorted(estimates, key=itemgetter('fiscal_year', 'fiscal_quarter'), reverse=True)
    write_csv(ticker, sorted_estimates)


def write_csv(ticker, estimates):
    '''(string, list of dicts) -> ticker_estimate.csv

    Writes the returned estimates from estimize to a file named after the ticker
    '''
    data_string = 'ticker,date,eps,wallstreet_estimates,estimize_estimates,fiscal_year,fiscal_quarter\n'
    for e in estimates:
        dt_string = e['release_date'].split("T")[0] #throw away time information
        dt = datetime.strptime(dt_string,'%Y-%m-%d')
        data_string += "{0},{1},{2},{3},{4},{5},{6}\n".format(ticker, dt.date(), e['eps'],e['wallstreet_eps_estimate'],
        e['consensus_eps_estimate'],e['fiscal_year'],e['fiscal_quarter'])

    filename = "./stocks/" + ticker + "_estimize.csv"
    with open(filename, 'w') as f:
        f.write(data_string)


if __name__ == '__main__':
    with open('./finviz_tech.csv', 'rU') as csvfile:
        stockreader = csv.reader(csvfile)
        for row in stockreader:
            eps_estimates(row[0])
