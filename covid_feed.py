import json
import yaml
import urllib
import requests
import datetime
import feed_functions as ff
from string import Template


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Getting variables from config
kwargs = {
    'path':config['common']['path'],
    'db': config['db_add']['db'],
    'table': config['covid']['table'],
    'days': config['common']['days']
    }
sql = Template(config['covid']['sql']).substitute(**kwargs)

# Default time slice
date_now = datetime.datetime.now().date()
date_end = date_now - datetime.timedelta(kwargs['days'])

tt = {
    "date": {
        "$gte": {
            "__type": "Date",
            "iso": date_end.strftime("%Y-%m-%dT00:00:00.000")
        }
    }
}

where = urllib.parse.quote_plus(json.dumps(tt))

url = config['covid']['url'] % where

headers = {
    'X-Parse-Application-Id': f"{config['covid']['app_id']}",
    'X-Parse-REST-API-Key': f"{config['covid']['api_key']}"
}

try:
    row_data = json.loads(requests.get(url, headers=headers).\
    content.decode('utf-8'))
except Exception as e:
    err = config['start']['err_str']
    ff.logining(err)
    ff.mail(err + e, date_now, date_end)


def transform_data(data):
    """ Function for data transformation """
    clean_data = []
    # Exceptions processing
    for i in data:
        if i['countryName'][0].isdigit() == False \
        and len(i['countryName']) < 50 \
        and i['countryName'] != 'Confirmed cases':
            if i['countryName'] == \
            'Tanzania{{efn\n{{olist|list_style_type=none':
                i['countryName'] = 'Tanzania'
            if str(i['cases']) == 'None':
                i['cases'] = 'NULL'
            if str(i['deaths']) == 'None':
                i['deaths'] = 'NULL'
            if str(i['recovered']) == 'None':
                i['recovered'] = 'NULL'
            clean_data.append([i['countryName'], str(i['cases']), \
            str(i['deaths']), str(i['recovered']), \
            str(i['date']['iso'][:10])])
    return clean_data
 
# Calling funtions 
ff.logining()
datas = []
datas.extend(transform_data(row_data['results']))
ff.write_to_csv(datas)
try:
    connect = ff.db_connect()
except:
    err = config['db_add']['err_con']
    ff.mail(err, date_now, date_end)
    ff.logining(err)
try:
    ff.query_DB(connect, sql)
except:
    err = config['db_add']['err_add']
    ff.mail(err, date_now, date_end)
    ff.logining(err)
ff.clean_csv()
