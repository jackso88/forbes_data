import json
import yaml
import urllib
import requests
import datetime
import re
import feed_functions as ff
from string import Template


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Default time slice
date_now = datetime.datetime.now().date()
date_end = date_now - datetime.timedelta(days=config['common']['days'])

# Getting variables from config
path = config['common']['path']
db = config['db_add']['db']
table = config['covid']['table']
sql = Template(config['covid']['sql']).substitute(path=path, db=db, \
table=table)    

where = urllib.parse.quote_plus("""
{
    "date": {
        "$gte": {
            "__type": "Date",
            "iso": "2020-12-01T01:03:37.625"
        }
    }
}
""")

where = re.sub(r'2020-12-01', '2020-12-01', where)\
.replace('2020-12-01', f'{str(date_end)}') 
print(where)
url = config['covid']['url'] % where

headers = {
    'X-Parse-Application-Id': f"{config['covid']['app_id']}",
    'X-Parse-REST-API-Key': f"{config['covid']['api_key']}"
}
row_data = json.loads(requests.get(url, headers=headers).\
content.decode('utf-8'))

datas = []


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
datas.extend(transform_data(row_data['results']))
ff.write_to_csv(datas)
connect = ff.db_connect()
ff.query_DB(connect, sql)
ff.clean_csv()