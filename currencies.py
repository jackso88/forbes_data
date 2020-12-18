import yaml
import feed_functions as ff
from string import Template 

# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

path = config['common']['path']
db = config['db_add']['db']
table = config['currencies']['table']
sql = Template(config['covid']['sql']).substitute(path=path, db=db, \
table=table)
sql2 = Template(config['currencies']['sql']).substitute(path=path, db=db, \
table=table)
url = config['currencies']['url']  


def currency(resp_dict, country):
    """ preparing response API """
    clear_data = []
    for key, value in response_dict.items():
        if key == 'AED':
            clear_data.append([str(key), str(value), 'United Arab Emirates'])
        elif key == 'GBP':
            clear_data.append([str(key), str(value), 'Greate Britain'])
        elif key == 'KPW':
            clear_data.append([str(key), str(value), 'North Korea'])
        elif key =='KRW':
            clear_data.append([str(key), str(value), 'South Korea'])
        elif key =='SHP':
            clear_data.append([str(key), str(value), 'Saint Helena'])
        elif key == 'TMT':
            clear_data.append([str(key), str(value), 'Turkmenistan'])
        elif key == 'TRY':
            clear_data.append([str(key), str(value), 'Turkey'])
        elif key == 'TWD':
            clear_data.append([str(key),str(value),'Taiwan'])
        elif key == 'ZAR':
            clear_data.append([str(key),str(value),'South Africa'])
        elif key == 'AUD':
            clear_data.append([str(key),str(value),'Australia'])
        elif key == 'USD':
            clear_data.append([str(key),str(value),'United States'])
        else:
            for el in country:
                for i in el:
                    if i[:7] in value:
                        clear_data.append([str(key), str(value), str(i)])
    return clear_data

response_dict = ff.resp(url)
connect = ff.db_connect() 
countries = ff.query_DB(connect, sql2) 
data = currency(response_dict, countries)
ff.write_to_csv(data)
connect = ff.db_connect() 
ff.query_DB(connect, sql)
ff.clean_csv()


    
