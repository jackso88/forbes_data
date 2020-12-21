import yaml
import feed_functions as ff
from string import Template 

# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

kwargs = {
    'path': config['common']['path'],
    'db': config['db_add']['db'],
    'table': config['currencies']['table']
    }
    
sql = Template(config['currencies']['sql']).substitute(**kwargs)
sql2 = Template(config['currencies']['sql2']).substitute(**kwargs)
url = config['currencies']['url']  


def currency(resp_dict, country):
    """ preparing response API """
    clear_data = []
    for key, value in response_dict.items():
        if key == 'AED':
            clear_data.append([key,value,'United Arab Emirates'])
        elif key == 'GBP':
            clear_data.append([key,value,'Greate Britain'])
        elif key == 'KPW':
            clear_data.append([key,value,'North Korea'])
        elif key =='KRW':
            clear_data.append([key,value,'South Korea'])
        elif key =='SHP':
            clear_data.append([key,value,'Saint Helena'])
        elif key == 'TMT':
            clear_data.append([key,value,'Turkmenistan'])
        elif key == 'TRY':
            clear_data.append([key,value,'Turkey'])
        elif key == 'TWD':
            clear_data.append([key,value,'Taiwan'])
        elif key == 'ZAR':
            clear_data.append([key,value,'South Africa'])
        elif key == 'AUD':
            clear_data.append([key,value,'Australia'])
        elif key == 'USD':
            clear_data.append([key,value,'United States'])
        elif key == 'CRC':
            clear_data.append([key,value,'Costa Rica'])
        elif key == 'DKK':
            clear_data.append([key,value,'Denmark'])
        elif key == 'PLN':
            clear_data.append([key,value,'Poland'])
        else:
            for el in country:
                for i in el:
                    if i[:4] in value[:4]:
                        clear_data.append([key,value,i])
        if key not in [i[0] for i in clear_data]:
            clear_data.append([key,value,'NULL'])
    return clear_data

response_dict = ff.resp(url)
connect = ff.db_connect() 
countries = ff.query_DB(connect, sql)
data = currency(response_dict, countries)
ff.write_to_csv(data)
connect = ff.db_connect() 
ff.query_DB(connect, sql2)
ff.clean_csv()

    
