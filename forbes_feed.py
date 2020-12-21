import yaml
import datetime
import feed_functions as ff
from sys import argv
from string import Template


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Default time slice
date_now = datetime.datetime.now().date()
date_end = date_now - datetime.timedelta(days=config['common']['days'])

# Getting variables from config
kwargs = {
    'path': config['common']['path'],
    'db':  config['db_add']['db'],
    'table': config['db_add']['table']
    }
sql = Template(config['start']['sql']).substitute(**kwargs)
sql2 = config['start']['sql2']

# Parameter processing
if argv[1:]:
    date_par = ''.join([str(i) for i in argv[1:]])
    date_par = datetime.datetime.strptime(date_par, "%Y-%m-%d")
    date_par = date_par.date()
    date_end = date_par

# Starting app
ff.logining()
rates_list = []
while str(date_now) != str(date_end):
    try:
        url = Template(config['start']['url']).substitute(date=date_end)
        try:
            response_dict = ff.resp(url)
        except Exception as e:
            err = config['start']['err_str']
            ff.logining(err)
            ff.mail(err + e, date_now, date_end)
        rates_list.extend(ff.transform_dict(response_dict['rates'], \
        date_end))
        date_end += datetime.timedelta(days=1)
    except:
        err = config['common']['err_com']
        ff.mail(err, date_now, date_end)
        ff.logining(err)
        break
ff.write_to_csv(rates_list)

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
try:
    connect = ff.db_connect()
except:
    err = config['db_add']['err_con']
    ff.mail(err, date_now, date_end)
    ff.logining(err)
try:
    ff.query_DB(connect, sql2)
except:
    err = config['db_add']['err_add']
    ff.mail(err, date_now, date_end)
    ff.logining(err)
