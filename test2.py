import yaml
import datetime
import feed_functions as ff
from sys import argv
from string import Template


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.load(f)

# Default time slice
date_now = datetime.datetime.now().date()
date_end = date_now - datetime.timedelta(days=config['common']['days'])

# Parameter processing
if argv[1:]:
    date_par = ''.join([str(i) for i in argv[1:]])
    date_par = datetime.datetime.strptime(date_par, "%Y-%m-%d")
    date_par = date_par.date()
    date_end = date_par

# Starting app
ff.logining()
while str(date_now) != str(date_end):
    try:
        url = Template(config['start']['url']).substitute(date=date_end)
        try:
            response_dict = ff.resp(url)
        except:
            err = config['start']['err_str']
            ff.logining(err)
            ff.mail(err)
        data_frame = ff.clear_data(response_dict, str(date_end))
        insert = ff.insert_sql(data_frame)
        delete = ff.delete_sql(data_frame)
        try:
            connect = ff.db_connect()
        except:
            err = config['db_add']['err_con']
            ff.mail(err)
            ff.logining(err)
        try:
            ff.db_add(connect, delete, insert)
        except:
            err = config['db_add']['err_add']
            ff.mail(err)
            ff.logining(err)
        date_end += datetime.timedelta(days=1)
    except:
        err = config['common']['err_com']
        ff.mail(err)
        ff.logining(err)
        break
