import yaml
import pymysql
import pygal
import feed_functions as ff
from sys import argv
from string import Template


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Getting variables from config file
kwargs = {
    'table': config['db_add']['table'],
    'day': config['plot']['day'],
    'option': 'average'
        }
sql = Template(config['plot']['sql']).substitute(**kwargs)


# Parameter processing
if argv[1:]:
    kwargs['option'] = argv[1]
    if argv[1] == 'count':
        sql = Template(config['plot']['sql2']).substitute(**kwargs)
    elif len(argv[1]) == 3:
        sql = Template(config['plot']['sql3']).substitute(**kwargs)
    else:
        sql = argv[1]
    


def visual_data(data,**kwargs):
    """ Data visualization """
    line_chart = pygal.Line()
    line_chart.title = f"""Data visualization for last {kwargs['day']} 
        day(s)"""
    line_chart.x_labels = map(str, [i[1] for i in data[::-1]])
    line_chart.add(kwargs['option'], [i[0] for i in data[::-1]])
    line_chart.render_to_file('chart.svg')


# Functions calling
connect = ff.db_connect()
data = ff.query_DB(connect, sql)
visual_data(data, **kwargs)
