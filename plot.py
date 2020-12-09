import yaml
import pymysql
import pygal
import feed_functions as ff
from datetime import datetime, timedelta
from sys import argv


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.load(f)

# Default time slice
date_now = datetime.now().date()
date_end = date_now - timedelta(days=config['common']['days_draw'])


def read_from_DB(connection, sql):
    """ Function for reading data from DB """
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    result = cursor.fetchall()
    connection.close()
    return result


def visual_data(data):
    """ Data visualization """
    line_chart = pygal.Line()
    line_chart.title = 'Data visualization for last month'
    line_chart.x_labels = map(str, [i[1] for i in data[::-1]])
    line_chart.add(option, [i[0] for i in data[::-1]])
    line_chart.render_to_file('chart.svg')


txt = "\nEnter 'average' for average value drawing or "
txt += "country code for visualisation data. For currency count visuali"
txt += "zation enter 'count' "

option = input(txt)

table = config['db_add']['table']

if option == 'average':
    sql = f"select avg(value), date from {table} group by 2 order by 2 "
    sql += f"desc limit {config['common']['days_draw']}"

elif option == 'count':
    sql = f"select count(country_id), date from {table} group by date "
    sql += f"order by 2 desc limit {config['common']['days_draw']}"

elif len(option) == 3:
    sql = f"select value, date from {table} where country_id = '{option}'"
    sql += f" order by date desc limit {config['common']['days_draw']}"

connect = ff.db_connect()
data = read_from_DB(connect, sql)
visual_data(data)
