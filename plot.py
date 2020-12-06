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

connect = ff.db_connect()

def read_from_DB(connection):
    """ Function for reading data from DB """
    table = config['db_add']['table']
    cursor = connection.cursor()
    sql = f"select * from {table} where date >= '{str(date_end)}' "
    sql += f"and value between (select avg(value) from {table})*0.6 "
    sql += f"and (select avg(value) from {table})*1.4;"
    cursor.execute(sql)
    connection.commit()
    result = cursor.fetchall()
    connection.close()
    return result

def transform_data(data_t):
    """ Data preparation function for visualization """
    datas = {}
    date = []
    for element in data_t:
        if element[0] in datas:
            if isinstance(datas[element[0]], list):
                datas[element[0]].append(element[1])
            else:
                datas[element[0]] = [datas[element[0]], element[1]]
        else:
            datas[element[0]] = element[1]
        date.append(str(element[2]))
    datas['dates'] = list(set(date))
    return datas
    
data = read_from_DB(connect)
dict_d = transform_data(data)

line_chart = pygal.Line()
line_chart.title = 'Data visualizing for last month'
line_chart.x_labels = map(str, dict_d['dates'])
for key, value in dict_d.items():
    if key != 'dates':
        line_chart.add(key, value)
line_chart.render_to_file('bar_chart.svg')
