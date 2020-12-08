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
        if element[2] not in date:
            date.append(element[2])
    datas['dates'] = date
    return datas

connect = ff.db_connect()

txt = "\nEnter '0' for average value drawing or '1' for slicing data "
option = int(input(txt))

table = config['db_add']['table']

if option == 1:
    sql = f"select * from {table} where date >= '{str(date_end)}' "
    sql += f"and value between (select avg(value) from {table})*0.6 "
    sql += f"and (select avg(value) from {table});"    
    data = read_from_DB(connect, sql)
    dict_d = transform_data(data)
    line_chart = pygal.Line()
    line_chart.title = 'Data slice visualization for last month'
    line_chart.x_labels = map(str, dict_d['dates'])
    for key, value in dict_d.items():
        if key != 'dates':
            line_chart.add(key, value)
    line_chart.render_to_file('un_chart.svg')
    
elif option == 0:
    sql = f"select avg(value), date from {table} group by 2 order by 2 "
    sql += f"desc limit {config['common']['days_draw']}"
    data = read_from_DB(connect, sql)
    line_chart = pygal.Line()
    line_chart.title = 'AVG value data visualization for last month'
    line_chart.x_labels = map(str, [i[1] for i in data[::-1]])
    line_chart.add('AVG value', [i[0] for i in data[::-1]])
    line_chart.render_to_file('avg_chart.svg')
