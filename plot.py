import yaml
import pymysql
import pygal
import feed_functions as ff


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

txt = "For average value drawing enter 'average'\nFor country specific"
txt += " visualization enter three-digit country code\nFor currency "
txt += "count visualization enter 'count' "

option = input(txt)

txt_day = "\nHow many last days should I visualize? "

day = input(txt_day)

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
    line_chart.title = f'Data visualization for last {day} day(s)'
    line_chart.x_labels = map(str, [i[1] for i in data[::-1]])
    line_chart.add(option, [i[0] for i in data[::-1]])
    line_chart.render_to_file('chart.svg')


table = config['db_add']['table']

if option == 'average':
    sql = f"select avg(value), date from {table} group by 2 order by 2 "
    sql += f"desc limit {day}"

elif option == 'count':
    sql = f"select count(country_id), date from {table} group by date "
    sql += f"order by 2 desc limit {day}"

elif len(option) == 3:
    sql = f"select value, date from {table} where country_id = '{option}'"
    sql += f" order by date desc limit {day}"

connect = ff.db_connect()
data = read_from_DB(connect, sql)
visual_data(data)
