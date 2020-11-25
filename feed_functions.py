import requests
import smtplib
import pymysql
import logging
import yaml
import pandas as pd
from sys import argv
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.load(f)

def logining(txt=str('Program started. ')):
    """ Function for loginning """
    logging.basicConfig(
                        filename=config['loggining']['filename'],
                        filemode=config['loggining']['filemode'],
                        level=logging.DEBUG,
                        format=config['loggining']['format']
                       )
    if 'started' in txt:
        logging.info(txt)
    else:
        logging.error(txt)


def mail(err):
    """ Function for sending mail """
    # Data for autorization
    addr_from = config['mail']['addr_from']
    addr_to = config['mail']['addr_to']
    password = config['mail']['password']

    # Filling in the address and subject of the message
    msg = MIMEMultipart()
    msg['From'] = addr_from
    msg['To'] = addr_to
    msg['Subject'] = config['mail']['subj']

    # Message creating
    body = config['mail']['body'] + str(err) + str(date_now)
    msg.attach(MIMEText(body, 'plain'))

    # Sending message
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.set_debuglevel(False)
    server.starttls()
    server.login(addr_from, password)
    server.send_message(msg)
    server.quit()


def insert_sql(df):
    """ Function for creating insert query"""
    ins_sql = []
    table = config['db_add']['table']
    cols = "`,`".join([str(i) for i in df.columns.tolist()])
    for i, row in df.iterrows():
        sql = f"INSERT INTO `{table}` (`" + cols + "`) VALUES $s"
        sql = Template(sql).substitute(s=tuple(row))
        ins_sql.append(str(sql))
    return(ins_sql)


def delete_sql(df):
    """ Function for creating delete query """
    del_sql = []
    table = config['db_add']['table']
    for i, row in df.iterrows():
        info = dict(row)
        sql = f"DELETE FROM {table} WHERE date = '$s'"
        sql = Template(sql).substitute(s=info['date'])
        if sql not in del_sql:
            del_sql.append(sql)
    return(del_sql)


def db_connect():
    """ Function for connecting to DB """
    # Connect to DB
    connection = pymysql.connect(
                            host=config['db_add']['host'],
                            user=config['db_add']['user'],
                            password=config['db_add']['password'],
                            db=config['db_add']['db']
                                )
    return connection


def db_add(connection, delete, insert):
    """ Function for transfer data to DB """
    cursor = connection.cursor()
    cursor.execute('START TRANSACTION;')
    for i in delete:
        cursor.execute(i)
    for i in insert:
        cursor.execute(i)
    cursor.execute('COMMIT;')
    connection.commit()
    connection.close()


def clear_data(dt, date):
    """ Creating dataframe from API response """
    # Raw data processing
    datas, cntr = [], []
    for key, value in dt.items():
        if key == 'rates':
            for k, v in value.items():
                datas.append(v)
                cntr.append(k)

    # Creating data frame
    frame = {
        "country_id": pd.Series(cntr, index=range(0, len(cntr))),
        "value": pd.Series(datas, index=range(0, len(datas))),
        "date": pd.Series(date, index=range(0, len(datas)))
        }
    df1 = pd.DataFrame(frame)
    return df1


def resp(url):
    """ Function for geting response"""
    # Connecting to API and getting raw data
    data = requests.get(url)
    response_dict = data.json()
    return response_dict
