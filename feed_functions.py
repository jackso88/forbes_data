import requests
import smtplib
import pymysql
import logging
import csv
import yaml
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)

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


def mail(err, date_now, date_end):
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


def resp(url):
    """ Function for geting response"""
    # Connecting to API and getting raw data
    data = requests.get(url)
    response_dict = data.json()
    return response_dict

    
def transform_dict(dt, date):
    datas = []
    for key, value in dt.items():
        datas.append([str(key),str(value),str(date)])
    return datas


def write_to_csv(datas):
    name = config['common']['path']
    with open(name, 'a') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quoting=csv.QUOTE_MINIMAL)
        writer.writerows(datas)
         
            
def clean_csv():
    """ Function for cleaning CSV file """
    name = config['common']['path']
    with open(name, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        writer.writerow('')


def query_DB(connection, sql):
    """ Function for DB queries """
    cursor = connection.cursor()
    cursor.execute(sql)
    connection.commit()
    result = cursor.fetchall()
    connection.close()
    return result
