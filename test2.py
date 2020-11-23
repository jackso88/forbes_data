import requests
import datetime
import smtplib
import pymysql
import logging
import yaml
import pandas as pd
from sys import argv
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage


# Reading configuration file
with open("config.yaml", "r") as f:
    config = yaml.load(f)

# Default time slice
date_now = datetime.datetime.now().date()
date_end = date_now - datetime.timedelta(days=config['common']['days'])


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


def db_add(df):
    """ Function for creating data to DB """
    # Connect to DB
    try:
        connection = pymysql.connect(
                                host=config['db_add']['host'],
                                user=config['db_add']['user'],
                                password=config['db_add']['password'],
                                db=config['db_add']['db']
                                    )
    except:
        err = config['db_add']['err_con']
        mail(err)
        logining(err)

    cursor = connection.cursor()
    table = config['db_add']['table']
    cols = "`,`".join([str(i) for i in df.columns.tolist()])

    # Transfer data to DB
    try:
        for i, row in df.iterrows():
            info = dict(row)
            sql_del = f"DELETE FROM `{table}` WHERE date=%s"
            sql_del += "AND country_id=%s;"
            cursor.execute(
                sql_del, (info['date'], info['country_id'])
                )
            sql = f"INSERT INTO `{table}` (`" + cols + "`) "
            sql += "VALUES (%s, %s, %s);"
            cursor.execute(sql, tuple(row))
    except:
        err = config['db_add']['err_del']
        mail(err)
        logining(err)

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
    db_add(df1)


def start(now, end):
    """ Function for starting application """
    # Connecting to API and getting raw data
    while str(now) != str(end):
        url = f"https://openexchangerates.org/api/historical/{str(end)}"
        url += f".json?app_id={str(config['start']['app_id'])}"
        try:
            data = requests.get(url)
        except:
            err = config['start']['err_str']
            mail(err)
            logining(err)
            break
        response_dict = data.json()
        clear_data(response_dict, end)
        end += datetime.timedelta(days=1)


# Parameter processing
try:
    if argv[1:]:
        date_par = ''.join([str(i) for i in argv[1:]])
        date_par = datetime.datetime.strptime(date_par, "%Y-%m-%d")
        date_par = date_par.date()
        start(date_now, date_par)
    else:
        start(date_now, date_end)
    logining()
except:
    err = config['common']['err_com']
    mail(err)
    logining(err)
