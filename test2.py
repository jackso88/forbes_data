import requests
import datetime
import smtplib
import pymysql
import logging
import pandas as pd
from sys import argv
from email.mime.multipart import MIMEMultipart     
from email.mime.text import MIMEText     
from email.mime.image import MIMEImage 


#Default time slice
date_now = datetime.datetime.now().date()
date_end = date_now - datetime.timedelta(days=3)

def logining(txt):
	"""Function for loginning"""
	logging.basicConfig(
						filename='test2.log', 
						filemode='w', 
						format='%(name)s - %(levelname)s - %(message)s'
						)
	logging.warning(txt)
	
def mail(err):
	""" Function for sending mail"""
	
	#Data for autorization
	addr_from = "herokusite@gmail.com"                 
	addr_to   = "andreilukin88@gmail.com"                   
	password  = "hb1641012"                                 
	
	#Filling in the address and subject of the message
	msg = MIMEMultipart()                               
	msg['From']    = addr_from                          
	msg['To']      = addr_to                            
	msg['Subject'] = 'ALARM! Broken feed'                   
	
	#Message creating
	body = 'Hi, i am your feed about forbes data, i have some problems. ' + str(err)
	msg.attach(MIMEText(body, 'plain'))                 
	
	#Sending message
	server = smtplib.SMTP('smtp.gmail.com', 587)           
	server.set_debuglevel(True)                        
	server.starttls()                                 
	server.login(addr_from, password)
	try:
		server.send_message(msg)
	except:
		err = 'I lost your mail. ' + str(date_now)
		logining(err)
	server.quit()
    
def db_add(df):
	"""Function for creating data to DB"""
	#Connect to DB
	try:
		connection = pymysql.connect(
									host='localhost',
									user='andrei',
									password='Hb1641012',
									db='test'
									)
	except:
		err = "I can't connect with DB server" + str(date_now)
		mail(err)
		logining(err)
		
	cursor = connection.cursor()
	
	#Deleting data from DB
	try:
		for i, row in df.iterrows():
			info = dict(row)
			cursor.execute("DELETE FROM `forbes` WHERE date=%s", 
			(info['date']))
			connection.commit()
	except:
		err = "I can't delete data from DB. " + str(date_now)
		mail(err)
		logining(err)
		
	cols = "`,`".join([str(i) for i in df.columns.tolist()])
	
	#Inserting data to DB
	try:
		for i, row in df.iterrows():
			sql = "INSERT INTO `forbes` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
			cursor.execute(sql, tuple(row))
			connection.commit()
	except:
		err = "I can't create data at DB. " + str(date_now)
		mail(err)
		logining(err)
		
	result = cursor.fetchall()
	connection.close()
	
def clear_data(dt, date):
	"""Creating dataframe from API response"""
	
	#Raw data processing
	datas, cntr = [], []
	for key, value in dt.items():
		if key == 'rates':
			for k, v in value.items():
				datas.append(v)
				cntr.append(k)
				
	#Creating data frame
	frame = {
	         "country_id":pd.Series(cntr, index = range(0, len(cntr))), 
		     "value":pd.Series(datas, index = range(0, len(datas))),
		     "date":pd.Series(date, index = range(0, len(datas)))
		     }
	df1 = pd.DataFrame(frame)
	db_add(df1)

def start(now, end):	
	"""Function for starting application"""
	#Connecting to API and getting raw data
	while str(now) != str(end):
		url = f'https://openexchangerates.org/api/historical/{str(end)}.json?app_id=c54e441aec6c4f42a2cb17d81a675cae'
		try:
			data = requests.get(url)
		except:
			err = "I can't get data on API. " + str(date_now)
			mail(err)
			logining(err)
			break
		response_dict = data.json()
		clear_data(response_dict, end)
		end += datetime.timedelta(days=1)

# Parameter processing
try:
	if argv[1:]:
		date_par = ''
		for i in argv[1:]:
			date_par += i
		start(date_now, datetime.datetime.strptime(date_par, "%Y-%m-%d").date())
	else:
		start(date_now, date_end)
except:
	err = "I can't start. " + str(date_now)
	mail(err)
	logining(err)
