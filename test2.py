import requests
import datetime
import smtplib
import pymysql
import pandas as pd
from email.mime.multipart import MIMEMultipart     
from email.mime.text import MIMEText     
from email.mime.image import MIMEImage 


a = datetime.datetime.now().date()
b = a - datetime.timedelta(days=3)

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
    server.send_message(msg)
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
		err = "I can't connect with DB server" + str(a)
		mail(err)
		
	cursor = connection.cursor()
	
	#Deleting data from DB
	try:
		for i, row in df.iterrows():
			info = dict(row)
			cursor.execute("DELETE FROM `forbes` WHERE date=%s", 
			(info['date']))
			connection.commit()
	except:
		err = "I can't delete data from DB. " + str(a)
		mail(err)
		
	cols = "`,`".join([str(i) for i in df.columns.tolist()])
	
	#Inserting data to DB
	try:
		for i, row in df.iterrows():
			sql = "INSERT INTO `forbes` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
			cursor.execute(sql, tuple(row))
			connection.commit()
	except:
		err = "I can't create data at DB. " + str(a)
		mail(err)
		
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
	
#Connecting to API and getting raw data	
while str(b) != str(a):
	url = f'https://openexchangerates.org/api/historical/{str(b)}.json?app_id=c54e441aec6c4f42a2cb17d81a675cae'
	try:
		data = requests.get(url)
	except:
		err = "I can't get data on API. " + str(a)
		mail(err)
		break
		
	response_dict = data.json()
	clear_data(response_dict, b)
	b += datetime.timedelta(days=1)
