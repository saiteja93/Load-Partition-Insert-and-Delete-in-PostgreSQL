#!usr/bin/python2.7

import psycopg2
import datetime
import time

#trying to connect
try: 
	con = psycopg2.connect(user='postgres', password='1234', dbname='postgres')
except:
	print "connection failed"
cur = con.cursor()

def Load_Ratings(s)
