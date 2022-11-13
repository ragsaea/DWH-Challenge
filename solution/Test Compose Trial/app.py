import asyncio
import aiohttp
import os
import time
import pandas as pd
from datetime import datetime as dt
import requests as req
from pandas import json_normalize
import pyodbc
from fast_to_sql import fast_to_sql as fts
import connectorx as cx
import sqlalchemy
from pandasql import sqldf
import re 
import numpy as np
from time import strftime
from time import gmtime
from humanfriendly import format_timespan
import requests
from requests_negotiate import HTTPNegotiateAuth
import phonenumbers
import json
from os import listdir
from os.path import isfile, join
import glob
from tabulate import tabulate

import redis
from flask import Flask

app = Flask(__name__)
cache = redis.Redis(host='redis', port=6379)

@app.route('/')

def hello():
    
    onlyfiles = glob.glob('D:\\dwh-coding-challenge\\DWH-Challenge\\data\\accounts\\*.json')

    list_data=[]

    for i in range(0,len(onlyfiles)):
        f = open(onlyfiles[i])
        data = json.load(f)
        list_data.append(data)

    list_data=pd.DataFrame(list_data)
    #SOLUTION NO 1, accounts
    print(tabulate(list_data))

    onlyfiles = glob.glob('D:\\dwh-coding-challenge\\DWH-Challenge\\data\\cards\\*.json')

    list_data=[]

    for i in range(0,len(onlyfiles)):
        f = open(onlyfiles[i])
        data = json.load(f)
        list_data.append(data)

    list_data=pd.DataFrame(list_data)
    #SOLUTION NO 1, cards
    print(tabulate(list_data))

    onlyfiles = glob.glob('D:\\dwh-coding-challenge\\DWH-Challenge\\data\\savings_accounts\\*.json')

    list_data=[]

    for i in range(0,len(onlyfiles)):
        f = open(onlyfiles[i])
        data = json.load(f)
        list_data.append(data)

    list_data=pd.DataFrame(list_data)
    #SOLUTION NO 1, savings_accounts
    print(tabulate(list_data))


    onlyfiles = glob.glob('D:\\dwh-coding-challenge\\DWH-Challenge\\data\\*\\*.json')


    list_data=[]

    for i in range(0,len(onlyfiles)):
        f = open(onlyfiles[i])
        data = json.load(f)
        data['table_name']  = os.path.basename(os.path.dirname(onlyfiles[i]))
        list_data.append(data)


    list_data=pd.DataFrame(list_data)
    list_data=list_data.sort_values('ts')
    #SOLUTION NO 1 but on loop and merger on 1 dataframe, Each table name on table_name or coloumn number 5
    print(tabulate(list_data))


    onlyfiles = glob.glob('D:\\dwh-coding-challenge\\DWH-Challenge\\data\\*\\*.json')


    list_data2=[]

    for i in range(0,len(onlyfiles)):
        f = open(onlyfiles[i])
        data = json.load(f)
        data['table_name']  = os.path.basename(os.path.dirname(onlyfiles[i]))
        list_data2.append(data)

    #Sorting List by Historical
    list_data2=pd.DataFrame(list_data)
    list_data2=list_data2.sort_values('ts')
    list_data2 = list_data2.values.tolist()

    def Convert(lst):
        res_dct = {lst[i]: lst[i + 1] for i in range(0, len(lst), 2)}
        return res_dct
        
    for i in range(0,len(list_data2)):
        if(i==0):
            if('u' in list_data2[i][1]):
                list_data2[i].append([])
                list_data2[i][6]=list_data2[i][5]
            else:
                list_data2[i].append([])
                list_data2[i][6]=list_data2[i][3]
        else:
            if('u' in list_data2[i][1]):
                list_data2[i].append([])
                list_data2[i][6]=list_data2[i-1][6] | list_data2[i][5]
            else:
                list_data2[i].append([])
                list_data2[i][6]=list_data2[i-1][6] | list_data2[i][3]

    #Solution No 2 with updated_result if the update and new data takes place
    list_data2=pd.DataFrame(list_data2,columns=['id','op','ts','data','table_name','set','updated_result'])

    print(tabulate(list_data2))

    # SOLUTION NO 3
    # Change timestamp to datetime for easier read


    list_data2['ts']=[time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(str(x)[:10]))) for x in list_data2['ts']]

    print(tabulate(list_data2))
    # Convenient Read

    #   From result from point no 2, discuss how many transactions has been made, when did each of them occur, and how much the value of each transaction?  
    #    Transaction is defined as activity which change the balance of the savings account or credit used of the card

        #how many transactions has been made
            #  There are 8 transactions with 4 of them are Balance Type and 4 are credit used, 
                #  But if we look at card type thats been used as transaction, c2 has 2 historical transaction and c1 has triple transaction amount
                    #c2 has 2 historical transaction and c1 has triple transaction amount
        #when did each of them occur
            #  C1 has 6 Transaction that happen at  1x balance transaction  at 2 January 2020, 1x credit used at 6 January 2020, 1x credit used at 8 January 2020, 3x (2x balance and 1x credit used transaction) at 10 January 2020
                #   C2 has 2 Transaction that happen at  1x credit used  at 18 January 2020, 1x balance transaction at 20 January 2020  
                    #   All of them happen at January 2020
        #how much the value of each transaction
            #  C1 has sum of 76000 value balance transaction and 38000 credit used value transaction
                #   C2 has sum of 33000 value balance transaction and 37000 credit used value transaction
                    #  With 2 of them combined has value 184000

        #Interesting Fact that, It's true that 'monthly_limit' of credit will increase as we using it
    return 'AKU'
    