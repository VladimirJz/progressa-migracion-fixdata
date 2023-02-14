import sys
from  safi.core import Connector,Request,Utils
#from  os import path
from datetime import datetime
from decimal import Decimal

import logging
from multiprocessing import Process,Pool
import asyncio
import time
#from multiprocessing import Pool
#settings=Utils.load_settings('pgss.cfg')    


update_repo=[
	{'routine':'PGSSALDOSAMORTICRE',
		'keyword':'update',
		'output':'table',
		'parameters':[{  'order':1, 'name':'CreditoID', 'type': int, 'default':0, 'required':True     },
					  {  'order':2, 'name':'FechaCorte', 'type':datetime, 'default':"1900-01-01", 'required':True     },
					  {  'order': 3, 'name':'Actualiza', 'type':Decimal, 'default':True, 'required':True },
					  {  'order': 4, 'name':'Debug', 'type':str, 'default':"N",   'required':False      },
				]
		},
            ]
cfg={'dbuser': 'app', 'dbname': 'migracionProgressa', 'dbpassword': 'Vostro1310', 'dbhost': 'localhost', 'dbport': '3306'}

def task(requests):
    db=Connector(**cfg)
    time.sleep(1)
    result=db.get(requests)
    return [result.data]

def progress(results):
    print('*', end='', flush=True)




def main():

    print(datetime.now())
    POOL_SIZE=4

    #cfg=Utils.load_settings('pgss.cfg')
    #print(cfg)

    safi=Connector(**cfg)
    print(safi.is_connected())
    parameters=['2022-11-30',False,'N']
    creditos_con_error=Request.Generic('PGSSALDOSAMORTIPRO',parameters)
    results=[]
    data=safi.get(creditos_con_error)        

    if(data):                       
            data_block=Utils.paginate(data.data,POOL_SIZE)
            n=0
            request_list=[]
            for row in data_block:
                request_list=Request.GenericBulk('update',row,update_repo).map(CreditoID='CreditoID',FechaCorte='FechaCorte')

                with Pool(2) as pool:
                    results.append(pool.map(task, request_list,chunksize=1))
                    pool.close()
                    pool.join()
            print(' Done.')
            print(datetime.now())

            for result in results:
                print(result)

if __name__ == '__main__':
    main()

                


                

        #print(data)
        #print('done..')
 
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     


#--------------------------------------------------------------------------






#9:20


#14:18
#14:34

# primer ronda
#2023-02-14 14:44:17.106308
#2023-02-14 15:00:30.122035