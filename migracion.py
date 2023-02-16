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
					  {  'order':3, 'name':'Actualiza', 'type':Decimal, 'default':True, 'required':True },
					  {  'order':4, 'name':'Debug', 'type':str, 'default':"N",   'required':False      },
				]
		},
            ]
cfg={'dbuser': 'app', 'dbname': 'migracionProgressa', 'dbpassword': 'Vostro1310', 'dbhost': 'localhost', 'dbport': '3306'}
cfg['program_name']='Migracion_serialized'

def task(requests):
    db=Connector(**cfg)
    result=db.get(requests)
    return [result.data]

def progress(n):
    n=n+1
    print(n, end='\r', flush=True)
    #return n




def main():

    print(datetime.now())
    inicio=datetime.now()

    POOL_SIZE=40
    NUM_THREADS=8

    #cfg=Utils.load_settings('pgss.cfg')
    #print(cfg)

    safi=Connector(**cfg)
    parameters=['2022-11-30',False,'N']
    creditos_con_error=Request.Generic('PGSSALDOSAMORTIPRO',parameters)
    results=[]
    data=safi.get(creditos_con_error)    
    print('Creditos por procesar:' + str(data.rowcount))    

    if(data):                       
            data_block=Utils.paginate(data.data,POOL_SIZE)
            n=0
            r=0
            item=0
            request_list=[]
            for row in data_block:
                item=item+1
                request_list=Request.GenericBulk('update',row,update_repo).map(CreditoID='CreditoID',FechaCorte='FechaCorte')
                print(len(request_list))
                with Pool(NUM_THREADS) as pool:
                    n= n + POOL_SIZE
                    results.append(pool.map(task, request_list,chunksize=5))
                    #pool.close() # for async
                    #pool.join() # for async 
            print('Registros:' + str(data.rowcount)) 
            print(f'Loops procesados: { item } ')
            print(f'items procesados: { r } ')
            print(' \n.')
            print(' Done.')
            print(datetime.now())
    fin=datetime.now()
    duracion=inicio-fin
    print('Duration: {}'.format(fin - inicio))


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
#2023-02-14 15:00:30.122035###
##
##
##

# 2023-02-14 16:45:12.332747