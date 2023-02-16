import sys
from safi.core import Connector,Request
from safi.extras import Utils,Alias as k 
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
cfg={'dbuser': 'app', 'dbname': 'microfin', 'dbpassword': 'Vostro1310', 'dbhost': 'localhost', 'dbport': '3306'}
cfg['program_name']='simulador_DEPOSITOS'
def task(requests):
    db=Connector(**cfg)
    result=db.get(requests)
    return [result.data]

def progress(n):
    n=n+1
    print(n, end='\r', flush=True)
    #return n




def main():

    print(f'Hora de inicio:{datetime.now()}')
    inicio=datetime.now()
    POOL_SIZE=40
    NUM_THREADS=8
    #cfg=Utils.load_settings('pgss.cfg')
    #print(cfg)

    safi=Connector(**cfg)
    parameters=['2022-11-30',False,'N']
    vencimientos=Request.Cartera(k.VENCIMIENTOS_CARTERA)
    vencimientos.add(FechaInicio='2022-10-10',
                    FechaFin='2022-10-20',
                    AtrasoInicial=1,
                    AtrasoFinal=99999)

    results=[]
    data=safi.get(vencimientos)    
    
    filas=len(data.data)
    print('Creditos por Procesar:' + str(len(data.data)))   
    #print (data.data)
    if(data):                       
            block_generator=Utils.paginate(data.data,POOL_SIZE)
            n=0
            r=0
            item=0
            request_list=[]
            for data_block in block_generator: 
                item=item+1 
                for datarow in data_block :
                    r=r+1
                request_list=Request.Bulk(k.CUENTA_DEPOSITO ,data_block).map(CuentaAhoID='CuentaID', 
                                                                    CantidadMov='Pago',
                                                                    Fecha='FechaEmision',
                                                                    FechaAplicacion='FechaEmision')


                #print (request_list)
                #exit()
                with Pool(NUM_THREADS) as pool:
                    n= n + len(request_list)
                    results.append(pool.map_async(task, request_list,chunksize=8, callback=progress(n)))
                    pool.close() # for async
                    pool.join() # for async 
            
            print('Registros:' + str(data.rowcount)) 
            print(f'Loops procesados: { item } ')
            print(f'items procesados: { r } ')
            print(' \n.')
    
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