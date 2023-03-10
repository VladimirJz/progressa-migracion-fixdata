import sys
from  safi.core import Connector,Request,Utils
#from  os import path
from datetime import datetime
import logging
from multiprocessing import Process,Pool
import asyncio
settings=Utils.load_settings('pgss.cfg')    

async def task_coroutine(requests,safi_session):
    print(f'task is running')
    db=Connector(**settings)
    result=db.get(requests)
    print(result.data)
    return result


async def main_coroutine(request_list,safi_session):
    started_tasks = [asyncio.create_task(task_coroutine(request,safi_session)) for request in request_list]
    await asyncio.sleep(0.1)
    tasks = asyncio.all_tasks()
    for task in tasks:
        print(f'> {task.get_name()}, {task.get_coro()}')
    # wait for all tasks to complete
    for task in started_tasks:
        await task

def main():
    POOL_SIZE=20
    settings=Utils.load_settings('pgss.cfg')    

    db=Connector(**settings)

    if db.is_available :
        vencimientos=Request.Cartera('vencimientos').add(FechaInicio='2022-10-10',FechaFin='2022-10-20',AtrasoInicial=1,AtrasoFinal=99999)
        
        data=db.get(vencimientos)
        #dataset=safi.Utils.to_csv(data,**kwargs)
        if(data):                       
            data_block=Utils.paginate(data.data,POOL_SIZE)
            n=0
            request_list=[]
            for row in data_block:
                n+=1
                #print (row)
                #print('vuelta='+str(n))
                request_list=Request.Bulk('pago-credito',row).map(CreditoID='CreditoID', MontoPagar='Pago',CuentaID='CuentaID')
                #print((request_list))
                
               
               
                asyncio.run(main_coroutine(request_list,db))
               # for request in  request_list:
               #     print(request.routine)
                    #print(request.parameters)
               #     resultado=db.get(request,format='raw')[0]
                    
                    #pool = multiprocessing.pool.Pool()
                    
                    #print((resultado))

                    #if(resultado['NumErr']>0):
                


                

        #print(data)
        #print('done..')
 
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     
main()

#--------------------------------------------------------------------------






