import sys
from  safi.core import Connector,Request,Utils
#from  os import path
from datetime import datetime
import logging
from multiprocessing import Process,Pool
import asyncio
#from multiprocessing import Pool
settings=Utils.load_settings('pgss.cfg')    

def task(requests):
    db=Connector(**settings)
    result=db.get(requests)
    return [result.data]

def progress(results):
    print('*', end='', flush=True)




def main():
    POOL_SIZE=10
    settings=Utils.load_settings('pgss.cfg')    

    db=Connector(**settings)

    if db.is_available :
        vencimientos=Request.Cartera('vencimientos').add(FechaInicio='2022-10-10',FechaFin='2022-10-20',AtrasoInicial=1,AtrasoFinal=99999)
        results=[]
        data=db.get(vencimientos)
        #dataset=safi.Utils.to_csv(data,**kwargs)
        if(data):                       
            data_block=Utils.paginate(data.data,POOL_SIZE)
            n=0
            request_list=[]
            for row in data_block:
                request_list=Request.Bulk('pago-credito',row).map(CreditoID='CreditoID', MontoPagar='Pago',CuentaID='CuentaID')

                with Pool(4) as pool:
                    results.append(pool.map_async(task, request_list,chunksize=1,callback=progress))
                    pool.close()
                    pool.join()
            print(' Done.')
            for result in results:
                print(result.get())

if __name__ == '__main__':
    main()

                


                

        #print(data)
        #print('done..')
 
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     


#--------------------------------------------------------------------------






#9:20
