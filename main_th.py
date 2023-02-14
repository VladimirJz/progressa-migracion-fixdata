import sys
from  safi.core import Connector,Request,Utils
#from  os import path
from datetime import datetime
import logging
from multiprocessing import Process,Pool
import asyncio
from multiprocessing import Pool
settings=Utils.load_settings('pgss.cfg')    

def task(requests):
    print(f'task is running')
    db=Connector(**settings)
    result=db.get(requests)
    print(result.data)
    return result






def main():
    POOL_SIZE=4
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
                request_list=Request.Bulk('pago-credito',row).map(CreditoID='CreditoID', MontoPagar='Pago',CuentaID='CuentaID')

                with Pool(4) as pool:
                    r = set(pool.map(task, request_list))

if __name__ == '__main__':
    main()

                


                

        #print(data)
        #print('done..')
 
#-------------------------------------------------------------------------
#  RUN !
#-------------------------------------------------------------------------
     


#--------------------------------------------------------------------------






