from safi.core import Utils,Connector,Request
from safi import repository 
from decimal import Decimal
from datetime import date

class Migracion():
    update=[
      {'routine':'PGSSALDOSAMORTICRE',
            'keyword':'update',
            'output':'table',
            'parameters':[{  'order':1, 'name':'CreditoID', 'type': int, 'default':0, 'required':True },
                        {  'order':2, 'name':'FechaCorte', 'type':int, 'default':0, 'required':True },
                        {  'order': 3, 'name':'Actualiza', 'type':Decimal, 'default':0, 'required':True },
                        {  'order': 4, 'name':'Debug', 'type':int, 'default':1,   'required':False },
                    ]
            },
            ]



def main():
	POOL_SIZE=20

	cfg=Utils.load_settings('pgss.cfg')
	print(cfg)

	cfg={'dbuser': 'root', 'dbname': 'migracionProgressa', 'dbpassword': 'Vostro1310', 'dbhost': 'localhost', 'dbport': '3306'}
	safi=Connector(**cfg)
	print(safi.is_connected())
	parameters=['2022-11-30',False,'N']
	creditos_con_error=Request.Generic('PGSSALDOSAMORTIPRO',parameters)
	resultados=safi.get(creditos_con_error)        
	print(resultados.rowcount)
	if(resultados):                       
		data_block=Utils.paginate(resultados.data,POOL_SIZE)
		n=0
		request_list=[]
	for row in data_block:
		n+=1
		request_list=Request.Bulk('pago-credito',row).map(CreditoID='CreditoID')
		print((request_list))
		for request in  request_list:
					print(request.routine + '>' +  str(request.parameters))
					print(request.parameters)

main()