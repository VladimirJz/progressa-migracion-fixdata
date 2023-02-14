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
        
        cfg={'dbuser': 'root', 'dbname': 'migracionProgressa', 'dbpassword': 'Vostro1310', 'dbhost': 'localhost', 'dbport': '3308'}
        safi=Connector(**cfg)
        vencimientos=Request.Cartera('vencimientos').add(FechaInicio='2022-09-01',FechaFin='2022-09-01')
        resultados=safi.get(vencimientos)
        print(resultados)

        #creditos=Request.Cartera('update',update).add()




       
main()