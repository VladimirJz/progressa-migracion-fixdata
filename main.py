from safi.core import Utils,Connector,Request
from safi import repository 

def main():

    try:
        cfg=Utils.load_settings('pgss.cfg')
        safi=Connector(**cfg)
        saldos_globales=Request.Integracion(repository.SALDOS_DIARIOS).add()
        datos=safi.get(saldos_globales,output='onlydata')
        print(datos.data)


        #print(safi.is_available)
        #print(safi.is_connected())
    except Exception as e:
        print(e)

    print ('Continua')
main()