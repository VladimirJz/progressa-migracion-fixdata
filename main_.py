from safi.core import Utils,Connector,Request
from safi import repository 
def main():

            cfg=Utils.load_settings('pgss.cfg')
            safi=Connector(**cfg)
            saldos_globales=Request.Integracion(repository.SALDOS_DIARIOS).add()
            print(type(cfg))
            datos=safi.get(saldos_globales,output='onlydata')
            print(datos.data)
        #safi=Connector(**cfg)
        #print(safi.is_available)
        #print(safi.is_connected())


main()