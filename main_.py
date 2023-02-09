from safi.core import Utils,Connector,Request

def main():

        cfg=Utils.load_settings('pgss.cfg')
         if db.is_available :
        saldos_globales=Request.Integracion('saldos_diarios').add()
        print(type(cfg))
        #safi=Connector(**cfg)
        #print(safi.is_available)
        #print(safi.is_connected())


main()