from safi.core import Utils,Connector,Request

def main():

    try:
        cfg=Utils.load_settings('pgss.cfg')
        safi=Connector(**cfg)
        saldos_globales=Request.Integracion('saldos_diarios').add()

        #print(safi.is_available)
        #print(safi.is_connected())
    except Exception as e:
        print(e)

    print ('Continua')
main()