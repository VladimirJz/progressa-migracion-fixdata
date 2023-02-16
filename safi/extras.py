from datetime import datetime
from distutils.log import debug
import configparser
import  ftplib
from os import path
import csv

class Alias():
    # Interacion
    SALDOS_GLOBALES='saldos_diarios'
    CLIENTE_DETALLE='cliente_detalle'
    ALTA_LINEACREDITO='linea_credito'
    ALTA_AVALES='alta-aval'
    ASIGNA_AVALES='asigna-aval'
    
    # Cartera
    VENCIMIENTOS_CARTERA='vencimientos'
    '''
    Reporte de Vencimientos de cartera
    :FechaInicio
    :FechaVencimiento
    '''
    PAGO_CREDITO='pago-credito'
    '''
    Pago de Crédito con cargo a cuenta
    :CreditoID
    :MontoPagar
    :CuentaID
    '''

    CUENTA_DEPOSITO='deposito'
    '''
    Deposito a cuenta
    :CuentaAhoID
    :CantidadMov
    :Fecha
    :FechaAplicacion
    '''
    #Catalogos
    CLIENTE_TIPO_RELACIONES='tipo-relaciones'
    CREDITO_DESTINOS='destinos-credito'
    CREDITO_PRODUCTOS='productos-credito'
    CREDITO_PROMOTORES='promotores-credito'
    SUCURSALES='sucursales'



    pass



############################################################################
############################################################################
#---------------------------------------------------------------------------
# Utilerias
#---------------------------------------------------------------------------
############################################################################
############################################################################

class Utils:
    
    def camel_case(s):
        #s = sub(r"(_|-)+", " ", s).title().replace(" ", "")
        return ''.join([s[0].lower(), s[1:]])

    def _upper_keys(dataset):
        if isinstance(dataset, list):
            data=dataset[0]
            #print (data)
            data = {k.upper():v for k,v in data.items()}
            #print (data)
            dataset[0]=(data)
        #print (data)
        return dataset

    

    
    def paginate(dataset,limit):
        """ Recibe un bloque de datos extensos y regresa un <Generador> (lazy iterator), iterable por bloques
            de <limit> elementos.

        Args:
            dataset (list): Bloque de datos
            limit (int): Número de items que devuelve cada iteración.

        Yields:
            list: Lista iterable con <limit> items.
        """        '''
        Return a generator as a data subset by paginate a iterable object on set's of <limit> items using a lazy iterator.
        '''
        all_items=len(dataset)
        items=0
        def _yield_row(dataset):                
           # i=0
            for row in dataset:
                yield row
            pass

        data=[]
        row_iterator=_yield_row(dataset)
        for item in row_iterator:
            data.append(item)
            items=items +1 
            if(len(data)>=limit):
                #print(" = "*10)
                row_list=data
                data=[]
                yield row_list
            elif items == all_items:
                #print('ultima:')
                #print(len(row_list))
                #print(f'{all_items} todos')
                row_list=data
                data=[]
                yield row_list

    def post(data,**kwargs):
        REQUESTS_HEADER = {'Content-type': 'application/json'}
        api_endpoint=kwargs.pop('apiupdateendpoint')
        message='API: POST:' + api_endpoint
        logger.info(api_endpoint)
        print(api_endpoint)
        for row in data:
            print (row)
            print(type(row))
            r = requests.post(url = api_endpoint, data = row,headers=REQUESTS_HEADER)
            print (r)
        pass
                
    def to_csv(data,**kwargs):
        '''
        file_extension=
        field_separator=
        file_name=
        '''
        current_date=datetime.now().strftime("%Y-%m-%d")
        file_extension=kwargs.pop('fileformat')
        field_separator=kwargs.pop('fieldseparator')
        file_name=kwargs.pop('filename') + '_'+ current_date + '.' +file_extension
        file_dir=kwargs.pop('directory') + '/'
        full_filename=file_dir + file_name 
        #file_dir=kwargs.pop('directory')
        print(file_dir)
        file=Generic.File(file_name,file_dir)
        

        with open(full_filename, 'w') as f:  
            writer = csv.writer(f, delimiter =field_separator)          
            writer.writerows(data)
        if not path.exists(full_filename):
            message="IO/OS: the file don't was generate."
            logger.error(message)
            return False
        else:
            message="IO/OS: Bulk data on ["+full_filename +"] file sucessfully."
            logger.info(message)

            
        return file


    def get_filename(**kwargs):
        file_name=kwargs.pop('filename')
        file_extension=kwargs.pop('fileformat')
        file_dir=kwargs.pop('directory')
     
        full_filename=file_dir + '/' + file_name + '' + file_extension
        #file_separator=kwargs.pop('fieldseparator')
        pass
        
    def ftp_upload(file,**kwargs):
        ftp_user=kwargs.pop('ftpuser')
        ftp_pass=kwargs.pop('ftppassword')
        ftp_port=kwargs.pop('ftpport')
        ftp_dir=kwargs.pop('ftpremotedir')
        ftp_host=kwargs.pop('ftphost')
        
        print(ftp_host + ftp_user + ftp_pass)
        try:
            ftp = ftplib.FTP(ftp_host, ftp_user, ftp_pass)
            print(ftp_dir)
            ftp.cwd(ftp_dir)
        except ftplib.all_errors as e:
            message='FTP:' + str(e) + ''
            logger.error(message)
            return False

        else:
            message='FTP:Open conection whit server'
            logger.info(message)
        ftp.encoding = "utf-8"
        ftp_message=''
        print ('full' + file.full_name)
        print(file.name)

        print(file.path)        
        try:
            with open(file.full_name, "rb") as f:
                # use FTP's STOR command to upload the file
                print(ftp.cwd(ftp_dir))
                print(ftp.nlst())
                print (file)
                message= 'FTP:' +  ftp.storbinary(f"STOR {file.name}", f)
                #f.storbinary('STOR ' + file.name,f)
                print((message))
                logger.info(message)

        except ftplib.all_errors as e:
            message='FTP:' + str(e) + ''
            logger.error(message)
            return False
        else:
            message='FTP: File upload successfully'
            logger.info(message)
            return True
        pass

    def load_settings(config_file=None,section=None):
        '''
        Load a custom .cfg file to import <seccion> on dict ,
        whitout arguments look for <safi.cfg>  by default  on current dir
        and <DATABASE> section. 
        '''
        if(config_file is None):
            config_file='safi.cfg'
        config = configparser.ConfigParser()
        current_dir = path.dirname(path.realpath(__file__))
        parent_dir=path.dirname(current_dir)
        if(not path.exists(config_file)):               
            raise  Exception.FileNotFoud(config_file)
            sys.exit()

        config.read(config_file)
        if(section is None):
            section='DATABASE'
            try:
                settings=dict(config.items('DATABASE'))
            except:
                raise  Exception.NoSectionError(section)
                #sys.exit()
        else:
            try:
                settings=dict(config.items(section))
            except :
                raise Exception.NoSectionError(section)
            #sys.exit()


        #print(settings)

        return settings