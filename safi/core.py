from .database import Repository
from distutils.log import debug

from sre_constants import SUCCESS
import mysql.connector
from mysql.connector import errorcode
import json
import configparser

#import requests
from decimal import *
from datetime import datetime,date
from sys import exit
import csv
from os import path
import  ftplib
from django.db import connections
from re import sub



import logging

logger = logging.getLogger(f"main.{__name__}")
#logging.basicConfig(filename="log.txt", level=logging.DEBUG)




class Session():
    
    """ Gestiona la conexión con la Base de datos, proporcionado metodos de acceso simplificado.

    Raises:
        Exception.DBAutenticationError: Errores de autenticación de BD.
        Exception.DataBaseError: Errores relacionados con la operación de BD


    Returns:
        cls: safi.core.Session
    """    '''
    Gestiona la conexión con la base de datos  asi como la interacción con la misma
    '''
    REQUESTS_HEADER = {'Content-type': 'application/json'}

    def _error_handler(self,err,object):
        if err.errno == errorcode.ER_SP_DOES_NOT_EXIST:
            message="La rutina especificada en la instancia <Request> no existe"
            raise Exception.DataBaseError(object,message)
                      
        elif err.errno == errorcode.ER_SP_WRONG_NO_OF_ARGS:
            message="El número de parametros de la rutina especificada, no coincide"
            raise Exception.DataBaseError(object,message)
            
        elif err.errno == errorcode.ER_QUERY_INTERRUPTED:
            message="CRITICAL: La ejecución de la rutina fue cancelada"
            raise Exception.DataBaseError('MySQLConnection',message)
            
        
        elif err.errno==errorcode.CR_CONN_HOST_ERROR:
            message="CRITICAL: Imposible conectar con la Base de datos"
            raise Exception.DataBaseError(object,message)
        
        elif err.errno == errorcode.ER_ACCESS_DENIED_ERROR: 
            message="CRITICAL: Error de autenticación con la Base de datos"
            raise Exception.DataBaseError(object,message)
        
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            message="CRITICAL: La Base de datos no existe"
            raise Exception.DataBaseError(object,message)



        else:
            raise Exception.DataBaseError(self.db_uri,err)  
            

    @property
    def db_name(self):
        return self._db_name
    @db_name.setter
    def db_name(self,value):
        self._db_name=value

    @property
    def db_user(self):
        return self._db_user
    @db_user.setter
    def db_user(self,value):
        self._db_user=value
    
    @property
    def db_pass(self):
        return self._db_pass
    @db_pass.setter
    def db_pass(self,value):
        self._db_pass=value

    @property
    def db_host(self):
        return self._db_host
    @db_host.setter
    def db_host(self,value):
        self._db_host=value

    @property
    def db_port(self):
        return self._db_port
    @db_port.setter
    def db_port(self,value):
        self._db_port=value
    
    @property
    def db_strcon(self):
        return self._db_strcon
    @db_strcon.setter
    def db_strcon(self,value):
        self._db_strcon=value

    @property
    def db_uri(self):
        return self._db_uri
    @db_uri.setter
    def db_uri(self,value):
        self._db_uri=value

    @property
    def is_available(self):
        return self._is_available
    @is_available.setter
    def is_available(self,value):
        self._is_available=value

    # TODO: Renombrar el metodo get por un mas generico
    # la funcion ejecuta y lee el resultado.

    def get(self,request,output='raw'):
        """ Ejecuta una instacia <safi.core.Request>

        Args:
            request (cls): Instancia safi.core.Request
            output (str, optional): formato de salida. Defaults to 'raw'.

        Returns:
            dict/list: raw,json=dict{}  only_data= list[]
        """        '''
        Obtiene de la base de datos la petición 'Safi.Request' y la devuelve en  el formato requerido
        Por default devuelve un objeto cursor.
        '''
        def _format(resultset,output):
            if output=='json':
                return _json_str(resultset)
        
            if output=='onlydata':
                return _only_data(resultset)
                
            if output=='raw':
                    return _fetch_raw(resultset) 


        def _json_str(resulset):
            data_json=[]
            for row in resulset:
                json_str = json.dumps(row,cls=Generic.CustomJsonEncoder)
                data_json.append(json_str)
            
            return data_json

        def _only_data(resultset):
            '''
            Devuelve el resultset en formato de lista sin encabezados.
            '''
            print(resultset)
            data_no_headers=[]
            for row in resultset:
                data_no_headers.append( [i for i in row.values()])
            return data_no_headers
            pass
        
        def _fetch_raw(resultset):
            '''
            Devuelve el resultset en formato de lista sin encabezados.
            '''
            #print("raw");
            results=[]
            #print(type(resultset))
            for a in resultset:
             #   print(type(a))
             #   print(a)
                results.append(a)
            return results
        #print(type(request))
        #params=request.parameters
        

        params=request.parameters
        audit=[ 1, 1, date.today(), '127.0.0.1', 'api.rest', 1, 1]
        #params.extend(audit)
        routine=request.routine
        #print(routine)
        #print(params)

        resultset=self._run(routine,params,request)
               
        format_data=_format(resultset,output)
        #print(format_data)
        result=Output(format_data,request)    
      
        #raw_data=resultset
        return result


        



   
    
    def is_connected(self):
        '''
        Devuelve el ultimo estatus de la conexión.
        '''
        if self.connect():
            return True
        else:
            return False
    
        
    def _run(self,routine,params,request=None):
        '''Devuelve un objeto Cursor'''
        raw_data={}
        def _todict( cursor):
            "Return all rows from a cursor as a dict"

            columns = [col[0] for col in cursor.description]
            return [
                dict(zip(columns, row))
                for row in cursor.fetchall()]
        db=self.connect()
        if(not db):
            logger.error("MySQL: Lost connection whit ["+  self.db_name +  "] ")
            exit()
        #cursor=db.cursor()
        #print(routine)
        #print(params)
        try:
            #cursor.callproc(routine,params)
            #with db.cursor(dictionary=True) as cursor:
           
           with db.cursor(dictionary=True) as cursor:  
                cursor.callproc(routine,params)
                for results in cursor.stored_results():
                    #print (raw_data)
                    raw_data=results.fetchall()
                    
                    
           # ORIGINAL
            # with db.cursor() as cursor:  
            #     cursor.callproc(routine,params)    
            #     for result in cursor.stored_results():
            #         raw_data=result
            #         print(raw_data)

                
                #db.commit()
                db.close()

                #raw_data=results.fetchall()
                #print(rows)
                #print('tupoCursor:',rows)
        except mysql.connector.Error as err:
            # print('=' * 20) 
            # print(routine)
            # print(params)
            self._error_handler(err,routine)
            return None
        else: 
            pass
        return raw_data


        

    
  
        



   
    def _testConnection(self):
        success_connection=False
        if isinstance(self,Connector):
            # #print('init:' + str(success_connection))
            try:
                db_connection=mysql.connector.connect(**self.db_strcon)
                message="MySQL: The database is available"
                #print(message)
                success_connection= True
                db_connection.close()
                #print('try')

            except mysql.connector.Error as err:
                self._error_handler(err,self.db_uri)
                # if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                #     #message="MySQL: Authentication failed, wrong username or password"
                #     raise Exception.DBAutenticationError(self.db_user )
                
                # elif err.errno == errorcode.ER_BAD_DB_ERROR:
                #     message="La base de datos no existe"
                #     raise Exception.DataBaseError(self.db_name,message)
                
                # else:
                #     #message=err
                #     #logger.error(message)
                #     raise Exception.DataBaseError(self.db_name,err)
                    
        else:
            # The django manage the connection
            #logger.info(message)
            success_connection=True
            

        return success_connection



    def connect(self):
        '''
        Devuelve un objeto de  conexión con la Base de datos
        '''
        db_connection=None
        if isinstance(self,Connector):
            try:
                db_connection=mysql.connector.connect(**self.db_strcon,conn_attrs= {"_client_name": "SAFI.CORE.API","_source_host":"develop_test"} ,autocommit=False )#,pool_size=32
                #print(message)
                #print('try')

            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    raise Exception.DBAutenticationError(self.db_user )
                
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    message="La base de datos no existe"
                    raise Exception.DataBaseError(self.db_name,message)
                
                else:

                    raise Exception.DataBaseError(self.db_name,err)
        else:
            db_connection=connections[self.db_conname]

        return db_connection #success_connection

    def _set_strconx(self):
        str_cnx=dict( user=self.db_user,
                                    password=self.db_pass,
                                    host=self.db_host,
                                    database=self.db_name,
                                    port=self.db_port)
        #print (str_cnx)
        return str_cnx

# La clase Engine y Connector gestionan la conexión con la base de datos
# Engine : usa el pool de conexiones de django, por defila

class Engine(Session):
    
    # for django managed connection
    @property
    def db_conname(self):
        return self._db_conname
    @db_conname.setter
    def db_conname(self,value):
        self._db_conname=value

    def __init__(self,connection_name):
        '''

        '''
        super().__init__(connection_name)
        self._is_available=self._testConnection()
        self.__db_conname=connection_name


    # def connect(self):
    #     '''
    #     Devuelve un objeto de  conexión con la Base de datos
    #     '''
    #     db_connection=connections[self.__db_conname]
    #     success_connection=True
    #     return db_connection #success_connection

    
class Connector(Session):
    def __init__(self,**kwargs):
        super().__init__()
        self.db_name=kwargs.pop('dbname')
        self.db_user=kwargs.pop('dbuser')
        self.db_pass=kwargs.pop('dbpassword')
        self.db_host=kwargs.pop('dbhost')
        self.db_port=kwargs.pop('dbport')
        self._set_URI()
        #self.db_strcon=self._set_strconx()

        #print('_init_'+ str(self._testConnection()))
        self._is_available=self._testConnection()
    def _set_URI(self):
        self.db_uri=self.db_name + "//" + self.db_user + "@" + self.db_host +":"+ self.db_port
        self.db_strcon=dict( user=self.db_user,
                                    password=self.db_pass,
                                    host=self.db_host,
                                    database=self.db_name,
                                    port=self.db_port)

    # def connect(self):
    #     try:
    #         db_connection=mysql.connector.connect(**self.db_strcon)
    #         message="MySQL: Database connection is open."
    #         success_connection=True

    #     except mysql.connector.Error as err:
    #         if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    #             message="MySQL: Authentication failed, wrong username or password"
    #         elif err.errno == errorcode.ER_BAD_DB_ERROR:
    #             message="MySQL: The database [" +  self.db_name +  "] don't exists"
    #         else:
    #             message=err
    #         logger.error(message)
    #         return None
    #     else:
    #         #     db_connection.close()
    #             logger.info(message)
    #     return db_connection #success_connection
        

############################################################################
############################################################################
#---------------------------------------------------------------------------
# Request 
#---------------------------------------------------------------------------
############################################################################
############################################################################
          
class BaseRequest():
    '''
    Permite instanciar las peticiones a la BD como instancias de clase SAFI.Request en lugar de 
    usar directamente las rutinas de BD.
    '''
    def __init__(self,keyword,repository=None):
        #print(repo)
        self._properties=''
        self._parameters=[]
        self._routine=''
        self._keyword=keyword
        self._rowcount=0
        self._code=None
        self._output=None
        self._request=None
        #self_name=__class__.__name__
        self_name=self.__class__.__name__

        if  not isinstance(self,Request.Generic) and not isinstance(self,Request.GenericBulk):    
            if(repository==None ):
                repository=getattr(Repository, self_name) #The class name  must be equal to List  propertie
        if not isinstance(self,Request.Generic):
            self._properties=self.get_props(keyword,repository)

        self._repository=repository
    
    def get_props(self,keyword, repository):
        """ Obtiene los propiedades de la petición <keyword> solicitada y actualiza las propiedades
            de la instancia Request.

        Args:
            keyword (str): Nombre con el que se identifica la petición Request
            repository (str): Nombre de la SubClase Request donde se obtiene <keyword>

        Returns:
            dict: Retorna un diccionario con las propiedades de la solicitud <Keyword>
        """        '''
        Obtiene las propiedades de ejecucución del 'SAFI.Request' solicitado.
        '''
        #print(keyword)
        #print(repository)
        return [element for element in repository if element['keyword'] == keyword]
    
    # def add_bulk(self,**kwargs):
    #     class ParsedRequest():
    #         @property
    #         def parameters(self):
    #             return self._parameters
            
    #         @parameters.setter
    #         def parameters(self,value):
    #             self._parameters = value
                
    #         @property
    #         def routine(self):
    #             return self._routine
            
    #         @routine.setter
    #         def routine(self,value):
    #             self.routine = value
    #         def __init__(self,outer,**kwargs):
    #             self._parameters=kwargs
    #             self._routine=outer.routine
    #             pass
    #         pass

    #     parsed=ParsedRequest(self,**kwargs)
    #     return ParsedRequest
    # @classmethod    
    # def new(cls,**add_args):
    #     #print(cls)
    #     #print(cls.routine)
    #     return  cls.__init__()


    def add(self,**kwargs):
        """ 
        Agrega paramametros a la instancia request, para contolar el comportamiento de la misma, 
        sobre escribe los valores por default de cada parametro explicitamente asignado.
        

        Returns:
            safi.core.Request: Instancia Request lista para ejecutar.
        """
        raw_parameters=[]
        #print(kwargs)
        non_empty={k: v for k, v in kwargs.items() if v}
        #print(self.properties[0])
        # if len(self._properties)==0:
        #     unpack=self._properties
        # else:
        unpack=self._properties[0]

        self._routine=unpack['routine']
        self._output=unpack['output']
        parameter_properties=unpack['parameters']
        value=''
        for par in parameter_properties:
            #print(par['name'])
            
            #incoming_value=kwargs.get(par['name'])
            #print(incoming_value)
            #if(incoming_value):
            value= non_empty.get(par['name'],par['default'])
            raw_parameters.append(value)
            incoming_value=''
        self._parameters= raw_parameters
        #print('raw')
        #print (raw_parameters)
        return self

    @property
    def request(self):
        return self._request

    @request.setter
    def request(self,value):
        self._request = value
    
    @property
    def repository(self):
        return self._repository
    
    @repository.setter
    def repository(self,value):
        self._repository = value


    @property
    def keyword(self):
        return self._keyword
    
    @keyword.setter
    def keyword(self,value):
        self._keyword = value
    
    @property
    def properties(self):
        return self._properties
    
    @properties.setter
    def properties(self,value):
        self._properties = value
    
    @property
    def parameters(self):
        return self._parameters
    
    @parameters.setter
    def parameters(self,value):
        self._parameters = value
        
    @property
    def routine(self):
        return self._routine
    
    @routine.setter
    def routine(self,value):
        self._routine = value 
    
    @property
    def status_code(self):
        return self._code
    @status_code.setter
    def status_code(self,value):
        self._code=value
    

    @property
    def status_message(self):
        return self._message
    @status_message.setter
    def status_message(self,value):
        self._message=value


    @property
    def rowcount(self):
        return self._rowcount
    @rowcount.setter
    def rowcount(self,value):
        self._rowcount=value


    @property
    def output(self):
        return self._output
    @output.setter
    def output(self,value):
        self._output=value

############################################################################
############################################################################
#---------------------------------------------------------------------------
# Modulos 
#---------------------------------------------------------------------------
############################################################################
############################################################################
class Request():
    class Generic(BaseRequest):
        def __init__(self,routine,parameters=None):
            super().__init__(keyword=None)
            self._routine=routine
            self.parameters=parameters

    class Account(BaseRequest):
        def __init__(self,keyword,repository=None):
            super().__init__(keyword,repository)
        # def __init__(self,keyword):
        #     super().__init__(keyword)
        #     repository=Repository.Account
        #     self.properties=self.get_props(keyword,repository)

    class Integracion(BaseRequest):
        def __init__(self,keyword,repository=None):
            super().__init__(keyword,repository)
        # def __init__(self,keyword):
        #     super().__init__(keyword)
        #     self_name=__class__.__name__
        #     repository=getattr(Repository, self_name) #The class name  must be equal to List  propertie
        #     print(repository)
        #     self.properties=self.get_props(keyword,repository)

    class Cartera(BaseRequest):
        def __init__(self,keyword,repository=None):
            super().__init__(keyword,repository)
            # self_name=__class__.__name__
            # repository=getattr(Repository, self_name) #The class name  must be equal to List  propertie
            # print(repository)
            # self.properties=self.get_props(keyword,repository)
    
    class Catalogos(BaseRequest):
        def __init__(self,keyword,repository=None):
            super().__init__(keyword,repository)
        # def __init__(self,keyword,repository):
        #     super().__init__()
        #     repository=Repository.Catalogos
        #     self.properties=self.get_props(keyword,repository)

    
    class BaseBulk(BaseRequest):
        
        def __init__(self, keyword,datasource,repository=None):
            super().__init__(keyword,repository)
            #repository=Repository.Bulk
            #self.properties=self.get_props(keyword,repository)
            self._source=datasource
        
        @property
        def source(self):
            return self._source
        
        @source.setter
        def source(self,value):
            self._source = value
        
        

            
        def map(self,**kwargs):
            '''
            
            Mapea cada parametro <Key> de Kwargs con  el valor correspondiente del <value> (como key)
            dentro del origen de datos <dataset> por cada item del mismo, para generar una lista de 
            instancias Safi.Request  ejecutables.

            '''
            raw_parameters=[]
            #print(type(kwargs))
            #print(self.properties)
            #unpack=self.properties[0]
            #self._routine=unpack['routine']
            #parameter_properties=unpack['parameters']
            #print (self._source)
            list_request=[]
            #print(self._source) # lista de Lote
            for row in self._source:
                #print (row)
                #for par in kwargs:
                add_args={}
                for  key, value in kwargs.items():
                    #print(key,value)
                    key_value=row.get(value,-1)
                    if key_value==-1:
                        raise Exception ("No existe un elemento: <" + value + "> dentro de la colección, <" + key + "> no puede ser mapeado." );
                    #print('Key: ' + key +', value: ' + str(key_value))
                    add_args[key]=key_value                
                #print ("add_args", add_args)
                #request_item=self.add(**add_args)
                if isinstance(self,Request.GenericBulk) or isinstance(self,Request.GenericBulk):
                    request_item=self.__class__(self._keyword,self._source,self._repository)
                else:
                    request_item=self.__class__(self._keyword,self._source)
                r=request_item.add(**add_args)
                #print(type(r))
                #self._parameters=                
                #self._parameters=add_args
                #request_item=self.add_bulk(**add_args)
                #print(type(request_item))
                #print(type(request_item))
                #request_item.__dict__ = self.__dict__.copy() 
                #request_item._parameters=add_args;
                #print('objeto_instanciado:' + request_item.routine)
                list_request.append(request_item)
                #print ('items' + str(list_request.__len__()))
                #value= kwargs.get(par['name'],par['default'])
                #raw_parameters.append(value)
            #self._parameters= raw_parameters
            
            return (list_request)

    class Bulk(BaseBulk):
        def __init__(self, keyword, datasource):
            super().__init__(keyword, datasource)
        

    class GenericBulk(BaseBulk):
        def __init__(self, keyword, datasource,repository):
            super().__init__(keyword, datasource,repository)
        


    class _BulkParsedRequest(BaseRequest):
        def __init__(self,keyword):
            super().__init__(keyword)
            repository=Repository.Bulk
            self.properties=self.get_props(keyword,repository)
        

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

    def upper_keys(dataset):
        if isinstance(dataset, list):
            data=dataset[0]
            #print (data)
            data = {k.upper():v for k,v in data.items()}
            #print (data)
            dataset[0]=(data)
        #print (data)
        return dataset

    

    
    def paginate(dataset,limit):
        '''
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

class Output():
    def __init__(self,db_output,request):
        self._data=''
        if db_output:
            self._update(db_output,request)


    def _update(self,db_output,request): 
        if isinstance(db_output,list):
            self._rowcount=(len(db_output))
            self._columns=((db_output[0].keys()))

        self._data=db_output
        self._code=0
        
        if request.output=='message' and  self._rowcount==1 :
            db_output=Utils.upper_keys(db_output) # byPass: mismatch case  for header titles.        

        if self._rowcount>0:
            self._message='Consulta realizada correctamente'
        else:
            self._message='No Existen resultados.'
   
        if "NUMERR" in db_output[0]:
            self._code=db_output[0]['NUMERR']
            self._message=db_output[0]['ERRMEN']
    

    def to_json(self):
        data_json=[]
        data=self.data
        for row in data:
            json_str = json.dumps(row,cls=Generic.CustomJsonEncoder)
            data_json.append(json_str)
        
        return data_json

    def to_onlydata(self):
        '''
        Devuelve el resultset en formato de lista sin encabezados.
        '''
        data_no_headers=[]
        data=self.data
        for row in data:
            data_no_headers.append( [i for i in row.values()])
        return data_no_headers
        


    @property
    def status_code(self):
        return self._code
    @status_code.setter
    def status_code(self,value):
        self._code=value

    @property
    def status_message(self):
        return self._message
    @status_message.setter
    def status_message(self,value):
        self._message=value

    @property
    def rowcount(self):
        return self._rowcount
    @rowcount.setter
    def rowcount(self,value):
        self._rowcount=value
 
    @property
    def data(self):
        return self._data
    @data.setter
    def data(self,value):
        self._data=value
    
    @property
    def columns(self):
        return self._columns
    @columns.setter
    def columns(self,value):
        self._columns=value
    

            


    
############################################################################
############################################################################
#---------------------------------------------------------------------------
# Objetos genericos
#---------------------------------------------------------------------------
############################################################################
############################################################################

class Generic():
    
    class CustomJsonEncoder(json.JSONEncoder):
        def default(self, obj):
            # if passed in object is instance of Decimal
            # convert it to a string
            if isinstance(obj, Decimal):
                return str(obj)

            if isinstance(obj, datetime):
                return obj.isoformat()

            if isinstance(obj, date):
                return str(obj)
   
            #otherwise use the default behavior
            return json.JSONEncoder.default(self, obj)
    
    class File():
        @property
        def name(self):
            return self._name
        @name.setter
        def name(self,value):
            self._name=value

        @property
        def path(self):
            return self._path
        @path.setter
        def path(self,value):
            self._path=value
        
        @property
        def full_name(self):
            return self._full_name
        @full_name.setter
        def full_name(self,value):
            self._full_name=value

        def __init__(self,file_name,file_path):
            self.name=file_name
            self.path=file_path
            self.full_name=file_path + file_name


## Custom Exceptions 
class Exception(BaseException):
    class FileNotFoud(Exception):
   
        def __init__(self, value, *args):
            super().__init__(args)
            self.value = value
        def __str__(self):
            return f"The '{self.value}' file don't exists"

    class NoSectionError(Exception):
   
        def __init__(self, value=None, *args):
            super().__init__(args)
            self.value = value
        def __str__(self):
            return f"The '{self.value}' section, don't exists."

    class DBAutenticationError(Exception):
   
        def __init__(self, value=None, *args):
            super().__init__(args)
            self.value = value
        def __str__(self):
            return f"Autentication fail, wrong username '{self.value}' or password."
    
    class DataBaseError(Exception):
   
        def __init__(self, object=None, *args):
            super().__init__(args)
            self.object = object
            if(args):
                self.message=args[0]
            else:
                self.message='ERROR'
        def __str__(self):
            return f" { self.message }:'{ self.object }'."