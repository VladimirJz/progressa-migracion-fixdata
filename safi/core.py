from .database import Repository
from distutils.log import debug
#from email import message
#from sqlite3 import DatabaseError
from sre_constants import SUCCESS
import mysql.connector
from mysql.connector import errorcode
#import sqlite3
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
    
    '''
    Gestiona la conexión con la base de datos  asi como la interacción con la misma
    '''
    REQUESTS_HEADER = {'Content-type': 'application/json'}
    #service=service_stats()
    
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
    def is_available(self):
        return self._is_available
    @is_available.setter
    def is_available(self,value):
        self._is_available=value

    # TODO: Renombrar el metodo get por un mas generico
    # la funcion ejecuta y lee el resultado.

    def get(self,request,format='raw'):
        '''
        Obtiene de la base de datos la petición 'Safi.Request' y la devuelve en  el formato requerido
        Por default devuelve un objeto cursor.
        '''
        def json_str(resulset):
            data_json=[]
            for row in resulset:
                json_str = json.dumps(row,cls=Generic.CustomJsonEncoder)
                data_json.append(json_str)
            
            return data_json

        def only_data(resultset):
            '''
            Devuelve el resultset en formato de lista sin encabezados.
            '''
            print(resultset)
            data_no_headers=[]
            for row in resultset:
                data_no_headers.append( [i for i in row.values()])
            return data_no_headers
            pass
        
        def fetch_raw(resultset):
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
        print(type(request))
        #params=request.parameters
        
        params=request.parameters

        audit=[ 1, 1, date.today(), '127.0.0.1', 'api.rest', 1, 1]
        #params.extend(audit)

        routine=request.routine
        print(routine)
        print(params)
        resultset=self._run(routine,params,request)
        print(type(resultset))
        if(request.output=='message'):
            resultset=Utils.upper_keys(resultset)
        
        print(resultset)
            
        #request.rowcount=(len(resultset))

        #print("AQUI")
                
        #print(type(resultset))
        if format=='json':
         #   print("JS")
            return json_str(resultset)
     
        if format=='onlydata':
          #  print("OD")
            return only_data(resultset)
            
        if format=='raw':
           #     print("RW")
                return fetch_raw(resultset) 
        raw_data=resultset
        return raw_data


        



   
    
    def is_connected(self):
        '''
        Devuelve el ultimo estatus de la conexión.
        '''
        if self.connect():
            return True
        else:
            return False
        
    
    def get_updates(self,type):
        #type=kwargs.pop('type')
        args=list()
        args.append(1)
        API_ENDPOINT='http://localhost:8000/bodesa/api/saldosdetalle/'
        last_id=self.service.get(LAST_TRASACCTION_ID)
        logger.info("Ultima transaccion: " + str(last_id))
        #cursor.execute('SELECT * from USUARIOS')
        db=self.connect()
        cursor=db.cursor(dictionary=True)
        cursor.execute("call PGS_MAESTROSALDOS('I','T',556,'S') ") 
        result=cursor.fetchall()
        for row in result:
            app_json = json.dumps(row,cls=Utils.CustomJsonEncoder)
            #print(app_json)

            r = requests.post(url = API_ENDPOINT, data = app_json,headers=self.REQUESTS_HEADER)
            #print(r.status_code)

    def _update_request(self,request,raw_data):
        request.rowcount=(len(raw_data))
        if request.rowcount>0:
            request.status_code=0
            request.status_message='Consulta realizada correctamente'
        else:
            request.status_code=1
            request.status_message='No Existen resultados'
   
        if "NumErr" in raw_data[0]:
            request.status_code=raw_data[0]['NumErr']
            request.status_message=raw_data[0]['ErrMen']


        
    def _run(self,routine,params,request=None):
        '''Devuelve un objeto Cursor'''
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
            with db.cursor() as cursor:  
                cursor.callproc(routine,params)
                if(cursor.rowcount>0):                    
                    raw_data = _todict(cursor)
                    if(request):
                        self._update_request(request,raw_data)
                    #for result in cursor.stored_results():
                        #print (result)
                #    pass
                else:
                    raw_data=dict()

            #db.commit()

            return raw_data

                #print(rows)
                #print('tupoCursor:',rows)
        except mysql.connector.Error as err:
            print(err)
            message="MySQL: On Execute ["+ routine + "] >" +str(err)    
            logger.error(message)
            return None
        else: 
            message='MySQL:[' + routine  + '] executed sucessfully.'
            logger.info(message)
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
                #print('try')

            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                    #message="MySQL: Authentication failed, wrong username or password"
                    raise Exception.DBAutenticationError(self.db_user )
                
                elif err.errno == errorcode.ER_BAD_DB_ERROR:
                    message="La base de datos no existe"
                    raise Exception.DataBaseError(self.db_name,message)
                
                else:
                    #message=err
                    #logger.error(message)
                    raise Exception.DataBaseError(self.db_name,err)
                    
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
                db_connection=mysql.connector.connect(**self.db_strcon)
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


    def connect(self):
        '''
        Devuelve un objeto de  conexión con la Base de datos
        '''
        db_connection=connections[connection_name]
        success_connection=True
        return db_connection #success_connection

    
class Connector(Session):
    def __init__(self,**kwargs):
        super().__init__()
        self.db_name=kwargs.pop('dbname')
        self.db_user=kwargs.pop('dbuser')
        self.db_pass=kwargs.pop('dbpassword')
        self.db_host=kwargs.pop('dbhost')
        self.db_port=kwargs.pop('dbport')
        self.db_strcon=self._set_strconx()
        #print('_init_'+ str(self._testConnection()))
        self._is_available=self._testConnection()

    def connect(self):
        try:
            db_connection=mysql.connector.connect(**self.db_strcon)
            message="MySQL: Database connection is open."
            success_connection=True

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                message="MySQL: Authentication failed, wrong username or password"
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                message="MySQL: The database [" +  self.db_name +  "] don't exists"
            else:
                message=err
            logger.error(message)
            return None
        else:
            #     db_connection.close()
                logger.info(message)
        return db_connection #success_connection
        

############################################################################
############################################################################
#---------------------------------------------------------------------------
# Request 
#---------------------------------------------------------------------------
############################################################################
############################################################################
          
class GenericRequest():
    '''
    Permite instanciar las peticiones a la BD como instancias de clase SAFI.Request en lugar de 
    usar directamente las rutinas de BD.
    '''
    def __init__(self,keyword):
        #print(repo)
        self._properties=''
        self._parameters=[]
        self._routine=''
        self._keyword=keyword
        self._rowcount=0
        self._status_code=None
        self._output=None
    
    def get_props(self,keyword, repository):
        '''
        Obtiene las propiedades de ejecucución del 'SAFI.Request' solicitado.
        '''
        return [element for element in repository if element['keyword'] == keyword]
    
    def add_bulk(self,**kwargs):
        class ParsedRequest():
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
                self.routine = value
            def __init__(self,outer,**kwargs):
                self._parameters=kwargs
                self._routine=outer.routine
                pass
            pass

        parsed=ParsedRequest(self,**kwargs)
        return ParsedRequest
    @classmethod    
    def new(cls,**add_args):
        print(cls)
        print(cls.routine)
        return  cls.__init__()


    def add(self,**kwargs):
        '''
        Agrega parametros al 'SAFI.Request' e inicializa con los valores default
        aquellos que no son proporcionados explicitamente.
        '''
        #print(self)
        raw_parameters=[]
        non_empty={k: v for k, v in kwargs.items() if v}
        print(self.properties[0])
        
        unpack=self.properties[0]

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

    def add_org(self,**kwargs):
        '''
        Agrega parametros al 'SAFI.Request' e inicializa con los valores default
        aquellos que no son proporcionados explicitamente.
        '''
        raw_parameters=[]
        #print(self.properties)
        unpack=self.properties[0]
        self._routine=unpack['routine']
        parameter_properties=unpack['parameters']
        for par in parameter_properties:
            #print(par['name'])
            value= kwargs.get(par['name'],par['default'])
            raw_parameters.append(value)
        self._parameters= raw_parameters
        #print('raw')
        #print (raw_parameters)
        return self

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
        return self._status_code
    @status_code.setter
    def status_code(self,value):
        self._status_code=value
    

    @property
    def status_message(self):
        return self._status_message
    @status_message.setter
    def status_message(self,value):
        self._status_message=value


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

    class Account(GenericRequest):
        def __init__(self,keyword):
            super().__init__(keyword)
            repository=Repository.Account
            self.properties=self.get_props(keyword,repository)

    class Integracion(GenericRequest):
        def __init__(self,keyword):
            super().__init__(keyword)
            repository=Repository.Integracion
            self.properties=self.get_props(keyword,repository)

    class Cartera(GenericRequest):
        def __init__(self,keyword):
            super().__init__(keyword)
            repository=Repository.Cartera
            self.properties=self.get_props(keyword,repository)
    class Catalogos(GenericRequest):
        def __init__(self,keyword):
            super().__init__(keyword)
            repository=Repository.Catalogos
            self.properties=self.get_props(keyword,repository)

    class Bulk(GenericRequest):
        
        @property
        def source(self):
            return self._source
        
        @source.setter
        def source(self,value):
            self._source = value
        
        
        def __init__(self, keyword,datasource):
            super().__init__(keyword)
            repository=Repository.Bulk
            self.properties=self.get_props(keyword,repository)
            print(self.properties)
            self._source=datasource

            
        def parse(self,**kwargs):
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
                request_item=self.__class__(self._request,self._source)
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

    class _BulkParsedRequest(GenericRequest):
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
            print (data)
            data = {k.upper():v for k,v in data.items()}
            print (data)
            dataset[0]=(data)
        print (data)
        return dataset

    

    
    def paginate(dataset,limit):
        '''
        Return a generator as a data subset by paginate a iterable object on set's of <limit> items using a lazy iterator.
        '''
        
        def _yield_row(dataset):                
           # i=0
            for row in dataset:
                yield row
            pass

        data=[]
        row_iterator=_yield_row(dataset)
        for item in row_iterator:
            data.append(item)
            if(len(data)>=limit):
                print("-"*10)
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


        print(settings)

        return settings
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
   
        def __init__(self, value=None, *args):
            super().__init__(args)
            self.value = value
            self.message=args[0]

        def __str__(self):
            return f" { self.message }:'{ self.value }'."