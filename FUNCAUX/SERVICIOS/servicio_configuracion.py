#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa los diferentes servicios de configuracion:
puede ser para leer la configuracion del equipo o para la configuracion del UID.
'''
import os
import sys
import pickle
import random

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.APIS.api_redis import ApiRedis
from FUNCAUX.APIS.api_sql import ApiBdSql
from FUNCAUX.UTILS.spc_utils import trace_request, trace_response, log2
from FUNCAUX.UTILS.spc_log import config_logger, set_debug_dlgid
from FUNCAUX.UTILS import spc_responses

# ------------------------------------------------------------------------------

class ServicioConfiguracion():
    '''
    Procesa los pedidos de configuracion de equipos.
    Siguiendo el modelo de la API, incorpora una nueva capa de software
    Recibe REQUESTS de capas superiores y les responde con RESPONSES.
    Envia REQUEST a las API y recibe RESPONSES.

    REQUEST->{modifica}->API_REQUEST
    API_RESPONSES->{modifica}->RESPONSES
    
    Los request son objetos Request.

    '''
    def __init__(self):
        self.params = {}
        self.endpoint = ''
        self.response = spc_responses.Response()
        self.apiRedis = ApiRedis()
        self.apiSql = ApiBdSql()
        self.callback_endpoints =  { 'READ_CONFIG': self.__read_config__, 
                                     'READ_DEBUG_DLGID': self.__read_debug_dlgid__,
                                     'READ_DLGID_FROM_UID': self.__read_dlgid_from_uid__, 
                                     'SAVE_DLGID_UID': self.__save_dlgid_uid__,
                                    }
        self.tag = random.randint(0,1000)

    def process(self, endpoint='', params={}):
        '''
        Unica funcion publica que procesa los requests.
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.endpoint = endpoint
        self.params = params
        #
        trace_request( endpoint=self.endpoint, params=self.params, msg=f'Input SERVICIO Configuracion ({self.tag})')
        # Ejecuto la funcion de callback
        if self.endpoint in self.callback_endpoints:
            # La response la fija la funcion de callback
            self.callback_endpoints[self.endpoint]()
        else:
            # ERROR: No existe el endpoint
            self.response.set_status_code(405)
            self.response.set_reason(f"SERVICIO Configuracion: No existe endpoint {endpoint}")
        #
        trace_response( response=self.response, msg=f'Output SERVICIO Configuracion ({self.tag})')
        return self.response
       
    def __read_config__(self):
        '''
        Leo la configuracion de la redis.
        Si no esta, pruebo con la sql y si esta actualizo la redis.
        Con los datos de entrada, prepara el dict() para la api.
        De la api Redis recibe un PKCONF pero debe devolver un D_CONF
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        #
        # Armo el request para la API 
        api_endpoint = 'READ_CONFIG'
        api_params = { 'DLGID':dlgid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        self.response.set_dlgid(dlgid)
        if api_response.status_code() == 200:
            # La configuracion estaba en la Redis: Respondo
            self.response.set_status_code( api_response.status_code())
            self.response.set_reason(api_response.reason())
            pkconf = api_response.json().get('PK_D_CONFIG',b'')
            d_conf = pickle.loads(pkconf)
            self.response.set_json( {'D_CONFIG':d_conf} )
            return

        # Me dio error o no estaba en Redis: Intento leer de SQL
        api_response = self.apiSql.process(params=api_params, endpoint=api_endpoint)
        if api_response.status_code() == 200:
            # La configuracion estaba en la Sql: Actualizo Redis y respondo
            # No chequeo errores de la actualizacion
            #
            d_conf = api_response.json().get('D_CONFIG',{})
            pkconfig = pickle.dumps(d_conf)
            api_endpoint = 'SAVE_CONFIG'
            api_params = { 'DLGID':dlgid, 'PK_D_CONFIG':pkconfig}  
            _ = self.apiRedis.process(params=api_params, endpoint=api_endpoint)                    
        #
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json() )

    def __read_debug_dlgid__(self):
        '''
        Pido a la API redis dlgid que se usa en el debug.
        '''
        # Armo el request para la API. No quiero loguearlos !!
        api_endpoint = 'READ_DEBUG_DLGID'
        api_params = {'LOG':False}
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json())

    def __read_dlgid_from_uid__(self):
        '''
        Esto ocurre en un procedimiento de RECOVER.
        Si los datos estan en la Redis, sigo.
        Si no pregunto a Sql.
        No actualizo las BD, solo consulto. Las actualizaciones se hacen cuando llegan 
        frames correctos (BASE) desde el protocolo con update_credenciales.
        '''
        # Estraigo del request los parametros
        uid = self.params.get('UID','0123456789')
        #
        # Armo el request para la API 
        api_endpoint = 'READ_DLGID_FROM_UID'
        api_params = { 'UID':uid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        if api_response.status_code() == 200:
            # La configuracion estaba en la Redis: Respondo
            self.response.set_status_code( api_response.status_code())
            self.response.set_reason(api_response.reason())
            self.response.set_json( api_response.json())
            return
        # No estaba en Redis: consulto a sql.
        api_response = self.apiSql.process(params=api_params, endpoint=api_endpoint)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json() )

    def __save_dlgid_uid__( self):
        '''
        Las credenciales son el par (dlgid,id) que estan en Redis y Sql
        La actualizacion se hace en ambas.
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        uid = self.params.get('UID','0123456789')
        #
        # Armo el request para la API 
        api_endpoint = 'SAVE_DLGID_UID'
        api_params = { 'DLGID':dlgid, 'UID':uid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        if api_response.status_code() == 200:
            api_response = self.apiSql.process(params=api_params, endpoint=api_endpoint)
        #
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json() )

class TestServicioConfiguracion:

    def __init__(self):
        self.servicio = ServicioConfiguracion()
        self.dlgid = ''
        
    def read_config(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        endpoint = 'READ_CONFIG'
        params = { 'DLGID':self.dlgid }
        print('* SERVICIO_CONFIGURACION: TEST_READ_CONFIG Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_CONFIGURACION: TEST_READ_CONFIG End...')  

    def read_debug_dlgid(self):
        endpoint = 'READ_DEBUG_DLGID'
        params = {}
        print('* SERVICIO_CONFIGURACION: TEST_READ_DEBUG_DLGID Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_CONFIGURACION: TEST_READ_DEBUG_DLGID End...') 

    def read_dlgid_from_uid(self):
        uid = '0123456789'
        endpoint = 'READ_DLGID_FROM_UID'
        params = { 'UID':self.uid }
        print('* SERVICIO_CONFIGURACION: TEST_READ_DLGID_FROM_UID Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_CONFIGURACION: TEST_READ_DLGID_FROM_UID End...')         

    def save_dlgid_uid(self):
        self.dlgid = 'PRUEBA2'
        uid = '0123456789'
        set_debug_dlgid(self.dlgid)
        endpoint = 'SAVE_DLGID_UID'
        params = { 'DLGID':self.dlgid, 'UID':uid }
        print('* SERVICIO_CONFIGURACION: TEST_SAVE_DLGID_UID Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_CONFIGURACION: TEST_SAVE_DLGID_UID End...') 

if __name__ == '__main__':
    
    config_logger('CONSOLA')

    test = TestServicioConfiguracion()
    #test.read_config()
    #test.read_debug_dlgid()
    #test.read_dlgid_from_uid()
    #test.save_dlgid_uid()
