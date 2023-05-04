#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa el servicio de guardar/recuperar los datos en las BD
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
from FUNCAUX.UTILS.spc_utils import trace_request, trace_response
from FUNCAUX.UTILS.spc_log import log2, config_logger, set_debug_dlgid
from FUNCAUX.UTILS import spc_responses

# ------------------------------------------------------------------------------

class ServicioDatos():
    '''
    Interface de datos. 
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
        self.callback_endpoints =  { 'SAVE_DATA_LINE': self.__save_data_line__,
                                    'READ_ORDENES': self.__read_ordenes__,
                                    'DELETE_ENTRY': self.__delete_entry__,
                                    'READ_VALUE': self.__read_value__,
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
        trace_request( endpoint=self.endpoint, params=self.params, msg=f'Input SERVICIO Datos ({self.tag})')
        # Ejecuto la funcion de callback
        if self.endpoint in self.callback_endpoints:
            # La response la fija la funcion de callback
            self.callback_endpoints[self.endpoint]()
        else:
            # ERROR: No existe el endpoint
            self.response.set_status_code(405)
            self.response.set_reason(f"SERVICIO Datos: No existe endpoint {endpoint}")
        #
        trace_response( response=self.response, msg=f'Output SERVICIO Datos ({self.tag})')
        return self.response

    def __save_data_line__(self):
        '''
        Guarda los datos en la REDIS (PKLINE) y en la SQL.
        Esto ultimo se hace a travez de las colas de datos de REDIS !!!
        Los datos llegan en un dict que debemos serializar ya que las apis esperan 
        un pkline.
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        d_line = self.params.get('D_LINE',{})
        pkline = pickle.dumps(d_line)
        #
        # Armo el request para la API 
        api_endpoint = 'SAVE_DATA_LINE'
        api_params = { 'DLGID':dlgid, 'PK_D_LINE':pkline }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json()) 
        
    def __read_ordenes__(self):
        '''
        Pido a la API redis las ordenes para enviar en la respuesta al dlgid.
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        #
        # Armo el request para la API 
        api_endpoint = 'READ_ORDENES'
        api_params = { 'DLGID':dlgid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json())

    def __delete_entry__(self):
        '''
        Mando borrar una entrada ( dlgid key) por medio de la API redis
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        #
        # Armo el request para la API 
        api_endpoint = 'DELETE_ENTRY'
        api_params = { 'DLGID':dlgid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json())

    def __read_value__(self):
        '''
        Lee de la redis del dlgid el valor de la variable con nombre var_name
        Leo la PKLINE, y de este saco el valor de la variable.
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        var_name = self.params.get('VAR_NAME','')
        #
        # Armo el request para la API 
        api_endpoint = 'READ_PKLINE'
        api_params = { 'DLGID':dlgid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        #
        if api_response.status_code() == 200:
            # Con la response de la API preparo la response
            pk_line = api_response.json().get('PK_LINE',b'')
            d_values = pickle.loads(pk_line)
            value = d_values.get(var_name, None)
            self.response.set_json({'VALUE':value} )
        else:
            self.response.set_json( api_response.json())

class TestServicioDatos:

    def __init__(self):
        self.servicio = ServicioDatos()
        self.dlgid = ''

    def save_data_line(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        d_line =  { 'PA':1.23,'PB':4.56, 'bt':12.65 }
        endpoint = 'SAVE_DATA_LINE'
        params = { 'DLGID':self.dlgid, 'DLINE':d_line }
        print('* SERVICIO_DATOS: TEST_SAVE_DATA Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_DATOS: TEST_SAVE_DATA End...') 

    def read_ordenes(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        endpoint = 'READ_ORDENES'
        params = { 'DLGID':self.dlgid }
        print('* SERVICIO_DATOS: TEST_READ_ORDENES Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_DATOS: TEST_READ_ORDENES End...') 
 
    def delete_entry(self): 
        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        endpoint = 'DELETE_ENTRY'
        params = { 'DLGID':self.dlgid }
        print('* SERVICIO_DATOS: TEST_DELETE_ENTRY Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_DATOS: TEST_DELETE_ENTRY End...')  

if __name__ == '__main__':

    config_logger('CONSOLA')
 
    # Test api_redis
    print('TESTING SERVICIO DATOS...')
    test = TestServicioDatos()
    #test.save_data_line()
    #test.read_ordenes()
    test.delete_entry()
    sys.exit(0)
