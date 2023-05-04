#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa el servicio de guardar datos de monitoreo y performance.
'''
import os
import sys
import pickle
import random
import datetime

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.APIS.api_redis import ApiRedis
from FUNCAUX.UTILS.spc_utils import trace_request, trace_response
from FUNCAUX.UTILS.spc_log import log2, config_logger, set_debug_dlgid
from FUNCAUX.UTILS import spc_responses

# ------------------------------------------------------------------------------

class ServicioMonitoreo():
    '''
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
        self.callback_endpoints = { 'SAVE_STATS': self.__save_stats__, 
                                     'SAVE_FRAME_TIMESTAMP': self.__save_frame_timestamp__,
                                     'READ_QUEUE_LENGTH': self.__read_queue_length__,
                                     'DELETE_QUEUE': self.__delete_queue__,
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
        trace_request( endpoint=self.endpoint, params=self.params, msg=f'Input SERVICIO Monitoreo ({self.tag})')
        # Ejecuto la funcion de callback
        if self.endpoint in self.callback_endpoints:
            # La response la fija la funcion de callback
            self.callback_endpoints[self.endpoint]()
        else:
            # ERROR: No existe el endpoint
            self.response.set_status_code(405)
            self.response.set_reason(f"SERVICIO Monitoreo: No existe endpoint {endpoint}")
        #
        trace_response( response=self.response, msg=f'Output SERVICIO Monitoreo ({self.tag})')
        return self.response

    def __save_stats__(self):
        '''
        Guarda un dict de stats en redis en modo serializado (PKSTATS)
        Los datos llegan en un dict que debemos serializar ya que las apis esperan 
        un pkstats.
        '''
        # Estraigo del request los parametros
        d_stats = self.params.get('D_STATS',{})
        dlgid = self.params.get('DLGID','00000')
        #
        pkstats = pickle.dumps(d_stats)
        # Armo el request para la API 
        api_endpoint = 'SAVE_STATS'
        api_params = { 'PK_D_STATS':pkstats, 'DLGID':dlgid }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json()) 
 
    def __save_frame_timestamp__(self):
        '''
        Guarda el timestamp del ultimo frame para usarlo para saber quienes estan 
        conectado y cuando hace que no lo estan.
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        timestamp = datetime.datetime.now()
        pktimestamp = pickle.dumps(timestamp)
        #
        # Armo el request para la API 
        api_endpoint = 'SAVE_FRAME_TIMESTAMP'
        api_params = { 'DLGID':dlgid, 'PK_TIMESTAMP':pktimestamp }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json()) 

    def __read_queue_length__(self):
        ''' Lee la profundida de una cola de Redis.
        '''
        # Estraigo del request los parametros
        dlgid = self.params.get('DLGID','00000')
        queue_name = self.params.get('QUEUE_NAME','queue')
        #
        # Armo el request para la API 
        api_endpoint = 'READ_QUEUE_LENGTH'
        api_params = { 'QUEUE_NAME':queue_name }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json()) 

    def __delete_queue__(self):
        ''' Solicita borrar una cola de Redis.
        '''
        # Estraigo del request los parametros
        queue_name = self.params.get('QUEUE_NAME','queue')
        dlgid = self.params.get('DLGID','00000')
        #
        # Armo el request para la API 
        api_endpoint = 'DELETE_ENTRY'
        api_params = { 'DLGID':dlgid, 'DKEY':queue_name }  
        api_response = self.apiRedis.process(params=api_params, endpoint=api_endpoint)
        #
        # Con la response de la API preparo la response
        self.response.set_dlgid(dlgid)
        self.response.set_status_code( api_response.status_code())
        self.response.set_reason(api_response.reason())
        self.response.set_json( api_response.json()) 

class TestServicioMonitoreo:

    def __init__(self):
        self.servicio = ServicioMonitoreo()
        self.dlgid = ''

    def save_stats(self):
        d_statistics = {'time_start':0.0,
                'time_end':0.0,
                'count_frame_ping':10,
                'count_frame_config_base':20,
                'count_frame_data':30,
                'count_accesos_SQL':40,
                'count_accesos_REDIS':50,
                'duracion_frame':0.1,
                'length_stats_queue':100,
            }
        #
        endpoint = 'SAVE_STATS'
        params = { 'DSTATS': d_statistics }
        print('* SERVICIO_MONOTOREO: TEST_SAVE_STATS Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_MONOTOREO: TEST_SAVE_STATS End...')

    def save_frame_timestamp(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        endpoint = 'SAVE_FRAME_TIMESTAMP'
        params = { 'DLGID':self.dlgid }
        print('* SERVICIO_MONOTOREO: TEST_SAVE_FRAME_TIMESTAMP Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_MONOTOREO: TEST_SAVE_FRAME_TIMESTAMP End...')

    def read_queue_length(self):
        endpoint = 'READ_QUEUE_LENGTH'
        params = { 'QUEUE_NAME': 'STATS_QUEUE' }
        print('* SERVICIO_MONOTOREO: TEST_READ_QUEUE_LENGTH Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_MONOTOREO: TEST_READ_QUEUE_LENGTH End...')

    def delete_queue(self):
        endpoint = 'DELETE_QUEUE'
        params = { 'QUEUE_NAME': 'STATS_QUEUE' }
        print('* SERVICIO_MONOTOREO: TEST_DELETE_QUEUE Start...')  
        response = self.servicio.process(params=params, endpoint=endpoint)
        print(f'STATUS_CODE={response.status_code()}')
        print(f'REASON={response.reason()}')
        print(f'JSON={response.json()}')
        print('* SERVICIO_MONOTOREO: TEST_DELETE_QUEUE End...')
 
if __name__ == '__main__':

    config_logger('CONSOLA')

    test = TestServicioMonitoreo()
    #test.save_stats()
    #test.save_frame_timestamp()
    #test.read_queue_length()
    #test.delete_queue()


