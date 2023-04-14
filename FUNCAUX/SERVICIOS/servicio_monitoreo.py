#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa el servicio de guardar datos de monitoreo y performance.
'''
import os
import sys
import random
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.APIS.api_redis import ApiRedis
from FUNCAUX.UTILS.spc_utils import trace
from FUNCAUX.UTILS.spc_log import config_logger, set_debug_dlgid

# ------------------------------------------------------------------------------

class ServicioMonitoreo():
    '''
    ENTRADA: 
        D_INPUT =   { 'REQUEST':'SAVE_STATS', 
                      'DLGID':str,
                      'PARAMS: {'D_STATS': dict
                                'TIMESTAMP': str
                                'QNAME':str

                                }
                    }

    SALIDA: 
        D_OUTPUT =  { 'RESULT':bool, 
                      'DLGID':str,
                      'PARAMS': {}
                    }
    '''
    def __init__(self):
        self.d_input_service = {}
        self.d_output_service = {}
        self.cbk_request = None
        self.apiRedis_handle = ApiRedis()
        self.callback_functions =  { 'SAVE_STATS': self.__save_stats__, 
                                     'SAVE_FRAME_TIMESTAMP': self.__save_frame_timestamp__,
                                     'GET_QUEUE_LENGTH': self.__get_queue_length__,
                                     'DELETE_QUEUE': self.__delete_queue__,
                                    }

    def process(self, d_input:dict):
        '''
        Unica funcion publica que procesa los requests.
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.d_input_service = d_input
        # Chequeo parametros de entrada
        tag = random.randint(0,1000)
        trace(self.d_input_service, f'Input SERVICIO Monitoreo ({tag})')
        #
        self.cbk_request = self.d_input_service.get('REQUEST','')
        # Ejecuto la funcion de callback
        if self.cbk_request in self.callback_functions:
            self.callback_functions[self.cbk_request]()  
        #
        trace(self.d_output_service, f'Output SERVICIO Monitoreo ({tag})')
        return self.d_output_service

    def __save_stats__(self):
        '''
        Guarda los datos ( diccionario de stats ) en la REDIS en STATS_QUEUE.
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api
          
    def __save_frame_timestamp__(self):
        '''
        Guarda el timestamp y el tipo de frame ( CONFIG, DATA, PING) en la REDIS 'COMMS_STATUS'
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api

    def __get_queue_length__(self):
        '''
        Lee la profundidad de la cola pasada en los parametros.
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api

    def __delete_queue__(self):
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api


class TestServicioMonitoreo:

    def __init__(self):
        self.servicio_mon = ServicioMonitoreo()
        self.dlgid = ''

    def test_save_stats(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'SAVE_DATA_LINE','DLGID':'PABLO_TEST', 'PARAMS':{ 'PAYLOAD': {'Pa':1.23,'Pb':3.45}}}
        print('* SERVICE: SAVE_DATA Start...')  
        d_response = self.servicio_mon.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('OK')
        else:
            print('FAIL')
        print('* SERVICE: SAVE_DATA End.')  

    def test_save_frame_timestamp(self):

        self.dlgid = 'PABL0'
        set_debug_dlgid(self.dlgid)
        import datetime as dt
        now = dt.datetime.now()
        d_request = {'REQUEST':'SAVE_FRAME_TIMESTAMP', 'DLGID':self.dlgid, 'PARAMS': {'TIMESTAMP': now }}
        print('* SERVICE: SAVE_FRAME_TIMESTAMP Start...')  
        d_response = self.servicio_mon.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('OK')
        else:
            print('FAIL')
        print('* SERVICE: SAVE_FRAME_TIMESTAMP End.') 

    def test_get_queue_length(self):
        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'GET_QUEUE_LENGTH', 'DLGID':self.dlgid, 'PARAMS': {'QNAME':'STATS_QUEUE' }}
        print('* SERVICE:GET_QUEUE_LENGTH Start...')  
        d_response = self.servicio_mon.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            queue_length = d_response.get('PARAMS',{}).get('QUEUE_LENGTH', -1)
            print(f'OK {queue_length}')
        else:
            print('FAIL')
        print('* SERVICE: GET_QUEUE_LENGTH End.')  
 

if __name__ == '__main__':

    config_logger('CONSOLA')

    test = TestServicioMonitoreo()
    #test.test_save_stats()
    test.test_save_frame_timestamp()
    #test.test_get_queue_length()

