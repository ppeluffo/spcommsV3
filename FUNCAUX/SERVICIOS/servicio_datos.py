#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa el servicio de guardar/recuperar los datos en las BD
'''
import os
import sys
import random
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.APIS.api_redis import ApiRedis
from FUNCAUX.APIS.api_sql import ApiBdSql
from FUNCAUX.UTILS.spc_utils import trace
from FUNCAUX.UTILS.spc_log import config_logger, set_debug_dlgid

# ------------------------------------------------------------------------------

class ServicioDatos():
    '''
    Interface de datos con las BD
    La salida es la misma que da la API
    
    ENTRADA: 
        D_INPUT =   { SERVICIO:  { 'REQUEST':'READ_CONFIG', 
                                   'PARAMS: {'DLGID':str
                                     'D_CONF': dict
                                     'D_PAYLOAD':dict
                                     'UID':str
                                     }
                            }
                    }

    SALIDA: 
        D_OUTPUT =  { 'SERVICIO': { 'RESULT':bool, 
                                    'PARAMS': {'D_CONF':dict(), 
                                         'DEBUG_DLGID':str, 
                                         'ORDENES':str, 
                                         'DLGID':str }
                                        }
                            }
                    }
    '''
    def __init__(self):
        self.d_input_service = {}
        self.d_output_service = {}
        self.cbk_request = None
        self.apiRedis_handle = ApiRedis()
        self.callback_functions =  { 'SAVE_DATA_LINE': self.__save_data__, 
                                     'GET_ORDENES': self.__get_ordenes__,
                                     'DELETE_ENTRY': self.__delete_entry__,
                                    }

    def process(self, d_input:dict):
        '''
        Unica funcion publica que procesa los requests al SERVICIO .
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.d_input_service = d_input
        # Chequeo parametros de entrada
        tag = random.randint(0,1000)
        trace(self.d_input_service, f'Input SERVICIO Datos ({tag})')
        #
        self.cbk_request = self.d_input_service.get('REQUEST','')
        # Ejecuto la funcion de callback
        if self.cbk_request in self.callback_functions:
            self.callback_functions[self.cbk_request]()  
        #
        trace(self.d_output_service, f'Output SERVICIO Datos ({tag})')
        return self.d_output_service

    def __save_data__(self):
        '''
        Guarda los datos en la REDIS (PKLINE) y en la SQL.
        Esto ultimo se hace a travez de las colas de datos de REDIS !!!
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api
          
    def __get_ordenes__(self):
        '''
        Lee de REDIS si hay ordenes para enviar la equipo remoto en las respuestas.
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api

    def __delete_entry__(self):
        '''
        Borra la entrada correspondiente a dlgid de la bd REDIS
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api

class TestServicioDatos:

    def __init__(self):
        self.servicio_datos = ServicioDatos()
        self.dlgid = ''

    def test_save_data(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'SAVE_DATA_LINE','DLGID':'PABLO_TEST', 'PARAMS':{ 'PAYLOAD': {'Pa':1.23,'Pb':3.45}}}
        print('* SERVICE: SAVE_DATA Start...')  
        d_response = self.servicio_datos.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('OK')
        else:
            print('FAIL')
        print('* SERVICE: SAVE_DATA End.')  

    def test_get_ordenes(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'GET_ORDENES','DLGID':'PABLO_TEST'}
        print('* SERVICE: GET_ORDENES Start...')  
        d_response = self.servicio_datos.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('OK')
            orden = d_response.get('PARAMS',{}).get('ORDENES','ERR')
            print(f'ORDEN={orden}')
        else:
            print('FAIL')
        print('* SERVICE: GET_ORDENES End.')  

    def test_delete_entry(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'DELETE_ENTRY','DLGID':self.dlgid, 'PARAMS':{} }
        print('* SERVICE: DELETE_ENTRY Start...')  
        d_response = self.servicio_datos.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('OK')
        else:
            print('FAIL')
        print('* SERVICE: DELETE_ENTRY End.')  

if __name__ == '__main__':

    config_logger('CONSOLA')

    test = TestServicioDatos()
    #test.test_save_data()
    #test.test_get_ordenes()
    test.test_delete_entry()
