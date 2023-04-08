#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa el servicio de guardar/recuperar los datos en las BD
'''
import os
import sys
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.APIS.api_redis import ApiRedis
from FUNCAUX.APIS.api_sql import ApiBdSql
from FUNCAUX.UTILS.spc_utils import trace, check_particular_params, check_inputs

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
        self.d_input = {}
        self.d_input_service = {}
        self.d_output_service = {'SERVICIO': { 'RESULT':False, 'PARAMS':{'MODULE':'SERVICIO_DATOS'}} }
        self.cbk_request = None
        self.apiRedis_handle = ApiRedis()
        self.callback_functions =  { 'SAVE_DATA': self.__save_data__, 
                                     'GET_ORDENES': self.__get_ordenes__,
                                     'DELETE_ENTRY': self.__delete_entry__,
                                    }

    def process(self, d_input:dict):
        '''
        Unica funcion publica que procesa los requests al SERVICIO .
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.d_input = d_input
        trace(self.d_input, 'Input SERVICIO')
        # Chequeo parametros de entrada
        res, str_error = check_inputs(d_input, 'SERVICIO' )
        if res:
            self.d_input_service = d_input['SERVICIO']
            self.cbk_request = self.d_input_service.get('REQUEST','')
            # Ejecuto la funcion de callback ( el request )
            self.callback_functions[self.cbk_request]()      
        else:
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['SERVICIO']['PARAMS']['ERROR'] =  str_error
        #
        trace(self.d_output_service, 'Output SERVICIO')
        return self.d_output_service

    def __save_data__(self):
        '''
        Guarda los datos en la REDIS (PKLINE) y en la SQL.
        Esto ultimo se hace a travez de las colas de datos de REDIS !!!
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_service['PARAMS'], ('DLGID','D_PAYLOAD') )
        if res:
            # Proceso: Copio el dict de entrada al servicio y le agrego la seccion API
            d_input_api = self.d_input.copy()
            d_payload = self.d_input_service.get('PARAMS',{}).get('D_PAYLOAD',{})
            dlgid = self.d_input_service.get('PARAMS',{}).get('DLGID',[])
            d_input_api['API'] = { 'REQUEST': 'SAVE_DATA_LINE', 'PARAMS':{'D_PAYLOAD': d_payload, 'DLGID':dlgid }}
            # consulto a la api y evaluo el resultado
            d_output_api = self.apiRedis_handle.process( d_input_api)
            result = d_output_api.get('API',{}).get('RESULT',False)
            self.d_output_service['SERVICIO']['RESULT'] = result
        else:
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['API']['PARAMS']['ERROR'] =  str_error
          
    def __get_ordenes__(self):
        '''
        Lee de REDIS si hay ordenes para enviar la equipo remoto en las respuestas.
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_service['PARAMS'], ('DLGID',) )
        if res:
            # Proceso: Copio el dict de entrada al servicio y le agrego la seccion API
            d_input_api = self.d_input.copy()
            dlgid = self.d_input_service.get('PARAMS',{}).get('DLGID','')
            d_input_api['API'] = { 'REQUEST': 'GET_ORDENES', 'PARAMS':{'DLGID': dlgid }}
            # consulto a la api y evaluo el resultado
            d_output_api = self.apiRedis_handle.process( d_input_api)
            result = d_output_api.get('API',{}).get('RESULT',False)
            if result:
                ordenes = d_output_api.get('API',{}).get('PARAMS',{}).get('ORDENES','')
                self.d_output_service['SERVICIO']['RESULT'] = True
                self.d_output_service['SERVICIO']['PARAMS']['ORDENES'] =  ordenes
                return
        else:
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['API']['PARAMS']['ERROR'] =  str_error

    def __delete_entry__(self):
        '''
        Borra la entrada correspondiente a dlgid de la bd REDIS
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_service['PARAMS'], ('DLGID',) )
        if res:
            # Proceso: Copio el dict de entrada al servicio y le agrego la seccion API
            d_input_api = self.d_input.copy()
            dlgid = self.d_input_service['PARAMS']['DLGID']
            d_input_api['API'] = { 'REQUEST': 'DELETE_ENTRY', 'PARAMS':{'DLGID': dlgid}}
            # consulto a la api y evaluo el resultado
            d_output_api = self.apiRedis_handle.process( d_input_api)
            result = d_output_api.get('API',{}).get('RESULT',False)
            self.d_output_service['SERVICIO']['RESULT'] = result
        else:
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['API']['PARAMS']['ERROR'] =  str_error

class TestServicioDatos:

    def __init__(self):
        self.servicio_datos = ServicioDatos()

    def test_save_data(self):

        d_request = {'SERVICE_REQUEST':'SAVE_DATA','DLGID':'PABLO_TEST', 'D_PAYLOAD': {'Pa':1.23,'Pb':3.45}}
        print('* SERVICE: SAVE_DATA...')  
        d_res = self.servicio_datos.process(d_request)
        if d_res.get('RESULT'):
            print('OK')
        else:
            print('FAIL')
            print(d_res)      

    def test_get_ordenes(self):
        d_request = {'SERVICE_REQUEST':'GET_ORDENES','DLGID':'PABLO_TEST'}
        print('* SERVICE: GET_ORDENES...')  
        d_res = self.servicio_datos.process(d_request)
        if d_res.get('RESULT'):
            print('OK')
            orden = d_res.get('ORDENES','ERR')
            print(f'ORDEN={orden}')
        else:
            print('FAIL')
            print(d_res)

    def test_delete_entry(self):
        d_request = {'SERVICE_REQUEST':'DELETE_ENTRY','DLGID':'PABLO_TEST'}
        print('* SERVICE: DELETE_ENTRY...')  
        d_res = self.servicio_datos.process(d_request)
        if d_res.get('RESULT'):
            print('OK')
        else:
            print('FAIL')
            print(d_res)

if __name__ == '__main__':

    import pprint
    test = TestServicioDatos()
    test.test_save_data()
    test.test_get_ordenes()
    test.test_delete_entry()
