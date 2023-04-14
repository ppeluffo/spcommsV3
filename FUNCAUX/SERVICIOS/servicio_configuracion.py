#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa los diferentes servicios de configuracion:
puede ser para leer la configuracion del equipo o para la configuracion del UID.
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
from FUNCAUX.UTILS.spc_log import log2, config_logger, set_debug_dlgid
from FUNCAUX.UTILS.spc_utils import trace

# ------------------------------------------------------------------------------

class ServicioConfiguracion():
    '''
    Procesa los pedidos de configuracion de equipos.
    Usamos un selector ( callback ) por medio de un string en el dict de entrada.
    Nos permite modularizar y tener un diseño mas claro y uniforme con el resto del programa
    Interface:

    ENTRADA: 
        D_INPUT =   { 'REQUEST':'READ_CONFIG', 
                      'DLGID':str,
                      'PARAMS: {'D_CONF': dict
                                'D_PAYLOAD':dict
                                'UID':str
                                }
                    }

    SALIDA: 
        D_OUTPUT =  { 'RESULT':bool, 
                      'DLGID':str,
                      'PARAMS': {'D_CONF':dict(), 
                                    'DEBUG_DLGID':str, 
                                    'ORDENES':str, 
                                    'DLGID':str }
                                 }
                    }

    '''
    def __init__(self):
        self.d_input_service = {}
        self.d_output_service = {}
        self.cbk_request = None
        self.apiRedis_handle = ApiRedis()
        self.apiSql_handle = ApiBdSql()
        self.callback_functions =  { 'READ_CONFIG': self.__read_config__, 
                                     'READ_DLGID_FROM_UID': self.__read_dlgid_from_uid__,
                                     'UPDATE_CREDENCIALES': self.__update_credenciales__,
                                     'READ_DEBUG_DLGID': self.__get_debug_dlgid__,
                                    }

    def process(self, d_input:dict):
        '''
        Unica funcion publica que procesa los requests al SERVICIO .
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.d_input_service = d_input
        # Chequeo parametros de entrada
        tag = random.randint(0,1000)
        trace(self.d_input_service, f'Input SERVICIO Conf ({tag})')
        #
        self.cbk_request = self.d_input_service.get('REQUEST','')
        # Ejecuto la funcion de callback
        if self.cbk_request in self.callback_functions:
            self.callback_functions[self.cbk_request]()  
        else:
            trace(self.d_output_service, f'Output SERVICIO Conf NO DISPONIBLE ({tag})')   
        # 
        trace(self.d_output_service, f'Output SERVICIO Conf ({tag})')
        return self.d_output_service
       
    def __read_config__(self):
        '''
        Leo la configuracion de la redis.
        Si no esta, pruebo con la sql y si esta actualizo la redis.
        Con los datos de entrada, prepara el dict() para la api.
        '''
        query_params = {''}
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api
        result = d_output_api.get('RESULT',False)
        if result:
            return
        # Redis no tiene la configuracion: pregunto a sql
        d_output_api = self.apiSql_handle.process( d_input_api)
        self.d_output_service = d_output_api
        result = d_output_api.get('RESULT',False)
        if result:
            # Actualizo la redis
            d_input_api['REQUEST'] = 'SET_CONFIG'
            # Agrego el diccionario de la configuracion leida de sql
            d_conf = self.d_output_service.get('PARAMS',{}).get('D_CONF',{})
            d_input_api['PARAMS']['D_CONF'] = d_conf
            d_output_api = self.apiRedis_handle.process( d_input_api)
            # No chequeo este resultado.( podria hacerlo )
            # self.d_output_service = d_output_api
            return
        #
        # Los parametros de entrada no son correctos

    def __read_dlgid_from_uid__(self):
        '''
        Recupera de las BD el dlgid que corresponde al uid
        Pregunto primero a redis. Si no esta pregunto a SQL
        d_in =  { 'REQUEST':'READ_DLGID_FROM_UID','DLGID':dlgid, 'PARAMS': {'UID':uid } }
        RETURN: dlgid o None.
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        #  d_output_api = {'RESULT':?, 'DLGID':?, 'PARAMS':{'DLGID':?}}
        self.d_output_service = d_output_api
        result = d_output_api.get('RESULT',False)
        dlgid = d_output_api.get('PARAMS',{}).get('DLGID','00000')
        if result and dlgid != '00000':
            # El resultado esta en la REDIS ( esta actualizada )
            return
        #
        # No esta en redis: pruebo con la sql y si esta, actualizo la redis.
        d_output_api = self.apiSql_handle.process( d_input_api)
        self.d_output_service = d_output_api
        result = d_output_api.get('RESULT',False)
        if result:
            # Actualizo la redis
            d_input_api['REQUEST'] = 'SET_DLGID_UID'
            d_output_api = self.apiRedis_handle.process( d_input_api)
            # No chequeo este resultado.( podria hacerlo )
            # self.d_output_service = d_output_api
            return
        #
        # Los parametros de entrada no son correctos
        #
   
    def __get_debug_dlgid__(self):
        '''
        Recupera el dlgid que se usa para maximo debug.
        RETURN: dlgid o None.
        '''
        d_input_api = self.d_input_service
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api

    def __update_credenciales__( self):
        '''
        Si las credenciales (dlgid, uid) estan en redis, salgo OK
        Si no estan las actualizo en SQL y en REDIS
        d_in = { 'REQUEST':'UPDATE_CREDENCIALES','DLGID':dlgid, 'PARAMS': {'DLGID':dlgid, 'UID':uid } }
        '''
        # Leo de REDIS el par (uid, dlgid)
        dlgid = self.d_input_service.get('PARAMS',{}).get('DLGID','00000')
        uid = self.d_input_service.get('PARAMS',{}).get('UID','00000')
        #
        d_input_api = { 'REQUEST':'READ_DLGID_FROM_UID','DLGID':dlgid, 'PARAMS': {'UID':uid } }
        d_output_api = self.apiRedis_handle.process( d_input_api)
        self.d_output_service = d_output_api
        result = d_output_api.get('RESULT',False)
        dlgid_redis =d_output_api.get('PARAMS',{}).get('DLGID','00000')
        # Si coinciden, salgo con OK.
        if result and dlgid == dlgid_redis:
            return {'RESULT': True, 'PARAMS':{} }
        #
        # No coinciden: Actualizo SQL y REDIS. No chequeo errores
        d_input_api = { 'REQUEST':'SET_DLGID_UID','DLGID':dlgid, 'PARAMS': {'UID':uid } }
        _ = self.apiRedis_handle.process( d_input_api)
        _ = self.apiSql_handle.process( d_input_api)

class TestServicioConfiguracion:

    def __init__(self):
        self.servConf = ServicioConfiguracion()
        self.dlgid = ''
        
    def test_read_config(self):
        
        print('* SERVICE: READ_CONFIG Start...')  
        #
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        #
        d_request = {'REQUEST':'READ_CONFIG', 'PARAMS': {}, 'DLGID':self.dlgid }
        d_response = self.servConf.process(d_request)
        rsp = d_response.get('RESULT', False)
        if rsp:
            # d_conf = d_response.get('PARAMS',{}).get('D_CONF',{})
            print('TEST OK')
        else:
            print('TEST FAIL')
        print('* SERVICE: TEST READ_CONFIG End...') 

    def test_read_dlgid_from_ui(self):

        print('* SERVICE: READ_DLGID_FROM_UI Start...')  
        #
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        #
        d_request = {'REQUEST':'READ_DLGID_FROM_UID', 'PARAMS': {'UID':'0123456789' }, 'DLGID':self.dlgid }
        d_response = self.servConf.process(d_request)
        rsp = d_response.get('RESULT', False)
        if rsp:
            # d_conf = d_response.get('PARAMS',{}).get('D_CONF',{})
            dlgid = d_response.get('PARAMS',{}).get('DLGID','00000')
            print(f'TEST OK {dlgid}')
        else:
            print('TEST FAIL')
        print('* SERVICE: READ_DLGID_FROM_UI End...') 

    def test_debug_dlgid(self):

        print('* SERVICE: READ_DEBUG_DLGID Start...')  
        d_request = { 'REQUEST':'READ_DEBUG_DLGID', 'PARAMS': {} }
        d_response = self.servConf.process(d_request)
        rsp = d_response.get('RESULT', False)
        if rsp:
            debug_dlgid = d_response.get('PARAMS',{}).get('DEBUG_DLGID','00000')
            print(f'TEST OK {debug_dlgid}')
        else:
            print('TEST FAIL')
        print('* SERVICE: READ_DEBUG_DLGID End...') 

if __name__ == '__main__':
    
    config_logger('CONSOLA')

    test = TestServicioConfiguracion()
    #est.test_read_config()
    #test.test_read_dlgid_from_ui()

    #test.test_debug_dlgid()
