#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase que implementa los diferentes servicios de configuracion:
puede ser para leer la configuracion del equipo o para la configuracion del UID.
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

class ServicioConfiguracion():
    '''
    Procesa los pedidos de configuracion de equipos.
    Usamos un selector ( callback ) por medio de un string en el dict de entrada.
    Nos permite modularizar y tener un diseño mas claro y uniforme con el resto del programa
    Interface:
    La in/out es a travez de dict() que deben tener la clave 'SERVICIO'
    No importan las otras claves.

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
        self.d_output_service = {'SERVICIO': { 'RESULT':False, 'PARAMS':{'MODULE':'SERVICIO_CONF'}} }
        self.cbk_request = None
        self.apiRedis_handle = ApiRedis()
        self.apiSql_handle = ApiBdSql()
        self.callback_functions =  { 'READ_CONFIG': self.__read_config__, 
                                     'READ_DLGID_FROM_UID': self.__read_dlgid_from_ui__,
                                     'READ_CREDENCIALES': self.__read_credenciales__,
                                     'UPDATE_CREDENCIALES': self.__update_credenciales__,
                                     'READ_DEBUG_DLGID': self.__get_debug_dlgid__,
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
    
    def __read_config__(self):
        '''
        Leo la configuracion de la redis.
        Si no esta, pruebo con la sql y si esta actualizo la redis.
        Con los datos de entrada, prepara el dict() para la api.
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_service['PARAMS'], ('DLGID',) )
        if res:
            # Proceso: Copio el dict de entrada al servicio y le agrego la seccion API
            d_input_api = self.d_input.copy()
            dlgid = self.d_input_service['PARAMS']['DLGID']
            d_input_api['API'] = { 'REQUEST': 'READ_CONFIG', 'PARAMS':{'DLGID':dlgid}}
            # consulto a la api y evaluo el resultado
            d_output_api = self.apiRedis_handle.process( d_input_api).get('API',{})
            result = d_output_api['RESULT']
            if result:
                d_conf = d_output_api.get('PARAMS',{}).get('D_CONF',{})
                self.d_output_service['SERVICIO']['RESULT'] = True
                self.d_output_service['SERVICIO']['PARAMS']['D_CONF'] =  d_conf
                return
            # Redis no tiene la configuracion: pregunto a sql
            d_output_api = self.apiSql_handle.process( d_input_api)
            result = d_output_api['RESULT']
            if result:
                d_conf = d_output_api.get('PARAMS',{}).get('D_CONF',{})
                self.d_output_service['SERVICIO']['RESULT'] = True
                self.d_output_service['SERVICIO']['PARAMS']['D_CONF'] =  d_conf
                # Actualizo la redis
                d_input_api['API'] = { 'REQUEST': 'SET_CONFIG', 'PARAMS':{'DLGID':dlgid, 'D_CONF':d_conf}}
                d_output_api = self.apiRedis_handle.process( d_input_api)
                # No chequeo este resultado.( podria hacerlo )
                return
        else:
            # Los parametros de entrada no son correctos
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['SERVICIO']['PARAMS']['ERROR'] =  str_error

    def __read_dlgid_from_ui__(self):
        '''
        Recupera de las BD el dlgid que corresponde al uid
        Pregunto primero a redis. Si no esta pregunto a SQL
        RETURN: dlgid o None.
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_service['PARAMS'], ('UID',) )
        if res:
            # Proceso: Copio el dict de entrada al servicio y le agrego la seccion API
            d_input_api = self.d_input.copy()
            uid = self.d_input_service['PARAMS']['UID']
            d_input_api['API'] = { 'REQUEST': 'READ_DLGID_FROM_UID', 'PARAMS':{'UID':uid}}
            # consulto a la api y evaluo el resultado
            d_output_api = self.apiRedis_handle.process( d_input_api).get('API',{})
            result = d_output_api['RESULT']
            if result:
                new_dlgid = d_output_api.get('PARAMS',{}).get('DLGID','00000')
                self.d_output_service['SERVICIO']['RESULT'] = True
                self.d_output_service['SERVICIO']['PARAMS']['DLGID'] =  new_dlgid
                return
            # No esta en redis: pruebo con la sql y si esta, actualizo la redis.
            d_output_api = self.apiSql_handle.process( d_input_api)
            result = d_output_api['RESULT']
            if result:
                new_dlgid = d_output_api.get('PARAMS',{}).get('DLGID','00000')
                self.d_output_service['SERVICIO']['RESULT'] = True
                self.d_output_service['SERVICIO']['PARAMS']['DLGID'] =  new_dlgid
                # Actualizo la redis
                d_input_api['API'] = { 'REQUEST': 'SET_DLGID_UID', 'PARAMS':{'DLGID': new_dlgid } }
                d_output_api = self.apiRedis_handle.process( d_input_api)
                # No chequeo este resultado.( podria hacerlo )
                return
        else:
            # Los parametros de entrada no son correctos
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['SERVICIO']['PARAMS']['ERROR'] =  str_error
        #
   
    def __get_debug_dlgid__(self):
        '''
        Recupera el dlgid que se usa para maximo debug.
        RETURN: dlgid o None.
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_service['PARAMS'], tuple() )
        if res:
            # Proceso: Copio el dict de entrada al servicio y le agrego la seccion API
            d_input_api = self.d_input.copy()
            d_input_api['API'] = { 'REQUEST': 'READ_DEBUG_DLGID', 'PARAMS': dict() }
            # consulto a la api y evaluo el resultado
            d_output_api = self.apiRedis_handle.process( d_input_api).get('API',{})
            result = d_output_api['RESULT']
            if result:
                debug_dlgid = d_output_api.get('PARAMS',{}).get('DEBUG_DLGID','00000')
                self.d_output_service['SERVICIO']['RESULT'] = True
                self.d_output_service['SERVICIO']['PARAMS']['DEBUG_DLGID'] =  debug_dlgid
                return
        else:
            # Los parametros de entrada no son correctos
            self.d_output_service['SERVICIO']['RESULT'] = False
            self.d_output_service['SERVICIO']['PARAMS']['ERROR'] =  str_error
        #

    def __read_credenciales__(self):
        '''
        Con el par (dlgid,uid) busca en la BD si esta el mismo
        '''
        pass

    def __update_credenciales__( self):
        '''
        Actualiza el par UID,DLGID en las bases de datos.
        '''
        pass


class TestServicioConfiguracion:

    def __init__(self):
        self.servConf = ServicioConfiguracion()
        
    def test_read_config(self):
        d_request = { 'SERVICIO': {'REQUEST':'READ_CONFIG', 'PARAMS': {'DLGID':'PABLO'}}}
        print('* SERVICE: READ_CONFIG Start...')  
        d_response = self.servConf.process(d_request)
        d_service_response = d_response.get('SERVICIO',{})
        rsp = d_service_response.get('RESULT', False)
        if rsp:
            d_conf = d_service_response.get('PARAMS',{}).get('D_CONF',{})
            print('TEST OK')
        else:
            print('TEST FAIL')
        
        print('REQUEST:')
        pprint.pprint(d_request) 
        print('RESPONSE:')
        pprint.pprint(d_response)
        print('* SERVICE: TEST READ_CONFIG End...') 

    def test_debug_dlgid(self):
        d_request = { 'SERVICIO': {'REQUEST':'READ_DEBUG_DLGID', 'PARAMS': dict() }}
        print('* SERVICE: READ_DEBUG_DLGID Start...')  
        d_response = self.servConf.process(d_request)
        d_service_response = d_response.get('SERVICIO',{})
        rsp = d_service_response.get('RESULT', False)
        if rsp:
            debug_dlgid = d_service_response.get('PARAMS',{}).get('DEBUG_DLGID','00000')
            print(f'TEST OK {debug_dlgid}')
        else:
            print('TEST FAIL')
        
        print('REQUEST:')
        pprint.pprint(d_request) 
        print('RESPONSE:')
        pprint.pprint(d_response)
        print('* SERVICE: READ_DEBUG_DLGID End...') 

if __name__ == '__main__':
    import pprint
    test = TestServicioConfiguracion()
    #test.test_read_config()
    test.test_debug_dlgid()
