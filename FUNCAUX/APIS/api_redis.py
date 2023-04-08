#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

'''
Funciones de API de configuracion.
Se utilizan para leer configuracion de equipos de una BD persistente (GDA)
'''

import os
import sys
import pickle
import redis

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log2
from FUNCAUX.UTILS.spc_utils import trace, check_particular_params, check_inputs

# ------------------------------------------------------------------------------

class ApiRedis:
    '''
    Interface con la BD redis.
    ENTRADA: 
        D_INPUT =   { API:  { 'REQUEST':'READ_CONFIG', 
                            'PARAMS: {'DLGID':str
                                     'D_CONF': dict
                                     'D_PAYLOAD':dict
                                     'UID':str
                                     }
                            }
                    }

    SALIDA: 
        D_OUTPUT =  { 'API': { 'RESULT':bool, 
                               'PARAMS': {'D_CONF':dict(), 
                                         'DEBUG_DLGID':str, 
                                         'ORDENES':str, 
                                         'DLGID':str }
                                        }
                            }
                    }

    La interface que presenta esta normalizada. Todos los datos de entrada
    en un dict y todos los de salida en otro.
    Todos los requirements los corremos como callbacks de modo de tener solo
    un metodo publico ( process ).
    Esto nos permite tener un diseño bien estructurado:
    - entradas:procesos:salidas 
    De este modo podemos debugear la entrada y salida.
    '''
   
    def __init__(self):
        self.d_input = {}
        self.d_input_api = {}
        self.d_output_api = {'API': { 'RESULT':False, 'PARAMS':{'MODULE':'API_REDIS'}} }
        self.cbk_request = None
        self.rh = __BdRedis__()
        self.callback_functions =  { 'READ_CONFIG': self.__read_config__,
                                    'SET_CONFIG': self.__set_config__,
                                    'READ_DEBUG_DLGID': self.__read_debug_dlgid__,
                                    'SAVE_DATA_LINE': self.__save_data_line__,
                                    'GET_ORDENES': self.__get_ordenes__,
                                    'DELETE_ENTRY': self.__delete_entry__,
                                    'READ_DLGID_FROM_UID': self.__read_dlgid_from_ui__,
                                    'SET_DLGID_UID': self.__set_dlgid_uid__
                                    }

    def process(self, d_input:dict):
        '''
        Unica funcion publica que procesa los requests a la API.
        Permite poder hacer un debug de la entrada y salida.
        '''
        # Chequeo parametros de entrada
        self.d_input = d_input
        trace(self.d_input, "Input API")
        #
        res, str_error = check_inputs(d_input, 'API' )
        if res:
            self.d_input_api = d_input['API']
            self.cbk_request = self.d_input_api.get('REQUEST','')
            # Ejecuto la funcion de callback
            self.callback_functions[self.cbk_request]()  
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error
        #
        trace(self.d_output_api, 'Output API')
        return self.d_output_api
      
    def __read_config__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('DLGID',) )
        if res:
            # Proceso
            dlgid = self.d_input_api['PARAMS']['DLGID']
            d_conf = self.rh.get_config(dlgid)
            self.d_output_api['API']['RESULT'] = True
            self.d_output_api['API']['PARAMS']['D_CONF'] =  d_conf
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

    def __set_config__(self):
        # Chequeo parametros particulares 
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('DLGID','CONF') )
        if res:    
            # Proceso
            dlgid = self.d_input_api['PARAMS']['DLGID']
            d_conf = self.d_input_api['PARAMS']['D_CONF']
            result = self.rh.set_config(dlgid, d_conf) # True/False
            self.d_output_api['API']['RESULT'] = result
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

    def __read_debug_dlgid__(self):
        debug_dlgid = self.rh.get_debug_dlgid()
        self.d_output_api['API']['RESULT'] = True
        self.d_output_api['API']['PARAMS']['DEBUG_DLGID'] =  debug_dlgid

    def __save_data_line__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('DLGID','PAYLOAD') )
        if res:
            # Proceso
            dlgid = self.d_input_api['PARAMS']['DLGID']
            payload = self.d_input_api['PARAMS']['D_PAYLOAD']
            result = self.rh.save_payload( dlgid, payload) # True/False
            self.d_output_api['API']['RESULT'] = result
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

    def __get_ordenes__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('DLGID',) )
        if res:
            # Proceso
            dlgid = self.d_input_api['PARAMS']['DLGID']
            ordenes = self.rh.get_ordenes(dlgid)
            self.d_output_api['API']['RESULT'] = True
            self.d_output_api['API']['PARAMS']['ORDENES'] =  ordenes
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

    def __delete_entry__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('DLGID',) )
        if res:
            # Proceso
            dlgid = self.d_input_api['PARAMS']['DLGID']
            result = self.rh.delete_entry(dlgid)  # True/False
            self.d_output_api['API']['RESULT'] = result
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

    def __read_dlgid_from_ui__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('UID',) )
        if res:
            # Proceso
            uid = self.d_input_api['PARAMS']['UID']
            dlgid = self.rh.get_dlgid_from_uid(uid)
            self.d_output_api['API']['RESULT'] = True
            self.d_output_api['API']['PARAMS']['DLGID'] =  dlgid
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

    def __set_dlgid_uid__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('DLGID','UID') )
        if res:
            dlgid = self.d_input_api['PARAMS']['DLGID']
            uid = self.d_input_api['PARAMS']['UID']
            result = self.rh.set_dlgid_uid(dlgid, uid)  # True/False
            self.d_output_api['API']['RESULT'] = result
            self.d_output_api['API']['PARAMS']['DLGID'] =  dlgid
            self.d_output_api['API']['PARAMS']['UID'] =  uid
        else:
            self.d_output_api['API']['RESULT'] = False
            self.d_output_api['API']['PARAMS']['ERROR'] =  str_error

class __BdRedis__:
    '''
    Implemento los metodos para leer de la BD redis.
    Por ahora es la 192.168.0.6
    '''
    def __init__(self):
        self.connected = False
        self.rh = redis.Redis()

    def connect(self):
        '''
        Cada acceso primero verifica si esta conectada asi que aqui incrementamos el contador de accesos.
        '''
        if self.connected:
            return True

        host=Config['REDIS']['host']
        port=int(Config['REDIS']['port'])
        db=int( Config['REDIS']['db'], 10)
       
        try:
            self.rh = redis.Redis( host, port, db)
            self.connected = True
        except redis.RedisError as re:
            d_log = { 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR', 'MSG':'Bd redis init ERROR' }
            log2(d_log)
            d_log = { 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR', 'MSG':f'EXCEPTION={re}' }
            log2(d_log)
            self.connected = False

        return self.connected
    
    def get_config(self, dlgid:str)->dict:
        '''
        Recupera la clave dlgid, des-serializa y obtiene el diccionario con la configuracion
        RETURN: dict() o None
        '''
        if dlgid is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR', 'MSG':'REDIS NOT CONNECTED !!' }
            log2(d_log)
            return {}
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return {}
        #
        # Hay registro ( HASH ) del datalogger:
        pdict = self.rh.hget(dlgid, 'CONFIG')
        if pdict is not None:
            dconf = pickle.loads(pdict)
            return dconf
        #
        return {}

    def set_config(self, dlgid:str, d_conf:dict)->bool:
        '''
        Serializa d_conf y crea una entrada del datalogger en la redis local.
        Inicializa el resto de las entradas.
        RETURN: True/False
        '''
        if ( dlgid is None )  or ( d_conf is None ):
            d_log = { 'MODULE':__name__, 'FUNTION':'set_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid, 'MSG':'ERROR DE PARAMETROS' }
            log2(d_log)
            return False
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'set_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid, 'MSG':'REDIS NOT CONNECTED !!' }
            log2(d_log)
            return False
        # 
        pkconf = pickle.dumps(d_conf)
        _ = self.rh.delete(dlgid)   # Borramos previamente la clave
        #
        _ = self.rh.hset(dlgid, 'CONFIG', pkconf)
        _ = self.rh.hset(dlgid, 'VALID', 'TRUE')
        _ = self.rh.hset(dlgid, 'RESET', 'FALSE')
        return True

    def get_debug_dlgid(self)->str:
        '''
        Retorna el ID del equipo con el LEVEL=SELECT para logs
        RETURN: dlgid
        '''

        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'get_debug_dlgid', 'LEVEL':'ERROR',
                    'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return '00000'
        #
        # Hay registro ( HASH ) del datalogger:
        debug_dlgid = self.rh.hget('SPCOMMS', 'DEBUG_DLGID')
        if not debug_dlgid:
            d_log = { 'MODULE':__name__, 'FUNTION':'get_debug_dlgid', 'LEVEL':'ERROR',
                     'MSG':'ERROR in HGET: No record. !!' }
            log2(d_log)
            return '00000'
        #
        if debug_dlgid:
            return debug_dlgid.decode()
        else:
            return '00000'
         
    def save_payload(self, dlgid:str, d_payload:dict)->bool:
        '''
        Guarda el d_payload serializado en el registro del equpo 
        y tambien en la cola de datos para procesar hacia el SQL
        RETURN: True/False
        '''
        pkline = pickle.dumps(d_payload)
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'save_payload', 'LEVEL':'ERROR','DLGID':dlgid,
                      'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return False
        #
        _ = self.rh.hset(dlgid,'PKLINE', pkline)
        _ = self.rh.rpush( 'LQ_SPXR3_DATA', pkline)
        return True
 
    def get_ordenes(self, dlgid:str)->str:
        '''
        Todas las ordenes se escriben en la key ORDERS, inclusive el RESET !!
        RETURN: orders_str
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'get_ordenes', 'LEVEL':'ERROR',
                     'DLGID':dlgid, 'MSG':'REDIS NOT CONNECTED !!' }
            log2(d_log)
            return ''
        #
        ordenes = self.rh.hget(dlgid, 'ORDERS' )
        if ordenes:
            return ordenes.decode()
        else:
            return ''

    def delete_entry(self, dlgid:str)->bool:
        '''
        Borra el registro de la redis local !!
        RETURN: True/False
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'delete_entry', 'LEVEL':'ERROR',
                     'DLGID':dlgid, 'MSG':'REDIS NOT CONNECTED !!' }
            log2(d_log)
            return False
        #
        _ = self.rh.delete(dlgid)
        return True
    
    def get_dlgid_from_uid(self, uid:str)->str:
        '''
        Retorna el DLGID asociado al UID
        RETURN: dlgid
        En la redis tenemos un HASH llamado UID2DLGID donde cada clave es un
        uid y el valor asociado el dlgid
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'ERROR',
                     'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return '00000'
        #
        # Hay registro ( HASH ) del datalogger:
        dlgid = self.rh.hget('UID2DLGID', uid)
        if not dlgid:
            dlgid = '00000'
        #
        # Redis devuelve bytes....
        if isinstance(dlgid, bytes):
            dlgid = dlgid.decode()
        return dlgid

    def set_dlgid_uid(self, dlgid:str, uid:str)->bool:
        '''
        Ingresa una entrada en el HASH UID2DLGID con el valor del uid, dlgid.
        RETORNA: True/False
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'set_dlgid_uid', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return False
        #
        # Hay registro ( HASH ) del datalogger:
        self.rh.hset('UID2DLGID', uid, dlgid)       
        return True

class TestApiRedis:

    def __init__(self):
        self.d_conf = {}
        self.api = ApiRedis()
        import pprint

    def test_read_config(self):
        d_request = { 'API': {'REQUEST':'READ_CONFIG', 'PARAMS': {'DLGID':'PABLO'}}}
        print('* READ_CONFIG...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        d_conf = d_api_response.get('PARAMS',{}).get('D_CONF',{})
        if ('BASE', 'FIRMWARE') in d_conf:
            self.d_conf = d_conf
            print('TEST API OK')
        else:
            print('TEST API FAIL')
            print('REQUEST:')
            pprint.pprint(d_request) 
        print('RESPONSE:')
        pprint.pprint(d_response)

    def test_delete_entry(self):
        d_request = {'API': {'REQUEST':'DELETE_ENTRY', 'PARAMS': {'DLGID':'PABLO_TEST'}}}
        print('* DELETE_ENTRY...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        rsp = d_api_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')
        print('RESPONSE:')
        pprint.pprint(d_response)

    def test_set_config(self):
        d_request = {'API': {'REQUEST':'SET_CONFIG', 'PARAMS': {'DLGID':'PABLO_TEST', 'D_CONF':self.d_conf}}}
        print('* SET_CONFIG...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        rsp = d_api_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')
        print('RESPONSE:')
        pprint.pprint(d_response)

    def test_get_debug_dlgid(self):
        d_request = {'API': {'REQUEST':'READ_DEBUG_DLGID' }}
        print('* READ_DEBUG_DLGID...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        debug_dlgid = d_api_response.get('PARAMS',{}).get('DEBUG_DLGID','00000')
        print(f'OK {debug_dlgid}')
        print('RESPONSE:')
        pprint.pprint(d_response)
  
    def test_save_dataline(self):
        d_request = {'API': {'REQUEST':'SAVE_DATA_LINE', 'PARAMS': {'DLGID':'PABLO_TEST', 'D_PAYLOAD' : { 'PA':1.23,'PB':4.56}}}}
        print('* SAVE_DATA_LINE...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        rsp = d_api_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')
        print('RESPONSE:')
        pprint.pprint(d_response)

    def test_get_dlgid_from_uid(self):
        d_request = {'API': {'REQUEST':'READ_DLGID_FROM_UID', 'PARAMS': {'UID':'0123456789' }}}
        print('* READ_DLGID_FROM_UID...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        rsp = d_api_response.get('RESULT',False)
        dlgid = d_api_response.get('DLGID','11111')
        print(f'OK {dlgid}')
        print('RESPONSE:')
        pprint.pprint(d_response)

    def test_set_dlgid_uid(self):
        d_request = {'API': {'REQUEST':'SET_DLGID_UID', 'PARAMS': {'DLGID':'PRUEBA2', 'UID':'0123456789876543210' }}}
        print('* SET_DLGID_UID...')  
        d_response = self.api.process(d_request)
        d_api_response = d_response.get('API',{})
        rsp = d_api_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')
        print('RESPONSE:')
        pprint.pprint(d_response)

if __name__ == "__main__":

    import pprint
    # Test api_redis
    print('TESTING API REDIS...')
    test_api_redis = TestApiRedis()
    test_api_redis.test_read_config()
    #test_api_redis.test_delete_entry()
    #test_api_redis.test_set_config()
    #test_api_redis.test_get_debug_dlgid()
    #test_api_redis.test_save_dataline()
    #test_api_redis.test_get_dlgid_from_uid()
    #test_api_redis.test_set_dlgid_uid()
    sys.exit(0)
