#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

'''
Funciones de API de configuracion.
Se utilizan para leer configuracion de equipos de una BD persistente (GDA)
'''

import os
import sys
import pickle
import redis
import random
import datetime as dt

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log2, config_logger, set_debug_dlgid
from FUNCAUX.UTILS.spc_utils import trace, check_particular_params
from FUNCAUX.UTILS import spc_stats

# ------------------------------------------------------------------------------

class ApiRedis:
    '''
    Interface con la BD redis.
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

    La interface que presenta esta normalizada. Todos los datos de entrada
    en un dict y todos los de salida en otro.
    Todos los requirements los corremos como callbacks de modo de tener solo
    un metodo publico ( process ).
    Esto nos permite tener un diseño bien estructurado:
    - entradas:procesos:salidas 
    De este modo podemos debugear la entrada y salida.
    '''
   
    def __init__(self):
        self.d_input_api = {}
        self.d_output_api = {}
        self.cbk_request = None
        self.rh = BdRedis()
        self.callback_functions =  { 'READ_CONFIG': self.__read_config__,
                                    'SET_CONFIG': self.__set_config__,
                                    'READ_DEBUG_DLGID': self.__read_debug_dlgid__,
                                    'SAVE_DATA_LINE': self.__save_data_line__,
                                    'GET_ORDENES': self.__get_ordenes__,
                                    'DELETE_ENTRY': self.__delete_entry__,
                                    'READ_DLGID_FROM_UID': self.__read_dlgid_from_uid__,
                                    'SET_DLGID_UID': self.__set_dlgid_uid__,
                                    'SAVE_STATS': self.__save_stats__, 
                                    'SAVE_FRAME_TIMESTAMP': self.__save_frame_timestamp__,
                                    'GET_QUEUE_LENGTH': self.__get_queue_length__,
                                    'DELETE_QUEUE': self.__delete_queue__,
                                    }

    def process(self, d_input:dict):
        '''
        Unica funcion publica que procesa los requests a la API.
        Permite poder hacer un debug de la entrada y salida.
        '''
        self.d_input_api = d_input
        # Chequeo parametros de entrada
        tag = random.randint(0,1000)
        trace(self.d_input_api, f'Input API Redis ({tag})')
        #
        self.cbk_request = self.d_input_api.get('REQUEST','')
        # Ejecuto la funcion de callback
        if self.cbk_request in self.callback_functions:
            self.callback_functions[self.cbk_request]()  
        else:
            trace(self.d_output_api, f'Output API Redis NO DISPONIBLE.({tag})')    
        #
        trace(self.d_output_api, f'Output API Redis ({tag})')
        return self.d_output_api
      
    def __read_config__(self):
        dlgid = self.d_input_api.get('DLGID','')
        d_conf = self.rh.get_config(dlgid)
        result = False
        if d_conf:
            result = True
        self.d_output_api = {'RESULT':result, 'DLGID':dlgid, 'PARAMS': { 'D_CONF': d_conf }}

    def __set_config__(self):
        # Chequeo parametros particulares 
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('CONF',) )
        dlgid = self.d_input_api.get('DLGID','00000')
        if res:    
            # Proceso
            d_conf = self.d_input_api['PARAMS']['D_CONF']
            result = self.rh.set_config(dlgid, d_conf) # True/False
            self.d_output_api = {'RESULT': result, 'DLGID':dlgid, 'PARAMS': {}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':dlgid, 'PARAMS': {'ERROR': str_error}}

    def __read_debug_dlgid__(self):
        debug_dlgid = self.rh.get_debug_dlgid()
        self.d_output_api = {'RESULT': True, 'DLGID':'00000', 'PARAMS':{'DEBUG_DLGID':debug_dlgid, 'LOG':False}}

    def __save_data_line__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('PAYLOAD',) )
        dlgid = self.d_input_api.get('DLGID','00000')
        if res:
            # Proceso
            payload = self.d_input_api.get('PARAMS',{}).get('D_PAYLOAD',{})
            _ = self.rh.save_payload( dlgid, payload) # True/False
            self.d_output_api = {'RESULT': True, 'DLGID':dlgid, 'PARAMS':{}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':dlgid, 'PARAMS':{'ERROR':str_error}}

    def __get_ordenes__(self):
        # Proceso
        dlgid = self.d_input_api.get('DLGID','')
        ordenes = self.rh.get_ordenes(dlgid)
        self.d_output_api = {'RESULT': True, 'DLGID':dlgid, 'PARAMS':{'ORDENES': ordenes}}

    def __delete_entry__(self):
        # Proceso
        dlgid = self.d_input_api.get('DLGID','')
        result = self.rh.delete_entry(dlgid)  # True/False
        self.d_output_api = {'RESULT':result, 'DLGID':dlgid, 'PARAMS':{}}

    def __read_dlgid_from_uid__(self):
        '''
        d_in =  { 'REQUEST':'READ_DLGID_FROM_UID','DLGID':dlgid, 'PARAMS': {'UID':uid } }
        d_out = {'RESULT':?, 'DLGID':?, 'PARAMS':{'DLGID':?}}
        Si no lo encuentra retorna True, 00000
        '''
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('UID',) )
        if res:
            # Proceso
            uid = self.d_input_api.get('PARAMS',{}).get('UID','0123456789')
            dlgid = self.rh.get_dlgid_from_uid(uid)
            self.d_output_api = {'RESULT':True, 'DLGID':'00000', 'PARAMS':{'DLGID':dlgid}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}

    def __set_dlgid_uid__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('UID',) )
        dlgid = self.d_input_api.get('DLGID','00000')
        uid = self.d_input_api.get('PARAMS',{}).get('UID','0123456789')
        if res:
            _ = self.rh.set_dlgid_uid(dlgid, uid)  # True/False
            self.d_output_api = {'RESULT': True, 'DLGID':'00000', 'PARAMS':{'DLGID':dlgid, 'UID':uid}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}

    def __save_stats__(self):
        '''Chequeo parametros particulares'''
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('D_STATS',) )
        if res:
            # Proceso
            d_stats = self.d_input_api.get('PARAMS',{}).get('D_STATS',{})
            # No controlo errores
            _ = self.rh.save_stats( d_stats ) # True/False
            self.d_output_api = {'RESULT': True, 'DLGID':'00000', 'PARAMS':{}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}

    def __save_frame_timestamp__(self):
        '''Chequeo parametros particulares'''
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('TIMESTAMP',) )
        if res:
            # Proceso
            dlgid = self.d_input_api.get('DLGID','00000')
            timestamp = self.d_input_api.get('PARAMS',{}).get('TIMESTAMP', '')
            print(f'DEBUG:{dlgid},{timestamp}')
            # No controlo errores
            res = self.rh.save_frame_timestamp( dlgid, timestamp ) # True/False
            if not res:
                self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{}} 
                return   
            self.d_output_api = {'RESULT': True, 'DLGID':'00000', 'PARAMS':{}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}

    def __get_queue_length__(self):
        '''Leo el largo de la cola'''
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('QNAME',) )
        if res:
            # Proceso
            queue_name  = self.d_input_api.get('PARAMS',{}).get('QNAME',{})
            # No controlo errores
            queue_length = self.rh.get_queue_length(queue_name) # True/False
            self.d_output_api = {'RESULT': True, 'DLGID':'00000', 'PARAMS':{'QUEUE_LENGTH':queue_length} }
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}

    def __delete_queue__(self):
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('QNAME',) )
        if res:
            # Proceso
            queue_name  = self.d_input_api.get('PARAMS',{}).get('QNAME',{})
            # No controlo errores
            _ = self.rh.delete_queue(queue_name) # True/False
            self.d_output_api = {'RESULT': True, 'DLGID':'00000', 'PARAMS':{} }
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}


class BdRedis:
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
        spc_stats.inc_count_accesos_REDIS()

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
        ordenes = self.rh.hget(dlgid, 'ORDENES' )
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

    def get_queue_length(self, queue_name='STATS_QUEUE')->int:
        ''' Retorna el largo de la cola
            RETURN: largo o -1 (error)
        '''
        if not self.connect():
            return -1
        return self.rh.llen(queue_name)

    def save_stats(self, d_stats:dict)->bool:
        ''' Guarda un registro de estadisticas para Grafana 
            RETURN: True/False
        '''
        if not self.connect():
            return False
        pkdict = pickle.dumps(d_stats)
        self.rh.rpush('STATS_QUEUE', pkdict)
        return True

    def save_frame_timestamp(self, dlgid, timestamp ):
        ''' Guarda un registro de estadisticas para ver que equipos estan caidos. 
            RETURN: True/False
        '''
        if not self.connect():
            return False
        pktimestamp = pickle.dumps(timestamp)
        self.rh.hset('COMMS_STATUS', dlgid, pktimestamp )
        return True

    def delete_queue(self, queue_name='STATS_QUEUE')->bool:
        if not self.connect():
            return False
        _=self.rh.delete(queue_name)
        return True


class TestApiRedis:

    def __init__(self):
        self.d_conf = {}
        self.api = ApiRedis()
        self.dlgid = ''

    def test_read_config(self):
        
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)
        d_request ={'REQUEST':'READ_CONFIG', 'DLGID':self.dlgid, 'PARAMS':{} }
        print('* READ_CONFIG...')  
        d_response = self.api.process(d_request)
        d_conf = d_response.get('PARAMS',{}).get('D_CONF',{})
        if ('BASE', 'FIRMWARE') in d_conf:
            self.d_conf = d_conf
            print('TEST API OK')
        else:
            print('TEST API FAIL')

    def test_delete_entry(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)

        d_request = {'REQUEST':'DELETE_ENTRY', 'DLGID': self.dlgid, 'PARAMS': {} }
        print('* DELETE_ENTRY...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')

    def test_set_config(self):
        self.d_conf = {('BASE', 'ALMLEVEL'): '10', ('BASE', 'BAT'): 'ON', ('BASE', 'COMMITED_CONF'): '0', ('BASE', 'DIST'): '0', 
                       ('BASE', 'FIRMWARE'): '4.0.0a', ('BASE', 'HW_CONTADORES'): 'OPTO', ('BASE', 'IMEI'): '860585007136848', 
                       ('BASE', 'IPADDRESS'): '0.0.0.0', ('BASE', 'PWRS_HHMM1'): '1800', ('BASE', 'PWRS_HHMM2'): '1440', 
                       ('BASE', 'PWRS_MODO'): '0', ('BASE', 'RESET'): '0', ('BASE', 'SAMPLES'): '1', 
                       ('BASE', 'SIMID'): '8959801615239182186F', ('BASE', 'TDIAL'): '900', ('BASE', 'TIMEPWRSENSOR'): '5', 
                       ('BASE', 'TPOLL'): '30', ('BASE', 'UID'): '42125128300065090117010400000000', ('BAT', 'NAME'): 'bt', 
                       ('BAT', 'OFFSET'): '0', ('C0', 'EDGE'): 'RISE', ('C0', 'MAGPP'): '0.01', ('C0', 'NAME'): 'q0', 
                       ('C0', 'OFFSET'): '0', ('C0', 'PERIOD'): '100', ('C0', 'PWIDTH'): '10', ('C0', 'SPEED'): 'LS', 
                       ('A0', 'IMAX'): '20', ('A0', 'IMIN'): '4', ('A0', 'MMAX'): '10', ('A0', 'MMIN'): '0', 
                       ('A0', 'NAME'): 'HTQ', ('A0', 'OFFSET'): '0', ('A1', 'IMAX'): '20', ('A1', 'IMIN'): '4', 
                       ('A1', 'MMAX'): '10', ('A1', 'MMIN'): '0', ('A1', 'NAME'): 'X', ('A1', 'OFFSET'): '0', 
                       ('M2', 'ADDR'): '2070', ('M2', 'FCODE'): '3', ('M2', 'NAME'): 'QS', ('M2', 'SIZE'): '2', 
                       ('M2', 'TIPO'): 'float', ('M0', 'ADDR'): '2069', ('M0', 'CODEC'): 'C1032', ('M0', 'FCODE'): '3', 
                       ('M0', 'MMAX'): '10', ('M0', 'MMIN'): '0', ('M0', 'NAME'): 'AI0', ('M0', 'NRO_RECS'): '2', 
                       ('M0', 'POW10'): '0', ('M0', 'REG_ADDR'): '2069', ('M0', 'SIZE'): '2', ('M0', 'SLA_ADDR'): '2', 
                       ('M0', 'TIPO'): 'FLOAT', ('M0', 'TYPE'): 'FLOAT', ('M1', 'ADDR'): '2069', ('M1', 'CODEC'): 'C1032', 
                       ('M1', 'FCODE'): '3', ('M1', 'MMAX'): '10', ('M1', 'MMIN'): '0', ('M1', 'NAME'): 'AI0', ('M1', 'NRO_RECS'): '2', 
                       ('M1', 'POW10'): '0', ('M1', 'REG_ADDR'): '2069', ('M1', 'SIZE'): '2', ('M1', 'SLA_ADDR'): '2', 
                       ('M1', 'TIPO'): 'FLOAT', ('M1', 'TYPE'): 'FLOAT'}
        
        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'SET_CONFIG', 'DLGID':self.dlgid, 'PARAMS': {'D_CONF':self.d_conf}}
        print('* SET_CONFIG...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')

    def test_get_debug_dlgid(self):
        d_request = {'REQUEST':'READ_DEBUG_DLGID', 'DLGID':'00000','PARAMS':{} }
        print('* READ_DEBUG_DLGID...')  
        d_response = self.api.process(d_request)
        debug_dlgid = d_response.get('PARAMS',{}).get('DEBUG_DLGID','00000')
        print(f'OK {debug_dlgid}')
  
    def test_save_dataline(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'SAVE_DATA_LINE', 'DLGID':'PABLO_TEST', 'PARAMS': { 'D_PAYLOAD' : { 'PA':1.23,'PB':4.56, 'bt':12.65 }}}
        print('* SAVE_DATA_LINE...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print('TEST API OK')
        else:
            print('TEST API FAIL')

    def test_read_dlgid_from_uid(self):

        d_request = {'REQUEST':'READ_DLGID_FROM_UID', 'PARAMS': {'UID':'0123456789' }}
        print('* READ_DLGID_FROM_UID...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        dlgid = d_response.get('DLGID','11111')
        print(f'OK {dlgid}')

    def test_get_ordenes(self):

        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'GET_ORDENES', 'DLGID':self.dlgid, 'PARAMS': { }}
        print('* GET_ORDENES Start...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            orden = d_response.get('PARAMS',{}).get('ORDENES','')
            print(f'TEST API OK {orden}')
        else:
            print('TEST API FAIL')
        print('* GET_ORDENES End...') 

    def test_get_queue_length(self):
        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'GET_QUEUE_LENGTH', 'DLGID':self.dlgid, 'PARAMS': {'QNAME':'STATS_QUEUE' }}
        print('* GET_QUEUE_LENGTH Start...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print(f'TEST API OK')
        else:
            print('TEST API FAIL')
        print('* GET_QUEUE_LENGTH End...') 
    
    def test_delete_queue(self):
        self.dlgid = 'PABLO_TEST'
        set_debug_dlgid(self.dlgid)
        d_request = {'REQUEST':'DELETE_QUEUE', 'DLGID':self.dlgid, 'PARAMS': {'QNAME':'QUEUE_NEW' }}
        print('* DELETE_QUEUE Start...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print(f'TEST API OK')
        else:
            print('TEST API FAIL')
        print('* DELETE_QUEUE End...') 

    def test_save_frame_timestamp(self):
        self.dlgid = 'PABLO'
        set_debug_dlgid(self.dlgid)

        d_request = {'REQUEST':'SAVE_FRAME_TIMESTAMP', 'DLGID':self.dlgid, 'PARAMS': {'TIMESTAMP': dt.datetime.now() }}
        print('* SAVE_FRAME_TIMESTAMP Start...')  
        d_response = self.api.process(d_request)
        rsp = d_response.get('RESULT',False)
        if rsp:
            print(f'TEST API OK')
        else:
            print('TEST API FAIL')
        print('* SAVE_FRAME_TIMESTAMP End...') 

if __name__ == "__main__":

    config_logger('CONSOLA')

    # Test api_redis
    print('TESTING API REDIS...')
    test_api_redis = TestApiRedis()
    #test_api_redis.test_read_config()
    #test_api_redis.test_delete_entry()
    #test_api_redis.test_set_config()
    #test_api_redis.test_get_debug_dlgid()
    #test_api_redis.test_save_dataline()
    #test_api_redis.test_read_dlgid_from_uid()
    #test_api_redis.test_set_dlgid_uid()
    #test_api_redis.test_get_ordenes()
    #test_api_redis.test_get_queue_length()
    #test_api_redis.test_delete_queue()
    test_api_redis.test_save_frame_timestamp()

    sys.exit(0)
