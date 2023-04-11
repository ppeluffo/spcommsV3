#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

'''
Funciones de API de configuracion.
Se utilizan para leer configuracion de equipos de una BD persistente (GDA)
'''

import os
import sys
from sqlalchemy import create_engine
from sqlalchemy import text
import random

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
pparent = os.path.dirname(parent)
sys.path.append(pparent)

from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log2, set_debug_dlgid
from FUNCAUX.UTILS.spc_utils import trace, check_particular_params

# ------------------------------------------------------------------------------

class ApiBdSql:
    '''
    Interface con la BD SQL.
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
        self.pgh = __BdPgSql__()
        self.callback_functions =  { 'READ_CONFIG': self.__read_config__, 
                                     'READ_DLGID_FROM_UID': self.__read_dlgid_from_ui__,
                                    }

    def process(self, d_input:dict):
        #
        self.d_input_api = d_input
        # Chequeo parametros de entrada
        tag = random.randint(0,1000)
        trace(self.d_input_api, f'Input API Sql({tag})')
        #
        self.cbk_request = self.d_input_api.get('REQUEST','')
        # Ejecuto la funcion de callback
        if self.cbk_request in self.callback_functions:
            self.callback_functions[self.cbk_request]()  
        #
        trace(self.d_output_api, f'Output API Sql ({tag})')
        return self.d_output_api
   
    def __read_config__(self):
        dlgid = self.d_input_api.get('DLGID','')
        d_conf = self.pgh.get_config(dlgid)
        self.d_output_api = {'RESULT':True, 'DLGID':dlgid, 'PARAMS': { 'D_CONF': d_conf }}

    def __read_dlgid_from_ui__(self):
        # Chequeo parametros particulares
        res, str_error = check_particular_params(self.d_input_api['PARAMS'], ('UID',) )
        if res:
            # Proceso
            uid = self.d_input_api['PARAMS']['UID']
            dlgid = self.pgh.get_dlgid_from_uid(uid)
            self.d_output_api = {'RESULT':True, 'DLGID':'00000', 'PARAMS':{'DLGID':dlgid}}
        else:
            self.d_output_api = {'RESULT': False, 'DLGID':'00000', 'PARAMS':{'ERROR':str_error}}

class __BdPgSql__:

    def __init__(self):
        self.connected = False
        self.conn = None
        self.handle = None
        self.engine = None
        self.url = Config['BDATOS']['url_gda_spymovil']
        self.response = False

    def connect(self):
        if self.connected:
            return True
        # Engine
        try:
            self.engine = create_engine(self.url)
        except Exception as err_var:
            self.connected = False
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':'ERROR: engine NOT created. ABORT !!' } )
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':f'ERROR: EXCEPTION {err_var}' } )
            return False
        #
        # Connection
        try:
            self.conn = self.engine.connect()
            self.connected = True
            return True
        except Exception as err_var:
            self.connected = False
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':'ERROR: BDSPY NOT connected. ABORT !!' } )
            log2( { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR', 'MSG':f'ERROR: EXCEPTION {err_var}' } )
            return False

    def exec_sql(self, dlgid:str, sql:str):
        '''
        Ejecuta la orden sql.
        Retorna un resultProxy o None
        '''
        if not self.connect():
            log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'ERROR', 'DLGID': dlgid,
                   'MSG':'ERROR: No hay conexion a BD. Exit !!' } )
            return None

        try:
            query = text(sql)
        except Exception as err_var:
            log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'ERROR', 
                   'DLGID':dlgid, 'MSG':f'ERROR: SQLQUERY: {sql}' } )
            log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'ERROR', 
                   'DLGID':dlgid, 'MSG':f'ERROR: EXCEPTION {err_var}' } )
            return

        log2( { 'MODULE':__name__, 'FUNTION':'exec_sql', 'LEVEL':'SELECT', 
                   'DLGID':dlgid, 'MSG':f'QUERY={query}' } )
        rp = None
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            return None
        #
        return rp

    def get_config(self, dlgid:str)->dict:
        '''
        Leo la configuracion desde GDA
                +----------+---------------+------------------------+----------+
                | canal    | parametro     | value                  | param_id |
                +----------+---------------+------------------------+----------+
                | BASE     | RESET         | 0                      |      899 |
                | BASE     | UID           | 304632333433180f000500 |      899 |
                | BASE     | TPOLL         | 60                     |      899 |
                | BASE     | COMMITED_CONF |                        |      899 |
                | BASE     | IMEI          | 860585004331632        |      899 |

                EL diccionario lo manejo con 2 claves para poder usar el metodo get y tener
                un valor por default en caso de que no tenga alguna clave
        '''
        sql = """SELECT spx_unidades_configuracion.nombre as canal, spx_configuracion_parametros.parametro, 
                    spx_configuracion_parametros.value, spx_configuracion_parametros.configuracion_id as \"param_id\" FROM spx_unidades,
                    spx_unidades_configuracion, spx_tipo_configuracion, spx_configuracion_parametros 
                    WHERE spx_unidades.id = spx_unidades_configuracion.dlgid_id 
                    AND spx_unidades_configuracion.tipo_configuracion_id = spx_tipo_configuracion.id 
                    AND spx_configuracion_parametros.configuracion_id = spx_unidades_configuracion.id 
                    AND spx_unidades.dlgid = '{}'""".format(dlgid)
        
        rp = self.exec_sql(dlgid, sql)
        try:
            results = rp.fetchall()
        except AttributeError as ax:
            log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR', 'DLGID':dlgid, 'MSG':f'ERROR: AttributeError fetchall: {ax}' } )
            return None
        except Exception as ex:  # good idea to be prepared to handle various fails
            log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'ERROR', 'DLGID':dlgid, 'MSG':f'ERROR: Exception fetchall: {ex}' } )
            return None

        d = {}
        log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'SELECT', 'DLGID':dlgid, 'MSG':'Reading conf from GDA' } )
        for row in results:
            canal, pname, value, *pid = row
            d[(canal, pname)] = value
            log2( { 'MODULE':__name__, 'FUNTION':'get_config', 'LEVEL':'SELECT', 'DLGID':dlgid, 'MSG':f'BD conf: [{canal}][{pname}]=[{d[(canal, pname)]}]' } )
        return d

    def get_dlgid_from_uid(self, uid:str)->str:
        '''
        Consulta en GDA con clave UID para encontrar el DLGID correspondiente
        '''
        sql = f"""SELECT u.dlgid FROM spx_configuracion_parametros AS cp 
            INNER JOIN spx_unidades_configuracion AS uc ON cp.configuracion_id = uc.id 
            INNER JOIN spx_unidades AS u ON u.id = uc.dlgid_id 
            WHERE uc.nombre = 'BASE' AND cp.parametro = 'UID' AND value = '{uid}';"""

        #
        rp = self.exec_sql('DEFAULT', sql)
        try:
            results = rp.fetchall()
        except AttributeError as ax:
            log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'ERROR', 'MSG':f'ERROR: AttributeError fetchall: {ax}' } )
            return '00000'
        except Exception as ex:  # good idea to be prepared to handle various fails
            log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'ERROR', 'MSG':f'ERROR: Exception fetchall: {ex}' } )
            return '00000'

        log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'INFO', 'MSG':'Reading dlgid_from_uid in SQL.' } )
        if len(results) == 0:
            return '00000'
            
        dlgid = results[0][0]
        log2( { 'MODULE':__name__, 'FUNTION':'get_dlgid_from_uid', 'LEVEL':'INFO', 'MSG':f'UID:{uid}, DLGID:{dlgid}' } )
        return dlgid
