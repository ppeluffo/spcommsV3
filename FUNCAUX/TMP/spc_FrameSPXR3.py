#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
"""
Esta clase está especializada en los frames de datos de los dataloggers SPX2022 version 3.
Esta version modifica los mensajes para usar los delimitadores estandard de HTTP
Estos dataloggers transmiten via modems USR-IOT por medio de GET.
Hay 5 tipos de frames que se diferencian por el parametro CLASS
 RECOVER:       ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000
 CONF_BASE:     ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11
 CONF_AINPUTS:  ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01           
 CONF_COUNTERS: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86
 CONF_MODBUS:   ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86
 DATA:          ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496
 PING:          ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING

Los frames de Recover, conf_base, conf_ainputs,conf_counters mandan un checksum de la respectiva
configuracion local.

Modo de testing:
telnet localhost 80
GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING
HTTP/1.1
Host: www.spymovil.com

telnet localhost 80
GET /cgi-bin/spcommsV3/spcommsV3.py?ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86
HTTP/1.1
Host: www.spymovil.com

GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496
HTTP/1.1
Host: www.spymovil.com

"""

import datetime as dt
import sys
import urllib.request
from FUNCAUX.UTILS.spc_log import log2
from FUNCAUX.BD.spc_bd_redis_local import BdRedisLocal
from FUNCAUX.UTILS.spc_utils import u_hash, version2int
from FUNCAUX.APIS.api_redis import api_configuracion, api_bddatos

# ------------------------------------------------------------------------------

def get_dlg_config(dlgid):
    '''
    Verifica si existe en la redis local un registro con configuracion del 
    datalogger dlgid.
    Si existe devuelve un diccionario con la configuracion
    Si NO existe, por medio de la API lee la configuracion del modulo de configuraciones.
    Si este le devuelve un d_conf, actualiza su registro en la redis local y los
    devuelve. Si no retorna None
    '''
    redis_local_handle = BdRedisLocal()
    d_conf = redis_local_handle.get_local_dlg_config(dlgid)
    if d_conf is None:
        d_params = {'SERVICE':'GET_CONFIG', 'DLGID':dlgid }
        d_conf = api_configuracion(d_params)
        d_log = { 'MODULE':__name__, 'FUNTION':'get_dlg_config', 'LEVEL':'SELECT',
                 'DLGID':dlgid, 'MSG':f'CHECKING API_CONF {dlgid}' }
        log2(d_log)
        #
        if d_conf is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'get_dlg_config', 'LEVEL':'ERROR',
                      'MSG':f'NO EXISTE CONFIGURACION DE {dlgid}' }
            log2(d_log)
            return None
        #
        # Serializo y guardo la configuracion en la redis local.
        if redis_local_handle.set_local_dlg_config(dlgid, d_conf):
            return d_conf
        #
        return None
    #
    return d_conf

def calcular_hash_config_base(d_params):
    '''
    Calcula el hash de la configuracion base.
    '''
    # dlgid=d_params['DLGID']
    version=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    xhash = 0
    timerpoll = int(d_conf.get(('BASE', 'TPOLL'),'0'))
    timerdial = int(d_conf.get(('BASE', 'TDIAL'),'0'))
    pwr_modo = int(d_conf.get(('BASE', 'PWRS_MODO'),'0'))
    pwr_hhmm_on = int(d_conf.get(('BASE', 'PWRS_HHMM1'),'601'))
    pwr_hhmm_off = int(d_conf.get(('BASE', 'PWRS_HHMM2'),'2201'))
    #
    hash_str = f'[TIMERPOLL:{timerpoll:03d}]'
    xhash = u_hash(xhash, hash_str)
    #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
    #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
    #log2(d_log)
    #
    hash_str = f'[TIMERDIAL:{timerdial:03d}]'
    xhash = u_hash(xhash, hash_str)
    #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
    #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
    #log2(d_log)
    #
    hash_str = f'[PWRMODO:{pwr_modo}]'
    xhash = u_hash(xhash, hash_str)
    #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
    #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
    #log2(d_log)
    #
    hash_str = f'[PWRON:{pwr_hhmm_on:04}]'
    xhash = u_hash(xhash, hash_str)
    #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
    #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
    #log2(d_log)
    #
    hash_str = f'[PWROFF:{pwr_hhmm_off:04}]'
    xhash = u_hash(xhash, hash_str)
    #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
    #        'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
    #log2(d_log)
    #
    # A partir de la version 105 incorporamos 'samples'y almlevel'
    fw_version = version2int(version)
    if fw_version >= 105:
        samples = int(d_conf.get(('BASE', 'SAMPLES'),'1'))
        almlevel = int(d_conf.get(('BASE', 'ALMLEVEL'),'0'))
        hash_str = f'[SAMPLES:{samples:02}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #    'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
        #
        hash_str = f'[ALMLEVEL:{almlevel:02}]'
        xhash = u_hash(xhash, hash_str)
        #d_log = { 'MODULE':__name__, 'FUNCTION':'calcular_hash_config_base', 'LEVEL':'ERROR',
        #    'DLGID': d_params['DLGID'], 'MSG':f'DEBUG: HASH_BASE: hash_str=<{hash_str}>, hash={xhash}' }
        #log2(d_log)
    #
    return xhash

def get_response_base(d_params):
    '''
    Calcula la respuesta de configuracion base
    '''
    # dlgid=d_params['DLGID']
    version=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    timerpoll = int(d_conf.get(('BASE', 'TPOLL'),'0'))
    timerdial = int(d_conf.get(('BASE', 'TDIAL'),'0'))
    pwr_modo = int(d_conf.get(('BASE', 'PWRS_MODO'),'0'))
    pwr_hhmm_on = int(d_conf.get(('BASE', 'PWRS_HHMM1'),'601'))
    pwr_hhmm_off = int(d_conf.get(('BASE', 'PWRS_HHMM2'),'2201'))
    if pwr_modo == 0:
        s_pwrmodo = 'CONTINUO'
    elif pwr_modo == 1:
        s_pwrmodo = 'DISCRETO'
    else:
        s_pwrmodo = 'MIXTO'
    #
    response = 'CLASS=CONF_BASE&'
    response += f'TPOLL={timerpoll}&TDIAL={timerdial}&PWRMODO={s_pwrmodo}&PWRON={pwr_hhmm_on:04}&PWROFF={pwr_hhmm_off:04}'
    #
    fw_version = version2int(version)
    if fw_version >= 105:
        samples = int(d_conf.get(('BASE', 'SAMPLES'),'1'))
        almlevel = int(d_conf.get(('BASE', 'ALMLEVEL'),'0'))
        response += f'&SAMPLES={samples}&ALMLEVEL={almlevel}'
    #    
    return response

def calcular_hash_config_ainputs(d_params):
    '''
    Calcula el hash de la configuracion de canales analógicos.
    '''
    # dlgid=d_params['DLGID']
    # fwversion=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    xhash = 0
    for channel in ['A0','A1','A2']:
        if (channel,'NAME') in d_conf.keys():      # ( 'A1', 'NAME' ) in  d.keys()
            name=d_conf.get((channel, 'NAME'),'X')
            imin=int(d_conf.get((channel, 'IMIN'),'0'))
            imax=int(d_conf.get((channel, 'IMAX'),'0'))
            mmin=float(d_conf.get((channel, 'MMIN'),'0'))
            mmax=float(d_conf.get((channel, 'MMAX'),'0'))
            offset=float(d_conf.get((channel, 'OFFSET'),'0'))
            hash_str = f'[{channel}:{name},{imin},{imax},{mmin:.02f},{mmax:.02f},{offset:.02f}]'
        else:
            hash_str = f'[{channel}:X,4,20,0.00,10.00,0.00]'
        #
        xhash = u_hash(xhash, hash_str)
    return xhash

def get_response_ainputs(d_params):
    '''
    Calcula la respuesta de configuracion de canales analogicos
    '''
    # dlgid=d_params['DLGID']
    # fwversion=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    response = 'CLASS=CONF_AINPUTS&'
    for channel in ('A0','A1','A2'):
        name = d_conf.get((channel, 'NAME'), 'X')
        imin = int(d_conf.get((channel, 'IMIN'), 4))
        imax = int(d_conf.get((channel, 'IMAX'), 20))
        mmin = float(d_conf.get((channel, 'MMIN'), 0.00))
        mmax = float(d_conf.get((channel, 'MMAX'), 10.00))
        offset = float(d_conf.get((channel, 'OFFSET'), 0.00))
        response += f'{channel}={name},{imin},{imax},{mmin},{mmax},{offset}&'
    #
    response = response[:-1]
    return response

def calcular_hash_config_counters(d_params):
    '''
    Calcula el hash de la configuracion de canales digitales ( contadores )
    '''
    # dlgid=d_params['DLGID']
    # fwversion=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    xhash = 0
    for channel in ['C0','C1']:
        if (channel,'NAME') in d_conf.keys():      # ( 'C0', 'NAME' ) in  d.keys()
            name=d_conf.get((channel, 'NAME'),'X')
            str_modo = d_conf.get((channel, 'MODO'),'CAUDAL')
            if str_modo == 'CAUDAL':
                modo = 0    # caudal
            else:
                modo = 1    # pulsos
            magpp=float(d_conf.get((channel, 'MAGPP'),'0'))
            hash_str = f'[{channel}:{name},{magpp:.03f},{modo}]'
        else:
            hash_str = f'[{channel}:X,1.000,0]'
        #
        xhash = u_hash(xhash, hash_str)
    return xhash

def get_response_counters(d_params):
    '''
    Calcula la respuesta de configuracion de canales contadores
    '''
    # dlgid=d_params['DLGID']
    # fwversion=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    response = 'CLASS=CONF_COUNTERS&'
    for channel in ('C0','C1'):
        name = d_conf.get((channel, 'NAME'), 'X')
        magpp = float(d_conf.get((channel, 'MAGPP'), 1.00))
        str_modo = d_conf.get((channel, 'MODO'),'CAUDAL')
        response += f'{channel}={name},{magpp},{str_modo}&'
    #
    response = response[:-1]
    return response

def calcular_hash_config_modbus(d_params):
    '''
    Calcula el hash de la configuracion de canales modbus
    '''
    # dlgid=d_params['DLGID']
    # fwversion=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    xhash = 0
    for channel in ['M0','M1','M2','M3','M4']:
        if (channel,'NAME') in d_conf.keys():      # ( 'A1', 'NAME' ) in  d.keys()
            name=d_conf.get((channel, 'NAME'),'X')
            sla_addr=int(d_conf.get((channel, 'SLA_ADDR'),'0'))
            reg_addr=int(d_conf.get((channel, 'ADDR'),'0'))
            nro_regs=int(d_conf.get((channel, 'NRO_RECS'),'0'))
            fcode=int(d_conf.get((channel, 'FCODE'),'0'))
            mtype=d_conf.get((channel, 'TYPE'),'U16')
            codec=d_conf.get((channel, 'CODEC'),'C0123')
            pow10=int(d_conf.get((channel, 'POW10'),'0'))
            #
            hash_str = f'[{channel}:{name},{sla_addr:02d},{reg_addr:04d},{nro_regs:02d},{fcode:02d},{mtype},{codec},{pow10:02d}]'
        else:
            hash_str = f'[{channel}:X,00,0000,00,00,U16,C0123,00]'
        #
        xhash = u_hash(xhash, hash_str)
    return xhash

def get_response_modbus(d_params):
    '''
    Calcula la respuesta de configuracion de canales modbus
    '''
    # dlgid=d_params['DLGID']
    # fwversion=d_params['VERSION']
    d_conf=d_params['LOCAL_CONF']
    #
    response = 'CLASS=CONF_MODBUS&'
    for channel in ['M0','M1','M2','M3','M4']:
        name=d_conf.get((channel, 'NAME'),'X')
        sla_addr=int(d_conf.get((channel, 'SLA_ADDR'),'0'))
        reg_addr=int(d_conf.get((channel, 'ADDR'),'0'))
        nro_regs=int(d_conf.get((channel, 'NRO_RECS'),'0'))
        fcode=int(d_conf.get((channel, 'FCODE'),'0'))
        mtype=d_conf.get((channel, 'TYPE'),'U16')
        codec=d_conf.get((channel, 'CODEC'),'C0123')
        pow10=int(d_conf.get((channel, 'POW10'),'0'))
        response += f'{channel}={name},{sla_addr},{reg_addr},{nro_regs},{fcode},{mtype},{codec},{pow10}&'
    #
    response = response[:-1]
    return response

def update_dlg_dataline(d_params):
    '''
    Serializa los datos y los guarda en la key PKLINE
    '''
    redis_local_handle = BdRedisLocal()
    redis_local_handle.update_local_dlg_dataline(d_params)

def get_orders_to_dlg(dlgid):
    '''
    Lee las ordenes y arma un string para devolver
    Si alguna orden es RESET, borra el registro de REDIS
    '''
    redis_local_handle = BdRedisLocal()
    orders_line = redis_local_handle.get_orders(dlgid)
    if orders_line is None:
        return None
    #
    if 'RESET' in orders_line:
        redis_local_handle.delete_entry(dlgid)
    return orders_line

def testing(d_params):
    '''
    Funcion usada para probar los metodos de la clase implementada
    '''
    url = f"http://127.0.0.1:80/cgi-bin/spcomms/spcommsV3.py?{d_params['URL']}"
    title = d_params['TITLE']
    #
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    print(f'{title}...')
    print(response.read())

class FrameSpxR3():
    '''
    En el __init__ de BASE_frame tomo el dict d que tiene {'QUERY_STRING', 'ID', 'VER', 'TYPE', 'PAYLOAD'}
    Hago un overload del 'process'
    '''
    def __init__(self, d_qs):
        '''
        Las key del d son:
        [QUERY_STRING, ID, TYPE, VER, PAYLOAD ]
        d['PAYLOAD'] es todo lo que tiene que ver con los datos que envia el datalogger.
        Siempre son campos definidos por KEY:VALUE;
        '''
        self.d_qs = d_qs
        self.dlgid = None
        self.version = None
        self.d_payload = None
        self.response = ''
        d_log = { 'MODULE':__name__, 'FUNCTION':'Class FrameSpxR2', 'LEVEL':'ERROR',
                 'DLGID':self.dlgid, 'MSG':'init' }
        log2(d_log)

    def process(self):
        '''
        Procesamiento del frame tipo SPXR2 
        '''
        self.dlgid = self.d_qs.get('GET',{}).get('ID',['00000'])[0]
        self.version = self.d_qs.get('GET',{}).get('VER',['0.0.0'])[0]
        #
        clase = self.d_qs.get('GET',{}).get('CLASS',['UNKNOWN'])[0]
        #
        d_log = { 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'ERROR',
                 'DLGID':self.dlgid, 'MSG':f'CLASS={clase},DLGID={self.dlgid}, VERSION={self.version}' }
        log2(d_log)

        if clase == 'PING':
            self.response = self.process_frame_ping()
        elif clase == 'RECOVER':
            self.response = self.process_frame_recoverid()
        elif clase == 'CONF_BASE':
            self.response = self.process_frame_config_base()
        elif clase == 'CONF_AINPUTS':
            self.response = self.process_frame_config_ainputs()
        elif clase == 'CONF_COUNTERS':
            self.response = self.process_frame_config_counters()
        elif clase == 'CONF_MODBUS':
            self.response = self.process_frame_config_modbus()
        elif clase == 'DATA':
            self.response = self.process_frame_data()
        else:
            # Frame no reconocido
            self.response = 'ERROR:UNKNOWN FRAME TYPE'

        #log(module=__name__, function='process', level='SELECT', dlgid=self.dlgid, msg=f'RSP={self.response}')
        self.send_response()
        sys.stdout.flush()
        return self.dlgid, self.response

    def process_frame_ping(self):
        '''
        Funcion usada para indicar que el enlace esta activo
        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING
        HTTP/1.1
        Host: www.spymovil.com
        '''
        response = 'CLASS=PONG'
        return response

    def process_frame_recoverid(self):
        '''
        FRAME: ID:DEFAULT;TYPE:SPXR2;VER:1.0.0;CLASS:RECOVER;UID:42125128300065090117010400000000
        Extrae el UID del payload y consulta a la api de configuracion por el dlgid asociado.  
        Si hay un dlgid asociado, lo envia; si no mando DEFAULT
       
        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000
        HTTP/1.1
        Host: www.spymovil.com
        '''
        d_log = { 'MODULE':__name__, 'FUNTCION':'process_frame_recoverid', 'LEVEL':'SELECT',
                 'DLGID':self.dlgid, 'MSG':'SPXR2_FRAME_RECOVER' }
        log2(d_log)
        uid = self.d_qs.get('GET',{}).get('UID',[None])[0]
        d_params = {'SERVICE':'RECOVERID', 'UID':uid }
        d_rsp = api_configuracion(d_params)
        new_dlgid = d_rsp.get('DLGID', None)
        if new_dlgid is None:
            response = 'CLASS=RECOVER&CONFIG=ERROR'
        else:
            response = f'CLASS=RECOVER&DLGID={new_dlgid}'
        return response

    def process_frame_config_base(self):
        '''
        FRAME ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0x11
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion BASE
        #
        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11
        HTTP/1.1
        Host: www.spymovil.com
        '''
        if self.dlgid == 'DEFAULT':
            response = 'CLASS:CONF_BASE;CONFIG:ERROR'
            return response
        #
        # Verificamos tener una configuracion local valida
        d_local_conf = get_dlg_config(self.dlgid)
        if d_local_conf is None:
            response = 'CLASS=CONF_BASE&CONFIG=ERROR'
            return response
        #
        # Proceso el frame
        d_params = {'DLGID':self.dlgid, 'VERSION': self.version, 'LOCAL_CONF':d_local_conf}
        bd_hash = calcular_hash_config_base(d_params)
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash =  int( self.d_qs.get('GET',{}).get('HASH',[0])[0], 16) 
        #
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_base', 'LEVEL':'SELECT',
                 'DLGID':self.dlgid, 'MSG':f'BASE: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        #
        if dlg_hash == bd_hash:
            response = 'CLASS=CONF_BASE&CONFIG=OK'
        else:
            response = get_response_base(d_params)
        return response

    def process_frame_config_ainputs(self):
        '''
        FRAME ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x01
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion CONF_AINPUTS

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01
        HTTP/1.1
        Host: www.spymovil.com
        '''
        #
        if self.dlgid == 'DEFAULT':
            response = 'CLASS=CONF_AINPUTS&CONFIG=ERROR'
            return response
        #
        # Verificamos tener una configuracion local valida
        d_local_conf = get_dlg_config(self.dlgid)
        if d_local_conf is None:
            response = 'CLASS=CONF_AINPUTS&CONFIG=ERROR'
            return response
        #
        # Proceso el frame
        d_params = {'DLGID':self.dlgid, 'VERSION': self.version, 'LOCAL_CONF':d_local_conf}
        bd_hash = calcular_hash_config_ainputs(d_params)
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash =  int( self.d_qs.get('GET',{}).get('HASH',[0])[0], 16)
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_ainputs', 'LEVEL':'SELECT',
                 'DLGID':self.dlgid, 'MSG':f'AINPUTS: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        if dlg_hash == bd_hash:
            response = 'CLASS=CONF_AINPUTS&CONFIG=OK'
        else:
            response = get_response_ainputs(d_params)
        return response

    def process_frame_config_counters(self):
        '''
        FRAME ID:DEFAULT;TYPE:SPX;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x86
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion CONF_COUNTERS

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86
        HTTP/1.1
        Host: www.spymovil.com
        '''
        #
        if self.dlgid == 'DEFAULT':
            response = 'CLASS=CONF_COUNTERS&CONFIG=ERROR'
            return response
        #
        # Verificamos tener una configuracion local valida
        d_local_conf = get_dlg_config(self.dlgid)
        if d_local_conf is None:
            response = 'CLASS=CONF_COUNTERS&CONFIG=ERROR'
            return response
        # Proceso el frame
        d_params = {'DLGID':self.dlgid, 'VERSION': self.version, 'LOCAL_CONF':d_local_conf}
        bd_hash = calcular_hash_config_counters(d_params)
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash =  int( self.d_qs.get('GET',{}).get('HASH',[0])[0], 16)
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_counters', 'LEVEL':'SELECT',
                 'DLGID':self.dlgid, 'MSG':f'COUNTERS: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        if dlg_hash == bd_hash:
            response = 'CLASS=CONF_COUNTERS&CONFIG=OK'
        else:
            response = get_response_counters(d_params)
        return response

    def process_frame_config_modbus(self):
        '''
        FRAME ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_MODBUS;HASH:0x01
        Primero veo que haya un registro en REDIS con una configuracion valida.
        Compara el hash con el que calcula localmente de la configuracion.
        Si coincide devuelve un OK.
        Si no manda la configuracion CONF_MODBUS

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86
        HTTP/1.1
        Host: www.spymovil.com
        '''
        if self.dlgid == 'DEFAULT':
            response = 'CLASS=CONF_MODBUS&CONFIG=ERROR'
            return response
        #
        # Verificamos tener una configuracion local valida
        d_local_conf = get_dlg_config(self.dlgid)
        if d_local_conf is None:
            response = 'CLASS=CONF_MODBUS&CONFIG=ERROR'
            return response
        #
        # Proceso el frame
        d_params = {'DLGID':self.dlgid, 'VERSION': self.version, 'LOCAL_CONF':d_local_conf}
        bd_hash = calcular_hash_config_modbus(d_params)
        # Calculo el hash ( viene en hex por eso lo convierto a decimal )
        dlg_hash =  int( self.d_qs.get('GET',{}).get('HASH',[0])[0], 16)
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_config_modbus', 'LEVEL':'SELECT',
                 'DLGID':self.dlgid, 'MSG':f'MODBUS: dlg_hash={dlg_hash}, bd_hash={bd_hash}' }
        log2(d_log)
        if dlg_hash == bd_hash:
            response = 'CLASS=CONF_MODBUS&CONFIG=OK'
        else:
            response = get_response_modbus(d_params)
        return response
    
    def process_frame_data(self):
        '''
        Agrego el dict_payload al d y encolo.

        Testing:
        GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496
        HTTP/1.1
        Host: www.spymovil.com

        d_qs['PAYLOAD'] = 'CLASS:DATA;A0:0.00;A1:0.00;A2:0.00;C0:0.000;C1:0.000'
        self.dict_payload es el d_qs['PAYLOAD'] parseado
        { 'CLASS':'DATA', 'A0',0.00, 'A1':0.00,......,'C1':0.000 }

        Solo en los modo DATA o BLOCKEND mando respuesta.
        Esto es para acelarar los envios, que el datalogger no espere al servidor y desagote la memoria rapido.
        '''
        # Solo necesito los datos del payload. El resto los descarto
        d_payload = self.d_qs['GET'].copy()
        d_payload.pop('ID', None)
        d_payload.pop('TYPE', None)
        d_payload.pop('VER', None)
        d_payload.pop('CLASS', None)
        # Guardo los datos en la redis local
        d_params = {'DLGID':self.dlgid, 'DATA':d_payload }
        update_dlg_dataline(d_params)
        #
        # Envio a la BD persistente via API.
        d_params = {'SERVICE':'DATA', 'ORIGEN':'SPXR2', 'DLGID':self.dlgid, 'DATOS':self.d_qs['GET'] }
        api_bddatos(d_params)
        # 
        # Preparo la respuesta y transmito. Agrego las órdenes que estan en la redis local.
        #log(module=__name__, function='process', level='ERROR', dlgid=self.dlgid, msg=f'DEBUG process_frame_data1: modo={modo}')
        now=dt.datetime.now().strftime('%y%m%d%H%M')
        response = f'CLASS=DATA&CLOCK={now}'
        # Agrego ordenes que leo del redis local
        orders = get_orders_to_dlg(self.dlgid)
        d_log = { 'MODULE':__name__, 'FUNCTION':'process_frame_data', 'LEVEL':'ERROR',
                 'DLGID':self.dlgid, 'MSG':f'ORDERS={orders}' }
        log2(d_log)
        if orders is not None:
            response += orders
        #
        return response

    def send_response(self):
        '''
        Envia la respuesta html al datalogger
        '''
        print('Content-type: text/html\n\n',end='')
        print(f'<html><body><h1>{self.response}</h1></body></html>')
        d_log = { 'MODULE':__name__,'FUNCTION':'send_response', 'LEVEL':'ERROR',
                 'DLGID':self.dlgid, 'MSG':f'RSP={self.response}' }
        log2(d_log)


# Testeo del modulo
if __name__ == "__main__":
    '''
    Genera todos los frames que el modulo es capaz de procesar.
    CONF_AINPUTS:  ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_AINPUTS&HASH=0x01           
    CONF_COUNTERS: ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_COUNTERS&HASH=0x86
    CONF_MODBUS:   ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_MODBUS&HASH=0x86
    DATA:          ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=DATA&DATE=230321&TIME=094504&A0=0.00&A1=0.00&A2=0.00&C0=0.000&C1=0.000&bt=12.496
    '''
    d_test = {}
    # Frame PING
    d_test['URL'] = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING'
    d_test['TITLE'] = 'Test PING...'
    testing(d_test)
    #
    # Frame RECOVER
    d_test['URL'] = 'ID=DEFAULT&TYPE=SPXR3&VER=1.0.0&CLASS=RECOVER&UID=42125128300065090117010400000000'
    d_test['TITLE'] = 'Test RECOVERID...'
    testing(d_test)
    #
    # Frame CONFIG_BASE
    d_test['URL'] = 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=CONF_BASE&UID=42125128300065090117010400000000&HASH=0x11'
    d_test['TITLE'] = 'Test CONFIG_BASE...'
    testing(d_test)

