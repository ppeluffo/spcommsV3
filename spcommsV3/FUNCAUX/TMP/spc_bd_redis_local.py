#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

'''
La redis local guarda la informacion de configuracion de los equipos,
un pickle con la ultima linea de datos recibida y una linea con 
ordenes.
Las claves para un dlgid dado son: CONFIG, PKLINE, ORDERS
CONFIG: tiene el diccionario con toda la configuracin
PKLINE: tiene un pickle de un diccionario de la ultima linea de datos
ORDERS: string con ordenes ( RESET ) en el formato que acepta el datalogger.

Si bien por ahora solo hay una orden ( RESET ), podrian haber mas por lo tanto
la funcion que actualize ordenes debe incrementar y no sustituir !!
'''

import pickle
import redis
from FUNCAUX.UTILS.spc_config import Config
from FUNCAUX.UTILS.spc_log import log2

class BdRedisLocal:
    '''
    Clase que implenta todas las funciones que se realizan sobre la redis local
    '''
    def __init__(self):
        self.connected = False
        self.handle = None

    def connect(self):
        '''
        Cada acceso primero verifica si esta conectada asi que aqui incrementamos 
        el contador de accesos.
        '''
        if self.connected:
            return True

        host = Config['REDIS_LOCAL']['host']
        port = Config['REDIS_LOCAL']['port']
        dbase = Config['REDIS_LOCAL']['db']
        try:
            self.handle = redis.Redis(host=host, port=port, db=dbase)
            self.connected = True
        except Exception as err_var:
            d_log = { 'MODULE':__name__, 'FUNTION':'connect', 'LEVEL':'ERROR',
                      'MSG':f'Init ERROR host={host}, port={port}, db={dbase} !!' }
            log2(d_log)
            #
            d_log = { 'MODULE':__name__, 'FUNTION':'connect','LEVEL':'ERROR',
                      'MSG':f'EXCEPTION={err_var}' }
            log2(d_log)
            self.connected = False

        return self.connected

    def get_local_dlg_config(self, dlgid):
        '''
        Retorna el diccionario con la configuracion del dlgid o None
        '''
        if dlgid is None:
            d_log = { 'MODULE':__name__, 'FUNTION':'localRedis_get_dlg_config', 'LEVEL':'ERROR',
                      'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return None
        #    
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'localRedis_get_dlg_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid,'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return None
        #
        # Hay registro ( HASH ) del datalogger:
        if self.handle.exists(dlgid):
            pdict = self.handle.hget(dlgid, 'CONFIG')
            if pdict is not None:
                dconf = pickle.loads(pdict)
                return dconf
        #
        return None

    def set_local_dlg_config(self, dlgid, d_conf):
        '''
        Serializa d_conf y crea una entrada del datalogger en la redis local.
        Inicializa el resto de las entradas.
        '''
        if ( dlgid is None )  or ( d_conf is None ):
            d_log = { 'MODULE':__name__, 'FUNTION':'localRedis_set_dlg_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid, 'MSG':'ERROR DE PARAMETROS' }
            log2(d_log)
            return None
        #
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'localRedis_set_dlg_config', 'LEVEL':'ERROR',
                     'DLGID':dlgid, 'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return False
        # 
        pkconf = pickle.dumps(d_conf)
        try:
            self.handle.delete(dlgid)   # Borramos previamente la clave
        except:
            pass
        #
        self.handle.hset(dlgid, 'CONFIG', pkconf)
        self.handle.hset(dlgid, 'VALID', 'TRUE')
        self.handle.hset(dlgid, 'RESET', 'FALSE')
        return True
    
    def update_local_dlg_dataline(self, d_params):
        '''
        Serializa y guarda en la key PKLINE
        d_params = {'DLGID':self.dlgid, 'DATA':self.d_payload }
        '''
        dlgid = d_params['DLGID']
        pkline = pickle.dumps(d_params['DATA'])
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'localredis_update_local_dlg_dataline', 'LEVEL':'ERROR','DLGID':dlgid,
                      'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return
        #
        self.handle.hset(dlgid, 'PKLINE', pkline)
        return
    
    def get_orders(self, dlgid):
        '''
        Todas las ordenes se escriben en la key ORDERS, inclusive el RESET !!
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'localRedis_get_orders', 'LEVEL':'ERROR','DLGID':dlgid,
                      'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return None
        #
        order_str = self.handle.hget(dlgid, 'ORDERS' )
        return order_str

    def delete_entry(self, dlgid):
        '''
        Borra el registro de la redis local !!
        '''
        if not self.connect():
            d_log = { 'MODULE':__name__, 'FUNTION':'localRedis_deleteEntry', 'LEVEL':'ERROR','DLGID':dlgid,
                      'MSG':'REDIS LOCAL NOT CONNECTED !!' }
            log2(d_log)
            return
        #
        try:
            self.handle.delete(dlgid)
        except:
            pass
        