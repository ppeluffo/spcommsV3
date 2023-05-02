#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Proceso en forma lineal la cola de datos RXDATA_QUEUE.
En esta, cada elemento es un pickle de un dict con el formato:
 {'PROTO':protocol, 'DLGID':dlgid, 'D_LINE':d_line}.
El proceso master lee y saca todos los elementos de la cola y
arma 2 listas: uno para SPX y otro para PLC
Pone los elementos correspondientes en c/lista e invoca a un
subproceso para c/u.
Luego duerme 60s.
'''

import time
import signal
import pickle
from multiprocessing import Process
import sys

from FUNCAUX.UTILS.spc_log import config_logger, log2
from FUNCAUX.SERVICIOS import servicios

from sqlalchemy import create_engine
from sqlalchemy import text


class BD_SQL_BASE:

    def __init__(self):
        self.connected = False
        self.conn = None
        self.handle = None
        self.engine = None
        self.url = ""
        self.response = False

    def connect(self):
        if self.connected:
            return True
        # Engine
        try:
            self.engine = create_engine()
        except Exception as err_var:
            self.connected = False
            log2 ({ 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR',
                 'MSG':'ERROR: engine NOT created. ABORT !!'})
            log2 ({ 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR',
                 'MSG':f'EXCEPTION {err_var}'})
            return False
        #
        # Connection
        try:
            self.conn = self.engine.connect()
            self.connected = True
            return True
        except Exception as err_var:
            self.connected = False
            log2 ({ 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR',
                 'MSG':'ERROR: BDSQL NOT connected. ABORT !!'})
            log2 ({ 'MODULE':__name__, 'FUNCTION':'connect', 'LEVEL':'ERROR',
                 'MSG':f'EXCEPTION {err_var}'})
            return False
        #

    def exec_sql(self, sql):
        # Ejecuta la orden sql.
        # Retorna un resultProxy o None

        if not self.connect():
            log2 ({ 'MODULE':__name__, 'FUNCTION':'exec_sql', 'LEVEL':'ERROR',
                 'MSG':'ERROR: No hay conexion a BD. Exit !!'})
            return None

        try:
            query = text(sql)
        except Exception as err_var:
            log2 ({ 'MODULE':__name__, 'FUNCTION':'exec_sql', 'LEVEL':'ERROR',
                 'MSG':'ERROR: SQLQUERY: {sql}'})
            log2 ({ 'MODULE':__name__, 'FUNCTION':'exec_sql', 'LEVEL':'ERROR',
                 'MSG':'ERROR: EXCEPTION: {err_var}'})
            return

        rp = None
        try:
            rp = self.conn.execute(query)
        except Exception as err_var:
            '''
            if 'duplicate'.lower() in (str(err_var)).lower():
                # Los duplicados no hacen nada malo. Se da mucho en testing.
                log(module=__name__, function='exec_sql', level='WARN', dlgid=dlgid, msg='WARN {0}: Duplicated Key'.format(dlgid))
            else:
                log(module=__name__, function='exec_sql', level='ERROR', dlgid=dlgid, msg='ERROR: {0}, EXCEPTION {1}'.format(dlgid, err_var))
            '''
            return None
        return rp

    def insert_raw(self, dlgid, key, value):
        '''
        Ejecuta la iserción raw en la bd
        Retorna un resultProxy o None
        '''
        sql = f'INSERT INTO db_datos ( fechadata,fechasys,equipo,tag,valor) VALUES NOW(),"{dlgid}","{key}","{value}"  ON CONFLICT DO NOTHING' 
        result = self.exec_sql(dlgid, sql)
        if result is None:
            log2 ({ 'MODULE':__name__, 'FUNCTION':'insert_raw', 'LEVEL':'ERROR', 'MSG':f'{dlgid} INSERT BDQSL FAIL !!!'})
            return False
        else:
            log2 ({ 'MODULE':__name__, 'FUNCTION':'insert_raw', 'LEVEL':'SELECT', 'MSG':f'{dlgid} INSERT BDQSL OK'})
            return True

class BD_HISTORICO(BD_SQL_BASE):

    def __init__(self):
        self.url = "postgresql+psycopg2://admin:pexco599@192.168.0.19:5435"

class BD_TEMPORAL(BD_SQL_BASE):

    def __init__(self):
        self.url = "postgresql+psycopg2://admin:pexco599@192.168.0.19:5434"
   
def clt_C_handler(signum, frame):
    sys.exit(0)

def process_frames( protocolo, boundle_list ):
    nro_items = len(boundle_list)
    log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'{protocolo} ITEMS_LENGTH={nro_items}'})
    #
    # Instancio la base.
    bd_historico = BD_HISTORICO()
    bd_temporal = BD_TEMPORAL()
    #
    for d_data in boundle_list:
        dlgid = d_data.get('DLGID','0000')
        d_line = d_data.get('D_LINE',{})
        #log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'{protocolo} D_LINE={d_line}'})
        for key in d_line:
            value = d_line[key]
            msg = f'{protocolo},{dlgid},{key}=>{value}'
            log2 ({ 'MODULE':__name__, 'FUNCTION':'insert_line', 'LEVEL':'ERROR', 'MSG':msg})
            #
            if not bd_historico.insert_raw( dlgid, key, value):
                log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'INSERT BDSQL HISTORICO ERROR {dlgid},{key},{value}'})

            if not bd_temporal.insert_raw( dlgid, key, value):
                log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'INSERT BDSQL TEMPORAL ERROR {dlgid},{key},{value}'})

    #
    log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'{protocolo} EXIT'})
    sys.stdout.flush()

if __name__ == '__main__':

    signal.signal(signal.SIGINT, clt_C_handler)

    # Arranco el proceso que maneja los inits
    config_logger('CONSOLA')
    log2 ({ 'MODULE':__name__, 'FUNCTION':'__init__', 'LEVEL':'ERROR', 'MSG':'XPROCESS START'})
    
    servicios = servicios.Servicios()

    # Espero para siempre
    while True:
        # Leo el tamaño de la cola de RXDATA_QUEUE.
        endpoint = 'READ_QUEUE_LENGTH'
        params = { 'QUEUE_NAME':'RXDATA_QUEUE' }  
        response = servicios.process(params=params, endpoint=endpoint)
        if response.status_code() == 200:
            queue_length = response.json().get('QUEUE_LENGTH',0)
            log2 ({ 'MODULE':__name__, 'FUNCTION':'__init__', 'LEVEL':'ERROR', 'MSG':f'QUEUE_LENGTH={queue_length}'})
            #
            # Si hay datos leo la cola, la leo toda
            spxR2_list = []
            spxR3_list = []
            plcR2_list = []
            if queue_length > 0:
                endpoint = 'READ_DATA_BOUNDLE'
                params = { 'QUEUE_NAME':'RXDATA_QUEUE', 'COUNT':queue_length } 
                response = servicios.process(params=params, endpoint=endpoint)
                if response.status_code() == 200:
                    boundle = response.json().get('L_DATA_BOUNDLE',[])
                    # Separo los datos en listas independientes
                    for pk in boundle:
                        d = pickle.loads(pk)
                        protocolo = d.get('PROTO','ERR')
                        if protocolo == 'SPXR2':
                            spxR2_list.append(d)
                        elif protocolo == 'SPXR3':
                            spxR3_list.append(d)
                        elif protocolo == 'PLCR2':
                            plcR2_list.append(d)
                    #
                    # Proceso las listas
                    if len(spxR2_list) > 0:
                        p1 = Process(target = process_frames, args = ('SPXR2', spxR2_list))
                        p1.start()
                    #
                    if len(spxR3_list) > 0:
                        p2 = Process(target = process_frames, args = ('SPXR3', spxR3_list))
                        p2.start()
                    #
                    if len(plcR2_list) > 0:
                        p3 = Process(target = process_frames, args = ('PLCR2', plcR2_list))
                        p3.start()
                    #
                #
            #
        #
        time.sleep(60)
