#!/usr/bin/python3
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

from FUNCAUX.UTILS.spc_log import config_logger, log2
from FUNCAUX.SERVICIOS import servicios


def clt_C_handler(signum, frame):
    exit(0)

def process_frames( protocolo, boundle_list ):
    nro_items = len(boundle_list)
    log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'{protocolo} ITEMS_LENGTH={nro_items}'})
    for d_data in boundle_list:
        dlgid = d_data.get('DLGID','0000')
        d_line = d_data.get('D_LINE',{})
        log2 ({ 'MODULE':__name__, 'FUNCTION':'process_frames', 'LEVEL':'ERROR', 'MSG':f'{protocolo} D_LINE={d_line}'})
        for key in d_line:
            msg = f'{protocolo},{dlgid},{key}=>{d_line[key]}'
            log2 ({ 'MODULE':__name__, 'FUNCTION':'insert_line', 'LEVEL':'ERROR', 'MSG':msg})


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
                    process_frames('SPXR2', spxR2_list)
                    process_frames('SPXR3', spxR3_list)
                    process_frames('PLCR2', plcR2_list)
                #
            #
        #
        time.sleep(60)
