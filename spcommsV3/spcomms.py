#!/usr/bin/python3
"""

Pendiente:

5- Testing con prueba de carga
6- Grafana: 
    incorporar probes prometheus
    Visualizar graficas.
7- Pasar a docker y github
8- Profilear el codigo (vcode + austin)
---------------------------------------------------------------------------

Servidor de comunicaciones generales de los dispositivos de SPYMOVIL SPXR3
Todos los equipos acceden inicialmente a este script.
Todas las entradas se normalizan en un diccionario d_input_norm.
Todos los proceso genera un diccionario normalizado d_output_norm
#
d_input =  { 
    'GET':{ QS', 'SIZE'}, 
    'POST': {'STREAM', 'BYTES', 'SIZE' } 
}

d_output = {
    'DLGID':
    'RSP_PAYLOAD':
}

Modo de testing:
telnet localhost 80
GET /cgi-bin/spcommsV3/spcommsV3.py?ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING
HTTP/1.1
Host: www.spymovil.com

"""

import os
import sys

from FUNCAUX.UTILS.spc_log import config_logger, log2, set_debug_dlgid
from FUNCAUX.PROTOCOLOS import selector_protocolo, protocolo_SPXR3, protocolo_PLCR2
from FUNCAUX.UTILS import spc_stats
from FUNCAUX.SERVICIOS import servicios

VERSION = '0.0.4 @ 2023-04-11'

# -----------------------------------------------------------------------------
def read_input():
    '''
    La entrada puede ser GET (PLC,PLCPAY,SPX,...) o POST ( OCEANUS ) o ambas
    Responde con un diccionario con las claves GET y POST:
    d{ 'GET':{ QS', 'SIZE'}, 
       'POST': {'STREAM', 'BYTES', 'SIZE' } 
    }
    '''
    #
    # GET part ----------------------------------------------------------------
    get_query_string = os.environ.get('QUERY_STRING', None)
    try:
        get_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except ValueError:
        get_body_size = 0
    #
    if get_query_string  is None:
        log2( { 'MODULE':__name__,'FUNCTION':'read_input','MSG':'ERROR GET_QS NULL' })
        sys.exit(1)
    #
    d_get = { 'QS': get_query_string, 'SIZE': get_body_size }
    #
    # POST part ---------------------------------------------------------------
    try:
        post_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        post_body_size = 0
    #
    # log2( { 'MODULE':__name__,'FUNCTION':'read_input','MSG':f'POST_SIZE={post_body_size}' } )
    #
    post_body_stream = ''
    post_body_bytes = b''
    if post_body_size > 0:
        post_body_stream = sys.stdin.read(post_body_size)
        post_body_bytes = post_body_stream.encode('utf-8', 'surrogateescape')
        log2 ( { 'MODULE':__name__,'FUNCTION':'read_input','MSG':f'POST_BYTES={post_body_bytes}' })
    #
    d_post = { 'STREAM': post_body_stream,
              'BYTES': post_body_bytes,
              'SIZE': post_body_size
              }
    #
    d_rsp = {'GET':d_get, 'POST':d_post }
    #log2 ({ 'MODULE':__name__, 'FUNCTION':'read_input','MSG':f'D_INPUT={d_rsp}' })
    return d_rsp

def main():
    #
    try:
        spc_stats.init_stats()
        
        # Lo primero es configurar el logger
        config_logger('SYSLOG')
        #

        # Leo la entrada
        d_input = read_input()
        #
        # Determino que servicio va a atender al frame en virtud del prtocolo de este.
        prot_selector = selector_protocolo.SelectorProtocolo()
        protocolo = prot_selector.decode_protocol(d_input)
        #
        # Leemos el DEBUG_DLGID para setearlo bien al ppio.
        endpoint = 'READ_DEBUG_DLGID'
        params = {'LOG':False}
        servicio = servicios.Servicios()
        response = servicio.process(params=params, endpoint=endpoint)
        if response.status_code() == 200:
            debug_dlgid = response.json().get('DEBUG_DLGID','00000')
            set_debug_dlgid(debug_dlgid)
        #
        # Todos los protocolos me devuelven un dict con los datos de la salida.
        d_output = {}
        if protocolo == 'SPXR3':
            # Incluye los protocolos SPXR2 y SPXR3
            d_output = protocolo_SPXR3.ProtocoloSPXR3().process_protocol(d_input)
        elif protocolo == 'PLCR2':
            d_output = protocolo_PLCR2.ProtocoloPLCR2().process_protocol(d_input)
        else:
            log2 ({ 'MODULE':__name__, 'FUNCTION':'process', 'LEVEL':'INFO',
            'MSG':f'ERROR: PROTOCOLO NO DEFINIDO {protocolo}'})
            d_output = {}
        
        log2 ({ 'MODULE':__name__, 'FUNCTION':'MAIN', 'LEVEL':'SELECT',
        'DLGID':'00000', 'MSG':f'D_OUTPUT={d_output}'})  
        #
        # Actualizo las estadisticas
        endpoint = 'READ_QUEUE_LENGTH'
        params = { 'QUEUE_NAME': 'STATS_QUEUE','LOG':False }
        response = servicio.process(params=params, endpoint=endpoint)
        stats_queue_length = 0
        if response.status_code() == 200:
            stats_queue_length = response.json().get('QUEUE_LENGTH',0)
        spc_stats.set_stats_queue_length(stats_queue_length)
        #   
        endpoint = 'READ_QUEUE_LENGTH'
        params = { 'QUEUE_NAME': 'RXDATA_QUEUE','LOG':False }
        response = servicio.process(params=params, endpoint=endpoint)
        data_queue_length = 0
        if response.status_code() == 200:
            data_queue_length = response.json().get('QUEUE_LENGTH',0)
        spc_stats.set_data_queue_length(data_queue_length)
        #
        # Save stats
        dlgid = d_output.get('DLGID','00000')
        d_stats = spc_stats.read_d_stats()
        endpoint = 'SAVE_STATS'
        params = { 'DLGID':dlgid,'DSTATS':d_stats,'LOG':False }
        _ = servicio.process(params=params, endpoint=endpoint)
        #
        d_statistics = spc_stats.end_stats()
        # Log
        msg = f'STATS: elapsed={d_statistics["duracion_frame"]:.04f},'
        msg += f'cfconf={d_statistics["count_frame_config_base"]},'
        msg += f'cfdata={d_statistics["count_frame_data"]},'
        msg += f'cRedis={ d_statistics["count_accesos_REDIS"]},'
        msg += f'cSql={d_statistics["count_accesos_SQL"]},'
        msg += f'stats_QUEUE={d_statistics["length_stats_queue"]},'
        msg += f'data_QUEUE={d_statistics["length_data_queue"]}'
        d_log = { 'MODULE':__name__, 'FUNCTION':'STATS', 'LEVEL':'INFO', 'MSG':msg }
        log2(d_log)
        #
        dlgid =d_output.get('DLGID','00000')
        tag = d_output.get('TAG',0)
        response_body = d_output.get('RSP_PAYLOAD','ERROR')
        method = d_output.get('METHOD','GET')
        #
        if method == 'GET':
            send_response_get(response_body, tag)
        else:
            send_response_post(response_body, tag)
        #
        log2 ( { 'MODULE':__name__,
                'DLGID':dlgid,
                'FUNCTION':'send_response','MSG':f'({tag}) RSP=>{response_body}' }
            )
    except Exception as ex:
        log2 ({ 'MODULE':__name__, 'FUNCTION':'MAIN ERROR', 'LEVEL':'ERROR',
                'MSG':f'EXEPTION={ex}'})
    return

def send_response_post(response_body, tag):
    #
    try:
        #print("Content-Type: application/octet-stream\n")
        print("Content-Type: application/x-binary\n")
        sys.stdout.flush()
        sys.stdout.buffer.write(response_body)
        sys.stdout.flush()
    except Exception as ex:
        log2 ({ 'MODULE':__name__, 'FUNCTION':'send_response_post', 'LEVEL':'ERROR',
                'MSG':f'({tag}) RSP EXEPTION={ex}'})
        return
    sys.stdout.flush() 

def send_response_get(response_body, tag):
    '''
    Envia la respuesta con los tags HTML adecuados
    '''
    try:
        print('Content-type: text/html\n\n', end='')
        print(f'<html><body><h1>{response_body}</h1></body></html>')
    except Exception as ex:
        log2 ({ 'MODULE':__name__, 'FUNCTION':'send_response_get', 'LEVEL':'ERROR',
                'MSG':f'({tag}) RSP EXEPTION={ex}'})
        
if __name__ == '__main__':
    main()
