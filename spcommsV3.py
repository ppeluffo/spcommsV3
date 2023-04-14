#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
"""

Pendiente:

3- Incoporar protocolo SPXR2
4- Incorporar PLCR2
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

from FUNCAUX.UTILS.spc_log import config_logger, log2
from FUNCAUX.PROTOCOLOS import selector_protocolo
from FUNCAUX.UTILS import spc_stats

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
        log2( { 'MODULE':__name__, 
               'FUNCTION':'read_input',
               'MSG':'ERROR GET_QS NULL' } 
        )
        sys.exit(1)
    #
    d_get = { 'QS': get_query_string,
             'SIZE': get_body_size 
    }
    #
    # POST part ---------------------------------------------------------------
    try:
        post_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        post_body_size = 0
    #
    #log2( { 'MODULE':__name__, 
    #       'FUNCTION':'read_input',
    #       'MSG':f'POST_SIZE={post_body_size}' } )
    #
    post_body_stream = ''
    post_body_bytes = b''
    if post_body_size > 0:
        post_body_stream = sys.stdin.read(post_body_size)
        post_body_bytes = post_body_stream.encode('utf-8', 'surrogateescape')
        log2 ( { 'MODULE':__name__, 
                'FUNCTION':'read_input',
                'MSG':f'POST_BYTES={post_body_bytes}' } 
        )
    d_post = { 'STREAM': post_body_stream,
            'BYTES': post_body_bytes,
            'SIZE': post_body_size
    }
    #
    d_rsp = {'GET':d_get, 'POST':d_post }
    #log2 ({ 'MODULE':__name__, 'FUNCTION':'read_input','MSG':f'D_INPUT={d_rsp}' })
    return d_rsp

def send_response(d_reponse:dict):
    '''
    Envia la respuesta con los tags HTML adecuados
    '''
    dlgid = d_reponse.get('DLGID','00000')
    response_str = d_reponse.get('RSP_PAYLOAD','ERROR')
    tag = d_reponse.get('TAG',0)
    #
    print('Content-type: text/html\n\n', end='')
    print(f'<html><body><h1>{response_str}</h1></body></html>')
    #
    log2 ( { 'MODULE':__name__,
            'DLGID':dlgid,
            'FUNCTION':'send_response','MSG':f'({tag}) RSP=>{response_str}' }
    )
    #

def main():
    #
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

    # Todos los protocolos me devuelven un dict con los datos de la salida.
    d_output = {}
    if protocolo == 'SPXR3':
        from FUNCAUX.PROTOCOLOS import protocolo_SPXR3
        p_spxr3 = protocolo_SPXR3.ProtocoloSPXR3()
        d_output = p_spxr3.process(d_input)
    #
    # Respondo
    send_response(d_output)
    #
    d_statistics = spc_stats.end_stats()
    # Log
    msg = f'STATS: elapsed={d_statistics["duracion_frame"]:.04f},'
    msg += f'cfconf={d_statistics["count_frame_config_base"]},'
    msg += f'cfdata={d_statistics["count_frame_data"]},'
    msg += f'cRedis={ d_statistics["count_accesos_REDIS"]},'
    msg += f'cSql={d_statistics["count_accesos_SQL"]},'
    msg += f'QUEUE={d_statistics["length_stats_queue"]}'
    d_log = { 'MODULE':__name__, 'FUNCTION':'STATS', 'LEVEL':'INFO', 'MSG':msg }
    log2(d_log)

if __name__ == '__main__':
    main()
