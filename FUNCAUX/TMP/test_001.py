#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u
#
'''
GET /cgi-bin/spcomms/test_001.py?ID:DEFAULT;TYPE:SPXR2;VER:1.0.0;CLASS:RECOVER;UID:42125128300065090117010400000000
HTTP/1.1
Host: www.spymovil.com
'''
import os
import sys
import logging.handlers
import datetime as dt
from urllib.parse import parse_qs

# -----------------------------------------------------------------------------
def decode_input_001 ():
    '''
    La entrada puede ser GET (PLC,PLCPAY,SPX,...) o POST ( OCEANUS )
    Todos los frames tienen un query_string que me va a determinar el TYPE.
    Es lo primero que parseo
    '''
    # Leo el cgi GET ( Todos los protocolos lo traen )
    query_string = os.environ.get('QUERY_STRING','')
    try:
        request_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        request_body_size = 0

    if query_string is None:
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_001','MSG':'ERROR QS NULL' }
    else:
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_001','MSG':f'QS[{query_string}, SIZE={request_body_size}]' }
    log2(d_log)
    return {}
 
def decode_input_002 ():
    '''
    Parseo la entrada de GET con la libreria urllib.parse.parse_qs
    '''
    # Leo el cgi GET ( Todos los protocolos lo traen )
    query_string = os.environ.get('QUERY_STRING','')
    try:
        request_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        request_body_size = 0

    if query_string is None:
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_002','MSG':'ERROR QS NULL' }
    else:
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_002','MSG':f'QS[{query_string}, SIZE={request_body_size}]' }
    log2(d_log)

    d=parse_qs(query_string)
    d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_002','MSG':f'D={d}' }
    log2(d_log)

    return d

def decode_input_003():
    '''
    Parseo una entrada POST
    https://instructobit.com/tutorial/110/Python-3-urllib%3A-making-requests-with-GET-or-POST-parameters
    http://wsgi.tutorial.codepoint.net/parsing-the-request-post
    '''
    # the environment variable CONTENT_LENGTH may be empty or missing
    try:
        request_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        request_body_size = 0

    # When the method is POST the variable will be sent
    # in the HTTP request body which is passed by the WSGI server
    # in the file like wsgi.input environment variable.
    request_body = sys.stdin.read(request_body_size)
    request_body_bytes = request_body.encode('utf-8', 'surrogateescape')
    d = parse_qs(request_body_bytes)
    d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_003','MSG':f'D={d}' }
    log2(d_log)

    #d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_002','MSG':f'OS={os.environ}' }
    #log2(d_log)

    return d

def decode_input_004():
    '''
    El request tiene parte de GET y parte de POST
    '''
    # GET
    get_string = os.environ.get('QUERY_STRING', None)
    try:
        GET_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        GET_body_size = 0

    if get_string is None:
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_004','MSG':'ERROR QS NULL' }
        log2(d_log)
    else:
        d_get=parse_qs(get_string)
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_004','MSG':f'GET_STRING={get_string}, SIZE={GET_body_size}' }
        log2(d_log)
        d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_004','MSG':f'D_GET={d_get}' }
        log2(d_log)

    # POST
    try:
        POST_body_size = int(os.environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        POST_body_size = 0
    POST_body = sys.stdin.read(POST_body_size)
    POST_body_bytes = POST_body.encode('utf-8', 'surrogateescape')
    d_post = parse_qs(POST_body_bytes)
    d_log = { 'MODULE':__name__, 'FUNCTION':'decode_input_004','MSG':f'D_POST={d_post}' }
    log2(d_log)


    return {}



def send_response( msg='ERROR'):
   
    now=dt.datetime.now().strftime('%y%m%d%H%M%S')
    rsp_msg = f'STATUS:{msg};CLOCK:{now};'
    print('Content-type: text/html\n\n', end='')
    print(f'<html><body><h1>{rsp_msg}</h1></body></html>')
    d_log = { 'MODULE':__name__, 'FUNCTION':'send_response', 'MSG':f'RSP={rsp_msg}' }
    log2(d_log)

def config_logger():
 
    logging.basicConfig(level=logging.DEBUG)

    formatter = logging.Formatter('SPCOMMS [%(levelname)s] [%(name)s] %(message)s')
    handler = logging.handlers.SysLogHandler('/dev/log')
    handler.setFormatter(formatter)
    # Creo un logger derivado del root para usarlo en los modulos
    logger1 = logging.getLogger()
    # Le leo el handler de consola para deshabilitarlo
    lhStdout = logger1.handlers[0]  # stdout is the only handler initially
    logger1.removeHandler(lhStdout)
    # y le agrego el handler del syslog.
    logger1.addHandler(handler)
    # Creo ahora un logger child local.
    LOG = logging.getLogger('spy')
    LOG.addHandler(handler)

def log2(d_log):
    '''
    Se encarga de mandar la logfile el mensaje.
    Si el level es SELECT, dependiendo del dlgid se muestra o no
    Si console es ON se hace un print del mensaje
    El flush al final de print es necesario para acelerar. !!!
    '''
    module = d_log.get('MODULE','NO_MODULE')
    function = d_log.get('FUNCTION', 'NO_FUNTION')
    dlgid = d_log.get('DLGID','00000')
    msg = d_log.get('MSG','NO_MSG')                       
    logging.info('SPCOMMS [{0}][{1}][{2}]:[{3}]'.format( dlgid,module,function,msg))
 
if __name__ == '__main__':

    # Lo primero es configurar el logger
    config_logger()
    d = decode_input_004()
    #
    response = 'OK'
    send_response(response)
    d_log = { 'MODULE':__name__, 'FUNCTION':'__init__', 'LEVEL':'SELECT', 'MSG':f'END TEST_001: RSP={response}' }
    log2(d_log)

