#!/opt/anaconda3/envs/mlearn/bin/python3


from urllib.parse import urlencode
from urllib.request import urlopen
import pprint

URL_BASE = 'http://localhost/cgi-bin/spcomms/test_001.py'

def send_get():
    '''
    Envia un mensaje por medio de un GET al servidor.
    Los parametros del diccionario params los pone en el url que envia
    '''
    params = {
        'ID': 'PABLO',
        'TYPE': 'SPXR2',
        'VER': '1.0.0',
        'CLASS': 'CONF_BASE',
        'UID': '42125128300065090117010400000000',
        'HASH': '0xAF'
        }
    
    query_string = urlencode(params)
    url = URL_BASE + '?' + query_string
    #
    print('PARAMS=', end='')
    pprint.pprint(params)
    print(f'URL={url}')
    #
    with urlopen(url) as response:
        response_text=response.read()
        print(f'GET RESPONSE={response_text}')

def send_post():
    '''
    Envia datos pero usando el metodo POST
    '''
    url = URL_BASE
    params = {
        'PARAM1': 'PABLO',
        'PARAM2': 'SPYMOVIL',
        'PARAM3': 123.45,
        'PARAM4': 1234
    }

    post_string = urlencode( params ) 
    post_data = post_string.encode('utf-8')
    #
    print('PARAMS=', end='')
    pprint.pprint(params)
    print(f'URL={url}')
    print(f'POST_STRING={post_string}')
    print(f'POST_DATA={post_data}')
    #
    with urlopen(url=url, data=post_data) as response:
        response_text=response.read()
        print(f'POST_RESPONSE={response_text}')


def send_get_post():
    '''
    Envia datos en el URL como GET y como POST
    '''
    get_params = {
        'ID': 'PABLO',
        'TYPE': 'SPXR2',
        'VER': '1.0.0',
        'CLASS': 'CONF_BASE',
        'UID': '42125128300065090117010400000000',
        'HASH': '0xAF'
        }
    
    get_query_string = urlencode(get_params)
    url = URL_BASE + '?' + get_query_string
    #
    print('GET_PARAMS=', end='')
    pprint.pprint(get_params)
    print(f'GET_URL={url}')
    #
    post_params = {
        'PARAM1': 'POST_PABLO',
        'PARAM2': 'POST_SPYMOVIL',
        'PARAM3': 123.45,
        'PARAM4': 1234
    }
    #
    post_string = urlencode( post_params ) 
    post_data = post_string.encode('utf-8')
    #
    print('POST_PARAMS=', end='')
    pprint.pprint(post_params)
    print(f'POST_STRING={post_string}')
    print(f'POST_DATA={post_data}')
    #
    with urlopen(url=url, data=post_data) as response:
        response_text=response.read()
        print(f'RESPONSE={response_text}')


if __name__ == '__main__':
    #send_get()
    #send_post()
    send_get_post()
    