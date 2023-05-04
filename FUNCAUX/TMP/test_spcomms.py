#!/opt/anaconda3/envs/mlearn/bin/python3

import urllib.request
from FUNCAUX.APIS.api_redis import api_configuracion
from FUNCAUX.BD.spc_bd_redis_local import BD_REDIS_LOCAL

def test_api_configuracion_RECOVERID(uid='42125128300065090117010400000000'):
    d_params = {'SERVICE':'RECOVERID', 'UID':uid }
    d_rsp = api_configuracion(d_params)
    new_dlgid = d_rsp['DLGID']
    if new_dlgid == 'PABLO':
        print('Test: api_configuracion RECOVERID: OK')
        return True
    else:
        print('Test: api_configuracion RECOVERID: FAIL')
        return False
    
def test_api_configuracion_GET_CONFIG(dlgid='PABLO'):
    d_params = {'SERVICE':'GET_CONFIG', 'DLGID':dlgid }
    d_conf = api_configuracion(d_params)
    if d_conf is not None:
        print(f'Test: api_configuracion GET_CONFIG: OK')
        print(d_conf)
        return True
    else:
        print(f'Test: api_configuracion GET_CONFIG: FAIL')
        return False

def test_redis_local_get_dlg_config(dlgid='PABLO'):
    lrh = BD_REDIS_LOCAL()
    d_conf = lrh.get_dlg_config(dlgid)
    if d_conf is not None:
        print(f'Test: redis_local_get_dlg_config: OK')
        print(d_conf)
        return True
    else:
        print(f'Test: redis_local_get_dlg_config: FAIL')
        return False

def test_redis_local_set_dlg_config(dlgid='PABLO', d_conf={'BASE':'TEST_OK','A0':'ANALOG_0'}):
    lrh = BD_REDIS_LOCAL()
    rsp = lrh.set_dlg_config(dlgid, d_conf)
    if rsp:
        print(f'Test: redis_local_set_dlg_config: OK')
        return True
    else:
        print(f'Test: redis_local_set_dlg_config: FAIL')
        return False
    
def test_frame_ping():
    pass

def test_frame_config_base():
    url='http://127.0.0.1:80/cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_BASE;UID:42125128300065090117010400000000;HASH:0x11'
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    print(f'Test: frame_config_base...')
    print(response.read())

def test_frame_config_ainputs():
    url='http://127.0.0.1:80/cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_AINPUTS;HASH:0x01'
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    print(f'Test: frame_config_ainputs...')
    print(response.read())

def test_frame_config_counters():
    url='http://127.0.0.1:80/cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_COUNTERS;HASH:0x01'
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    print(f'Test: frame_config_counters...')
    print(response.read())

def test_frame_config_modbus():
    url='http://127.0.0.1:80/cgi-bin/spcomms/spcomms.py?ID:PABLO;TYPE:SPXR2;VER:1.0.0;CLASS:CONF_MODBUS;HASH:0x01'
    request = urllib.request.Request(url)
    response = urllib.request.urlopen(request)
    print(f'Test: frame_config_modbus...')
    print(response.read())


if __name__ == "__main__":
    # API
    #test_api_configuracion_RECOVERID()
    #test_api_configuracion_GET_CONFIG()
    # 
    # LOCAL REDIS
    #test_redis_local_get_dlg_config()
    #test_redis_local_set_dlg_config()
    #test_redis_local_get_dlg_config()
    #
    # FRAMES
    test_frame_config_base()
    test_frame_config_ainputs()
    test_frame_config_counters()
    test_frame_config_modbus()
    







