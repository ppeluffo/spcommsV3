#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

from urllib.parse import urlencode
from urllib.request import urlopen
import pprint
import datetime as dt

URL_BASE = 'http://localhost/cgi-bin/spcommsV3/spcommsV3.py'

class TESTS:

    def __init__(self):
        self.params = {}
        self.url = ''

    def process_test(self):
        print('PARAMS=', end='')
        pprint.pprint(self.params)
        print(f'URL={self.url}')
        #
        with urlopen(self.url) as response:
            response_text=response.read()
            print(f'GET RESPONSE={response_text}')

    def test_spxR3_ping(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'PING',
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 PING...')
        self.process_test()
        
    def test_spxR3_base(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_BASE',
            'UID':'42125128300065090117010400000000',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 BASE...')
        self.process_test()

    def test_spxR3_ainputs(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_AINPUTS',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 AINPUTS...')
        self.process_test()

    def test_spxR3_counters(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_COUNTERS',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 COUNTERS...')
        self.process_test()

    def test_spxR3_modbus(self):

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'CONF_MODBUS',
            'HASH':'0x11'
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 MODBUS...')
        self.process_test()

    def test_spxR3_data(self):

        now = dt.datetime.now()

        self.params = {
            'ID': 'PABLO',
            'TYPE': 'SPXR3',
            'VER': '1.0.0',
            'CLASS': 'DATA',
            'DATE':now.strftime('%y%m%d'),
            'TIME':now.strftime('%H%M%S'),
            'A0':0.00,
            'A1':0.00,
            'A2':0.00,
            'C0':0.000,
            'C1':0.000,
            'bt':12.496
        }
    
        query_string = urlencode(self.params)
        self.url = URL_BASE + '?' + query_string
        #
        print('Test SPXR3 DATA...')
        self.process_test()

if __name__ == '__main__':

    import timeit
    start_time = timeit.default_timer()

    test = TESTS()
    test.test_spxR3_ping()
    #test.test_spxR3_base()
    #test.test_spxR3_ainputs()
    #test.test_spxR3_counters()
    #test.test_spxR3_modbus()
    #test.test_spxR3_data()
    elapsed = timeit.default_timer() - start_time
    print(f'elapsed time: {elapsed}')



    
