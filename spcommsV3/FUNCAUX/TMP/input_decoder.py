#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase de normalizacion de las entradas
Recibe un diccionario con datos del nombre del servidor, GET y POST
y lo convierte a un diccionario con entradas normalizadas
'''

from urllib.parse import parse_qs
import os

class InputDecoder:
    '''
    Clase que implementa la normalizacion de las entradas
    Entrada:
        d_input = { 'SERVER':'spcommsV3.py', 'GET':GET_STRING, 'POST':POST_BYTE_STRING }
    Salida:
        d_input_norm = {
            'ID':
            'TYPE':
            'PROTO':
            'FW_VER':
            'IVER':
            'PROCESS':          # Proceso que atiende ( PING, CONFIG, DATA )
            'SUBPROCESS':       # Subproceso: ( CONF_ANALOG, CONF_BASE, ....)
            'SCRIPT':
        }

    '''
    def __init__(self):
        self.d_input = {}
        self.d_input_norm = {}
        self.d_qs = {}
       
    def get_iver(self, fw_ver):   
        '''
        Convierto el string VER que es del tipo 2.0.3a, lo convierte a 203
        de modo que pueda determinar numericamente la version en comparaciones
        '''
        l=fw_ver.split('.')
        if len(l) < 3:
            l = [ '2','0','0']
        rev_mayor = int(l[0])
        rev_media = int(l[1])
        rev_menor = int(l[2][0])
        iver = int(rev_mayor)*100 + int(rev_media)*10 + int(rev_menor)
        return iver

    def decoder_SPXR3(self, query_string):
        '''
        Recibimos un QUERY_STRING de un datalogger SPXR3.
        Este equipo transmite todo por GET.
        Estos frames usan los delimitadores estandard de html (=,&) de modo que puedo 
        parsearlo con urlib.parse_qs. Este nos devuelve un diccionario de listas
        '''
        self.d_qs = parse_qs(query_string)
        self.d_input_norm['ID'] = self.d_qs.get('ID',['00000'])[0]
        self.d_input_norm['TYPE'] = 'SPXR3'
        self.d_input_norm['PROTO'] = 'SPXR3'
        self.d_input_norm['FW_VER'] = self.d_qs.get('VER',['0.0.0'])[0]
        self.d_input_norm['IVER'] = self.get_iver(self.d_input_norm['FW_VER'])
        self.d_input_norm['SCRIPT'] = os. path. basename(__file__)
        self.d_input_norm['PAYLOAD'] = {}
        #
        clase = self.d_qs.get('CLASS',[])[0]
        if clase == 'PING':
            self.d_input_norm['PROCESS'] = 'PING'
        elif clase == 'CONF_BASE':
            self.d_input_norm['PROCESS'] = 'CONFIG'
            self.d_input_norm['SUBPROCESS'] = 'BASE'
        elif clase == 'CONF_ANALOG':
            self.d_input_norm['PROCESS'] = 'CONFIG'
            self.d_input_norm['SUBPROCESS'] = 'ANALOG'
        elif clase == 'CONF_COUNTER':
            self.d_input_norm['PROCESS'] = 'CONFIG'
            self.d_input_norm['SUBPROCESS'] = 'COUNTER'
        elif clase == 'CONF_MODBUS':
            self.d_input_norm['PROCESS'] = 'CONFIG'
            self.d_input_norm['SUBPROCESS'] = 'MODBUS'
        elif clase == 'DATA':
            self.d_input_norm['PROCESS'] = 'DATA'
        else:
            self.d_input_norm['PROCESS'] = 'ERROR'
        
    def normalize(self, d_in):
        '''
        Normaliza el diccionario de entrada 
        '''
        self.d_input = d_in
        # Debemos tantear para determinar el protocolo. Aun no puedo parsearlo
        query_string = self.d_input.get('GET',{}).get('QS','')
        #
        if 'SPXR3' in query_string:
            self.decoder_SPXR3(query_string)

        return self.d_input_norm

if __name__ == '__main__':
    # Testing
    i_decoder = InputDecoder()
    d_input = {
        'GET': {
            'QS': 'ID=PABLO&TYPE=SPXR3&VER=1.0.0&CLASS=PING', 
            'SIZE': 0
        },
        'POST': {
            'STREAM': '123456789', 
            'BYTES': b'123456789', 
            'SIZE': 9
        }
    }
    d_input_norm = i_decoder.normalize(d_input)
    import pprint
    print('INPUT_DICT: ')
    pprint.pprint(d_input)
    print('INPUT_DICT_NORM:')
    pprint.pprint(d_input_norm)


