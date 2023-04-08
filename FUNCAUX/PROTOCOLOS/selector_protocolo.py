#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Recibe el d_inputs y analiza el protocolo para determinar
que servicio va a atender el frame
'''

class SelectorProtocolo:

    def __init__(self):
        pass

    def decode_protocol(self, d_input):
        
        query_string = d_input.get('GET',{}).get('QS', '')
        protocolo = 'UNKNOWN'
        #
        if 'SPXR3' in query_string:
            protocolo = 'SPXR3'

        return protocolo
    
    