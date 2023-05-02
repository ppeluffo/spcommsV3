#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3
'''
Clase de formateo de las salidas
'''

class OutputFormatter():
    '''
    Clase que implementa el formateo de las salidas
    '''
    def __init__(self):
        self.d_output_norm = {}
        self.output_stream = b''
        
    def make_response(self, d_output):
        '''
        Recibe un diccionario normalizado de salida y genera el str de salida.
        '''
        self.output_stream = d_output.get('RSP_PAYLOAD','ERROR')
        return self.output_stream
    
if __name__ == '__main__':
    d_output_norm = {'RSP_PAYLOAD': 'PING'}
    o_encoder = OutputFormatter()
    response = o_encoder.make_response(d_output_norm)
    import pprint
    print('D_OUTPUT_NORM:')
    pprint.pprint(d_output_norm)
    print('RESPONSE:')
    print(f'{response}')
