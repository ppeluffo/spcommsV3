#!/opt/anaconda3/envs/mlearn/bin/python3
#!/usr/bin/python3 -u

'''
Funciones de uso general.
'''

# Dependencias
import re


TRACE_DEBUG=False

def version2int (str_version):
    '''
    VER tiene un formato tipo A.B.C.
    Lo convertimos a un numero A*100 + B*10 + C
    '''
    return int(re.sub(r"[A-Z,a-z,.]",'',str_version))

def u_hash( seed, line ):
    '''
    Calculo un hash con el string pasado en line.
    Devuelve un entero
    Se utiliza el algoritmo de Pearson
    https://es.abcdef.wiki/wiki/Pearson_hashing
    La función original usa una tabla de nros.aleatorios de 256 elementos.
	Ya que son aleatorios, yo la modifico a algo mas simple.

    Es la misma implementacion que se usa en los dataloggers.
    '''

    hash_table = [ 93,  153, 124,  98, 233, 146, 184, 207, 215,  54, 208, 223, 254, 216, 162, 141,
		 10,  148, 232, 115,   7, 202,  66,  31,   1,  33,  51, 145, 198, 181,  13,  95,
		 242, 110, 107, 231, 140, 170,  44, 176, 166,   8,   9, 163, 150, 105, 113, 149,
		 171, 152,  58, 133, 186,  27,  53, 111, 210,  96,  35, 240,  36, 168,  67, 213,
		 12,  123, 101, 227, 182, 156, 190, 205, 218, 139,  68, 217,  79,  16, 196, 246,
		 154, 116,  29, 131, 197, 117, 127,  76,  92,  14,  38,  99,   2, 219, 192, 102,
		 252,  74,  91, 179,  71, 155,  84, 250, 200, 121, 159,  78,  69,  11,  63,   5,
		 126, 157, 120, 136, 185,  88, 187, 114, 100, 214, 104, 226,  40, 191, 194,  50,
		 221, 224, 128, 172, 135, 238,  25, 212,   0, 220, 251, 142, 211, 244, 229, 230,
		 46,   89, 158, 253, 249,  81, 164, 234, 103,  59,  86, 134,  60, 193, 109,  77,
		 180, 161, 119, 118, 195,  82,  49,  20, 255,  90,  26, 222,  39,  75, 243, 237,
		 17,   72,  48, 239,  70,  19,   3,  65, 206,  32, 129,  57,  62,  21,  34, 112,
		 4,    56, 189,  83, 228, 106,  61,   6,  24, 165, 201, 167, 132,  45, 241, 247,
		 97,   30, 188, 177, 125,  42,  18, 178,  85, 137,  41, 173,  43, 174,  73, 130,
		 203, 236, 209, 235,  15,  52,  47,  37,  22, 199, 245,  23, 144, 147, 138,  28,
		 183,  87, 248, 160,  55,  64, 204,  94, 225, 143, 175, 169,  80, 151, 108, 122 ]

    h = seed
    for c in line:
        h = hash_table[h ^ ord(c)]
    return h

def trace(d_in:dict, msg:str):
    '''
    Muestra en pantalla el diccionario de entrada con el mensaje.
    Se usa para debug.
    '''
    if TRACE_DEBUG:
        import pprint
        print('---------------------------------------')
        print(f'{msg}')
        pprint.pprint(d_in)

def check_particular_params( d_input:dict, t:tuple)->dict:
    '''
    Verifica que las key particulares ( en la tupla t) se encuentren
    en el d_input
    '''
    if not all( key in d_input for key in t ):
        return { 'RES':False, 'ERROR':f'PARAMS error: {t} is missed'  }
    #
    return { 'RES': True, 'ERROR':''  }

def check_inputs( d_input:dict, main_key:str )->dict:
    '''
    Chequea los parametros generales de entrada al prcesamiento de las APIs
    El parametro debe ser un diccionario y tener las key 'API', 'REQUEST' y 'PARAMS'.
    'PARAMS' debe ser dict()
    '''
    if not isinstance(d_input, dict):
        return { 'RES':False, 'ERROR':'input is not a dict()' }
        
    # Chequeo selector de opciones
    if not main_key in d_input:
        return { 'RES':False, 'ERROR':f'key {main_key} not present' }
        
    if not 'REQUEST' in d_input.get(main_key,{}):
        return { 'RES':False, 'ERROR':'Error de parametros (REQUEST)'  }

    if not 'PARAMS' in d_input.get(main_key,{}):
        return { 'RES':False, 'ERROR':'Error de parametros (PARAMS)'   }
        
    if not isinstance(d_input[main_key]['PARAMS'], dict):
        return { 'RES':False, 'ERROR':'PARAMS is not a dict()'  }
    
    return { 'RES': True, 'ERROR':''  }
