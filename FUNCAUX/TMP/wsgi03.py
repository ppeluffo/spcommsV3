#!/home/pablo/Spymovil/www/cgi-bin/spcommsV3/venv/bin/python3

from wsgiref.simple_server import make_server
from urllib.parse import parse_qs

def application (environ, start_response):


    try:
        request_body_size = int(environ.get('CONTENT_LENGTH', 0))
    except (ValueError):
        request_body_size = 0

    request_body = environ['wsgi.input'].read(request_body_size)
    d = parse_qs(environ['QUERY_STRING'])
    d['REQUEST_BODY'] = [request_body] 
    d['REQUEST_SIZE'] = [request_body_size] 

    response_body = "<html><body>"
    for key in d:
        value = d.get(key,[''])[0]
        response_body += f'[{key}:{value}]<br>' 
    response_body += "</body></html>"
    
    response_body = b'\xff\xff'
    status = '200 OK'
    #('Content-Type', 'text/plain'),
    response_headers = [
        
        ('Content-Type', 'application/x-binary'),

        ('Content-Length', str(len(response_body)))
    ]
    start_response(status, response_headers)

    #response_body =  bytes(response_body, encoding="UTF-8")

    return [response_body]

# Instantiate the server
httpd = make_server (
    'localhost', # The host name
    8051, # A port number where to wait for the request
    application # The application object name, in this case a function
)

# Wait for a single request, serve it and quit
httpd.serve_forever()
