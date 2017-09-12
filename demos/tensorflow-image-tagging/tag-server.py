#!/usr/bin/env python3.6
"""
Very simple HTTP server in python.

Usage::
    ./tag-server.py [<port>]

Send a GET request::
    curl http://localhost:port?imageId=ID&imageSource=URL

"""
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

from urllib.request import urlretrieve
from urllib.parse import parse_qs
import traceback
import os
import subprocess

class Settings(object):
    def __init__(self):
        self.model_dir = '/tmp/imagenet'
        self.num_top_predictions = 5

class Server(BaseHTTPRequestHandler):
    def _set_headers(self, status):
        self.send_response(status)
        self.send_header('Content-type', 'text/html')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

    def do_OPTIONS(self):
        self._set_headers(200)
        self.wfile.write(bytes("", "utf8"))

    def do_GET(self):

        if "/status" == self.path:
            self._set_headers(200)
            self.wfile.write(bytes("OK", "utf8"))
            return

        try:
            get_data = parse_qs(self.path[2:])
            image_id = get_data['id'][0]
            image_name = get_data['name'][0]
            image_url = get_data['url'][0]
            num_top_predictions = get_data['p'][0]

            print('image_id: ' + str(image_id))
            print('image_name: ' + str(image_name))
            print('image_url: ' + str(image_url))

            image_location = 'images/' + image_name
            print('image_location: ' + str(image_location))
            urlretrieve(image_url, image_location)

            cmd = 'python3 classify_image.py --image_file={} --num_top_predictions={}'.format(
                image_location, num_top_predictions)
            print('command: ' + cmd)
            output = subprocess.getoutput(cmd)

            self._set_headers(200)
            output_lines = output.split("\n")
            output_html = "<br />".join(output_lines[6:]) if len(output_lines) < 10 else ""
            self.wfile.write(bytes("<div>" + output_html + "</div>", "utf8"))
        except:
            traceback.print_exc()
            self._set_headers(500)
            self.wfile.write(bytes("<html><body><h1>Internal Server Error</h1></body></html>", "utf8"))

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

def run(handler_class=Server, port=80):

    directory = "images"
    if not os.path.exists(directory):
        os.makedirs(directory)

    server_address = ('', port)
    httpd = ThreadedHTTPServer(('localhost', port), handler_class)
    print('Starting httpd...')
    httpd.serve_forever()

if __name__ == "__main__":
    from sys import argv

    if len(argv) == 2:
        run(port=int(argv[1]))
    else:
        run()
