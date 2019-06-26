#!/usr/bin/env python3
#
#  Copyright (c) 2019 Two Sigma Open Source, LLC
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to
#  deal in the Software without restriction, including without limitation the
#  rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
#  sell copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
#  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
#  IN THE SOFTWARE.
#

from http.server import BaseHTTPRequestHandler, HTTPServer
from signxml import XMLSigner, XMLVerifier
from xml.etree import ElementTree
from os import path, getcwd, chdir
from urllib.parse import parse_qs

# def print_my_path():
#     print('cwd:     {}'.format(getcwd()))
#     print('__file__:{}'.format(__file__))
#     print('abspath: {}'.format(path.abspath(__file__)))
#
# print_my_path()

chdir('..')

# print_my_path()

saml_response_redirect_template = open("resources/saml-response-redirect-template.html").read().encode('ascii')
saml_response_template = open("resources/saml-response-template.xml").read().encode('ascii')

print("hi")

print(ElementTree.parse("/home/nsinkov/x509/test.xml").getroot())

def sign(xml_path):
    cert = open("/home/nsinkov/x509/publickey.cer").read().encode('ascii')
    key = open("/home/nsinkov/x509/privatekey.pem").read().encode('ascii')
    # root = ElementTree.fromstring(data_to_sign)
    root = ElementTree.parse(xml_path)
    print(root)
    signed_root = XMLSigner().sign(root.getroot(), key=key, cert=cert)
    return signed_root
    # verified_data = XMLVerifier().verify(signed_root).signed_xml


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Respond to a GET request."""
        query_params = parse_qs(self.path.split("?")[1])
        saml_request = query_params["SAMLRequest"][0]
        relay_state = query_params["RelayState"][0]

        xxx get this out:
        # AssertionConsumerServiceURL="https://127.0.0.1/waiter-auth/saml/acs"


        self.send_response(200)
        self.send_header("content-type", "text/html")
        self.end_headers()
        response = saml_response_redirect_template\
            .replace("form-action-field", )\
            .replace("saml-response-field", )\
            .replace("relay-state-field", relay_state)
        self.wfile.write(response.encode())
        signed_root = sign("/home/nsinkov/x509/test.xml")
        xml_str = ElementTree.tostring(signed_root, encoding='utf8', method='xml')
        print(xml_str)
        self.wfile.write(b"xml is: %s" % xml_str)
        return

def run(server_class=HTTPServer, handler_class=MyHandler):
    server_address = ('', 8000)
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()

run()