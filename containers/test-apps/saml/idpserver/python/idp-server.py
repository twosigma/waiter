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

print("zzzxxx starting -1")

import base64
import html
import re
import sys
import zlib
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from os import path
from urllib.parse import parse_qs

from lxml import etree
from signxml import XMLSigner

port = sys.argv[1]
expected_acs_endpoint = sys.argv[2]
acs_redirect_url = sys.argv[3]
auth_user = sys.argv[4]

idpserver_root_dir = path.join(path.dirname(path.abspath(__file__)), "..")

print("zzzxxx starting")
sys.stdout.flush()

def readfile(file):
    return open(path.join(idpserver_root_dir, file)).read()


saml_response_redirect_template = readfile("resources/saml-response-redirect-template.html")


def format_time(time):
    return time.replace(microsecond=0).isoformat() + 'Z'


def make_saml_response():
    key = readfile("resources/privatekey.pem").encode('ascii')
    cert = readfile("resources/idp.crt").encode('ascii')
    saml_response_template = readfile("resources/saml-response-template.xml")
    saml_response = saml_response_template \
        .replace("issue-instant-field", format_time(datetime.utcnow())) \
        .replace("session-not-on-or-after-field", format_time(datetime.utcnow() + timedelta(days=1))) \
        .replace("not-on-or-after-field", format_time(datetime.utcnow() + timedelta(minutes=5))) \
        .replace("auth-user-field", auth_user)
    root = etree.fromstring(saml_response)
    signed_root = XMLSigner().sign(root, key=key, cert=cert)
    return base64.b64encode(etree.tostring(signed_root)).decode()


class MyHandler(BaseHTTPRequestHandler):
    try:
        def do_GET(self):
            """Respond to a GET request."""
            if self.path == "/healthcheck":
                self.send_response(200)
                print("zzzxxx 200 health check\n")
                sys.stdout.flush()
                self.send_header("content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"OK")
                return

            print("zzzxxx trying %s" % self.path)
            sys.stdout.flush()
            url_tokens = self.path.split("?")
            
            print("zzzxxx url_tokens ", url_tokens)
            sys.stdout.flush()
            if not url_tokens or len(url_tokens) < 2:
                return
            query_params = parse_qs(url_tokens[1])
            saml_request = query_params["SAMLRequest"][0]
            relay_state = query_params["RelayState"][0]

            saml_request_b64_decoded = base64.b64decode(saml_request)
            saml_request_zlib_decoded = zlib.decompress(saml_request_b64_decoded, -15)

            acs_endpoint_match = re.search('AssertionConsumerServiceURL="([^"]+)"', str(saml_request_zlib_decoded))
            if acs_endpoint_match and acs_endpoint_match[1] == expected_acs_endpoint:
                self.send_response(200)
                print("zzzxxx 200 acs")
                sys.stdout.flush()
                self.send_header("content-type", "text/html")
                self.end_headers()
                response = saml_response_redirect_template \
                    .replace("form-action-field", html.escape(acs_redirect_url)) \
                    .replace("saml-response-field", html.escape(make_saml_response())) \
                    .replace("relay-state-field", html.escape(relay_state))
                self.wfile.write(response.encode('ascii'))
            else:
                self.send_response(400)
                print("Invalid AssertionConsumerServiceURL is SAML request. Expecting %s. SAML request: %s"
                                 % (expected_acs_endpoint, saml_request_zlib_decoded.decode()))
                print("zzzxxx 400 acs")
                sys.stdout.flush()
                self.send_header("content-type", "text/html")
                self.end_headers()
                self.wfile.write(b"Invalid AssertionConsumerServiceURL is SAML request. Expecting %s. SAML request: %s"
                                 % (expected_acs_endpoint.encode('ascii'), saml_request_zlib_decoded))
    except:
        print("zzzxxx error", sys.exc_info())
        sys.stdout.flush()
    return


def run(server_class=HTTPServer, handler_class=MyHandler):
    server_address = ('', int(port))
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()


run()
