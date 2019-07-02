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

import base64
import html
import re
import sys
import zlib
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from os import chdir
from urllib.parse import parse_qs

from lxml import etree as _etree
from signxml import XMLSigner

port = sys.argv[1]
expected_acs_endpoint = sys.argv[2]
acs_redirect_url = sys.argv[3]
auth_user = sys.argv[4]

chdir('..')

saml_response_redirect_template = open("resources/saml-response-redirect-template.html").read()


def format_time(time):
    return time.replace(microsecond=0).isoformat() + 'Z'


def make_saml_response():
    key = open("resources/privatekey.pem").read().encode('ascii')
    cert = open("resources/idp.crt").read().encode('ascii')
    saml_response_template = open("resources/saml-response-template.xml").read()
    saml_response = saml_response_template \
        .replace("issue-instant-field", format_time(datetime.utcnow())) \
        .replace("session-not-on-or-after-field", format_time(datetime.utcnow() + timedelta(days=1))) \
        .replace("not-on-or-after-field", format_time(datetime.utcnow() + timedelta(minutes=5))) \
        .replace("auth-user-field", auth_user)
    root = _etree.fromstring(saml_response)
    signed_root = XMLSigner().sign(root, key=key, cert=cert)
    return base64.b64encode(_etree.tostring(signed_root)).decode()


class MyHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Respond to a GET request."""
        if self.path == "/healthcheck":
            self.send_response(200)
            self.send_header("content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"OK")
            return

        url_tokens = self.path.split("?")
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
            self.send_header("content-type", "text/html")
            self.end_headers()
            response = saml_response_redirect_template \
                .replace("form-action-field", html.escape(acs_redirect_url)) \
                .replace("saml-response-field", html.escape(make_saml_response())) \
                .replace("relay-state-field", html.escape(relay_state))
            self.wfile.write(response.encode('ascii'))
        else:
            self.send_response(400)
            self.send_header("content-type", "text/html")
            self.end_headers()
            self.wfile.write(b"Invalid AssertionConsumerServiceURL is SAML request. Expecting %s. SAML request: %s"
                             % (expected_acs_endpoint.encode('ascii'), saml_request_zlib_decoded))
        return


def run(server_class=HTTPServer, handler_class=MyHandler):
    server_address = ('', int(port))
    httpd = server_class(server_address, handler_class)
    httpd.serve_forever()


run()
