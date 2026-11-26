#!/usr/bin/env python3
"""A deliberately broken endpoint for exercising the walkthrough's cursor check."""
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        self.send_response(204)
        self.end_headers()

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'{"ts":"2025-01-01T00:00:01Z","line":"pagination example first"}\n')


HTTPServer(("127.0.0.1", 18081), Handler).serve_forever()
