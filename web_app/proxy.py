#!/usr/bin/env python3
"""Simple HTTP server that serves the web_app and proxies /api/* requests to the backend.

Run: python web_app/proxy.py
Open: http://localhost:8000
"""
import http.server
import socketserver
import urllib.request
import urllib.error
import urllib.parse
import os

PORT = 8000
BACKEND = os.environ.get('USAGE_API_BACKEND', 'http://localhost:5000')

class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # Proxy API requests to the backend to avoid CORS in the browser
        if self.path.startswith('/api') or self.path == '/health':
            target = BACKEND + self.path
            headers = {k: v for k, v in self.headers.items()}
            req = urllib.request.Request(target, headers=headers)
            try:
                with urllib.request.urlopen(req) as resp:
                    self.send_response(resp.getcode())
                    for k, v in resp.getheaders():
                        # Skip hop-by-hop headers
                        if k.lower() in ('transfer-encoding', 'connection', 'keep-alive'):
                            continue
                        self.send_header(k, v)
                    self.end_headers()
                    self.wfile.write(resp.read())
            except urllib.error.HTTPError as e:
                self.send_response(e.code)
                self.end_headers()
                try:
                    self.wfile.write(e.read())
                except Exception:
                    pass
            except Exception as e:
                self.send_response(502)
                self.end_headers()
                self.wfile.write(str(e).encode())
        else:
            # Serve static files from the directory this script lives in
            return http.server.SimpleHTTPRequestHandler.do_GET(self)

if __name__ == '__main__':
    os.chdir(os.path.dirname(__file__) or '.')
    with socketserver.TCPServer(('', PORT), Handler) as httpd:
        print(f'Serving web_app on http://localhost:{PORT} (proxy to {BACKEND})')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('Shutting down')
            httpd.server_close()
