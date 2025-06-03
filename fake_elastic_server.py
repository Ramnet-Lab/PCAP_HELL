#!/usr/bin/env python3
"""
Fake Elasticsearch server for testing curl bulk pushes.
"""
import logging
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from dotenv import load_dotenv
import os

load_dotenv()
es_node = os.getenv("ELASTICSEARCH_NODE", "http://localhost:9200")
parsed = urlparse(es_node)
host = parsed.hostname or "0.0.0.0"
port = parsed.port or 9200

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == "/pcap/_bulk":
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            logging.info(f"Received bulk data: {len(body)} bytes")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"took":1,"errors":false,"items":[]}')
        else:
            logging.warning(f"Unhandled path: {self.path}")
            self.send_response(404)
            self.end_headers()

if __name__ == "__main__":
    server = HTTPServer((host, port), Handler)
    logging.info(f"Fake Elasticsearch server listening on {host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down server")
        server.server_close()