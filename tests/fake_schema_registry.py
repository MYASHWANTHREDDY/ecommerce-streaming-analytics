"""A minimal in-process fake Schema Registry, used only by the test suite.

Why this exists: producer/events.py's AvroSerializer needs a reachable Schema
Registry to resolve/register a schema ID before it will locally validate+encode a
record (confirmed empirically -- the network call happens before the local fastavro
encode step, not after). CI runs `pytest tests/ -v` with no live services at all (see
.github/workflows/ci.yml), so the test suite can't depend on a real
confluentinc/cp-schema-registry container. This fake implements just enough of the
REST surface (POST /subjects/{subject}/versions, GET .../versions/latest,
GET /schemas/ids/{id}) for AvroSerializer's real, unmodified code path to work against
it -- these tests exercise the actual confluent-kafka library, not a stand-in for it.
"""

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse


class _Handler(BaseHTTPRequestHandler):
    schema: str = ""
    schema_id: int = 1

    def log_message(self, *args):
        pass  # keep test output quiet

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(length)
        path = urlparse(self.path).path
        if path.startswith("/subjects/") and path.endswith("/versions"):
            self._send(200, {"id": self.schema_id})
        else:
            self._send(404, {"error_code": 40401, "message": f"not found: {path}"})

    def do_GET(self):
        path = urlparse(self.path).path
        if path.endswith("/versions/latest"):
            self._send(
                200,
                {"subject": "orders-value", "id": self.schema_id, "version": 1, "schema": self.schema},
            )
        elif "/schemas/ids/" in path:
            self._send(200, {"schema": self.schema})
        else:
            self._send(404, {"error_code": 40401, "message": f"not found: {path}"})


def start(schema: str, schema_id: int = 1) -> str:
    """Starts the fake registry on a free local port and returns its base URL.
    Runs as a daemon thread for the lifetime of the test process -- never explicitly
    stopped, which is fine for a test-only, localhost-only server."""
    handler = type("_BoundHandler", (_Handler,), {"schema": schema, "schema_id": schema_id})
    server = HTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    port = server.server_address[1]
    return f"http://127.0.0.1:{port}"
