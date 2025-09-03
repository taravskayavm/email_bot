from __future__ import annotations

import http.server
import socket
import threading

from emailbot.extraction import extract_from_url


def _run_server(pages: dict[str, str]):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = pages.get(self.path, "").encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):  # noqa: D401, override
            return

    sock = socket.socket()
    sock.bind(("localhost", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = http.server.ThreadingHTTPServer(("localhost", port), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server, port


def _serve(html: str):
    pages = {"/": html}
    server, port = _run_server(pages)
    return server, f"http://localhost:{port}/"


def test_requires_at_token():
    html = "<html><body>121536 gmail dot com</body></html>"
    server, url = _serve(html)
    try:
        hits, _ = extract_from_url(url)
    finally:
        server.shutdown()
    emails = [h.email for h in hits]
    assert "121536@gmail.com" not in emails


def test_obfuscation_with_at():
    html = "<html><body>121536 at gmail dot com</body></html>"
    server, url = _serve(html)
    try:
        hits, _ = extract_from_url(url)
    finally:
        server.shutdown()
    emails = [h.email for h in hits]
    assert "121536@gmail.com" in emails


def test_numeric_local_kept():
    html = "<html><body>2@mail.ru and 2 at mail dot ru</body></html>"
    server, url = _serve(html)
    try:
        hits, stats = extract_from_url(url)
    finally:
        server.shutdown()
    emails = [h.email for h in hits]
    assert "2@mail.ru" in emails
    assert stats["obfuscated_hits"] >= 1

