from __future__ import annotations

import http.server
import socket
import threading
from pathlib import Path

from emailbot.extraction import extract_from_url


def _run_server(files: dict[str, tuple[bytes, dict[str, str]]]):
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body, headers = files.get(self.path, (b"", {}))
            self.send_response(200)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):  # noqa: D401, override
            return

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server, port


def _serve(root: Path, mapping: dict[str, str]):
    files: dict[str, tuple[bytes, dict[str, str]]] = {}
    for url, rel in mapping.items():
        p = root / rel
        data = p.read_bytes()
        headers = {"Content-Type": "text/html; charset=utf-8"}
        if rel.endswith(".js"):
            headers["Content-Type"] = "application/javascript"
        elif rel.endswith(".txt"):
            headers["Content-Type"] = "text/plain; charset=utf-8"
        elif rel.endswith(".xml"):
            headers["Content-Type"] = "application/xml"
        elif rel.endswith("robots.txt"):
            headers["Content-Type"] = "text/plain"
        files[url] = (data, headers)
    return _run_server(files)


def test_jsonld_extraction(tmp_path):
    root = Path(__file__).parent / "fixtures"
    server, port = _serve(root, {"/": "html/jsonld.html"})
    try:
        hits, stats = extract_from_url(f"http://127.0.0.1:{port}/")
    finally:
        server.shutdown()
    emails = [h.email for h in hits]
    assert "contact@site.com" in emails
    assert any(h.origin == "ldjson" for h in hits)


def test_next_hydration(tmp_path):
    root = Path(__file__).parent / "fixtures"
    server, port = _serve(root, {"/": "html/next.html"})
    try:
        hits, stats = extract_from_url(f"http://127.0.0.1:{port}/")
    finally:
        server.shutdown()
    emails = {h.email for h in hits}
    assert "next@site.com" in emails


def test_bundle_and_spa(tmp_path):
    root = Path(__file__).parent / "fixtures"
    mapping = {
        "/": "html/spa.html",
        "/assets/app.js": "assets/app.js",
    }
    server, port = _serve(root, mapping)
    try:
        hits, stats = extract_from_url(f"http://127.0.0.1:{port}/")
    finally:
        server.shutdown()
    emails = {h.email for h in hits}
    assert "contact@site.com" in emails
    assert any(h.origin == "bundle" for h in hits)


def test_api_and_sitemap(tmp_path):
    root = Path(__file__).parent / "fixtures"
    # Prepare robots and sitemap with actual port numbers
    robots = (root / "robots.txt").read_text().replace("PORT", "{port}")
    sitemap = (root / "sitemap.xml").read_text().replace("PORT", "{port}")
    files = {
        "/": root.joinpath("html/links.html").read_bytes(),
        "/api/files/document/policy.txt": root.joinpath("docs/policy.txt").read_bytes(),
        "/robots.txt": robots.encode(),
        "/sitemap.xml": sitemap.encode(),
        "/docs/policy.txt": root.joinpath("docs/policy.txt").read_bytes(),
    }
    # We'll fill port later after server started
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            body = files.get(self.path)
            if body is None:
                self.send_response(404)
                self.end_headers()
                return
            data = body
            headers = {"Content-Type": "text/plain; charset=utf-8"}
            if self.path.endswith(".html"):
                headers["Content-Type"] = "text/html; charset=utf-8"
            elif self.path.endswith(".xml"):
                headers["Content-Type"] = "application/xml"
            self.send_response(200)
            for k, v in headers.items():
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def log_message(self, *a):
            return

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    # replace port placeholders
    files["/robots.txt"] = robots.replace("{port}", str(port)).encode()
    files["/sitemap.xml"] = sitemap.replace("{port}", str(port)).encode()

    server = http.server.ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    try:
        hits, stats = extract_from_url(f"http://127.0.0.1:{port}/")
    finally:
        server.shutdown()
    emails = {h.email for h in hits}
    assert "doc@site.com" in emails
    # Should capture emails from API (heuristic) and sitemap
    assert stats["hits_api"] >= 1
    assert stats["hits_sitemap"] >= 1

