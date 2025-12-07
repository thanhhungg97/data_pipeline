"""
HTTP Server for serving the React dashboard.

Supports SPA (Single Page Application) routing by falling back to
index.html for routes that don't match actual files.
"""

import atexit
import http.server
import socketserver
import threading
from pathlib import Path


class DashboardServer:
    """Simple HTTP server to serve dashboard without CORS issues.

    Supports SPA (Single Page Application) routing by falling back to
    index.html for routes that don't match actual files.
    """

    def __init__(self, directory: str, port: int = 8765):
        self.port = port
        self.directory = directory
        self.server = None
        self.thread = None

    def start(self) -> int:
        """Start server and return the port."""
        directory = self.directory

        class SPAHandler(http.server.SimpleHTTPRequestHandler):
            """Handler with SPA fallback support."""

            def __init__(self, *args, **kwargs):
                super().__init__(*args, directory=directory, **kwargs)

            def do_GET(self):
                try:
                    # Try to serve the file normally first
                    path = self.translate_path(self.path)

                    # If file exists, serve it
                    if Path(path).exists():
                        return super().do_GET()

                    # For SPA routes (no file extension), serve index.html
                    # This allows React Router to handle the routing
                    if "." not in Path(self.path).name:
                        self.path = "/index.html"

                    return super().do_GET()
                except Exception as e:
                    print(f"[DashboardServer] Error handling {self.path}: {e}")
                    self.send_error(500, str(e))

        # Find available port
        for port in range(self.port, self.port + 100):
            try:
                self.server = socketserver.TCPServer(("127.0.0.1", port), SPAHandler)
                self.port = port
                break
            except OSError:
                continue
        else:
            raise RuntimeError("No available port found")

        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        return self.port

    def stop(self):
        """Stop the server."""
        if self.server:
            self.server.shutdown()
            self.server = None

    def get_url(self) -> str:
        """Get the dashboard URL."""
        return f"http://localhost:{self.port}"


# Global server instance (reused across runs)
_dashboard_server: DashboardServer | None = None


def get_dashboard_server(directory: str) -> DashboardServer:
    """Get or create dashboard server for the given directory."""
    global _dashboard_server

    if _dashboard_server is not None:
        # Stop existing server if directory changed
        if _dashboard_server.directory != directory:
            _dashboard_server.stop()
            _dashboard_server = None

    if _dashboard_server is None:
        _dashboard_server = DashboardServer(directory)
        _dashboard_server.start()
        # Register cleanup on exit
        atexit.register(lambda: _dashboard_server.stop() if _dashboard_server else None)

    return _dashboard_server
