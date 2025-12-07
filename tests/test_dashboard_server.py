"""Tests for DashboardServer file serving functionality."""

import sys
import tempfile
import time
from pathlib import Path

import pytest
import requests

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import dashboard_server as dashboard_server_module
from dashboard_server import DashboardServer, get_dashboard_server

# ============================================================
# Test Fixtures
# ============================================================


@pytest.fixture
def temp_dashboard_dir():
    """Create temp dir with mock dashboard files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create basic files
        Path(tmpdir, "index.html").write_text("<html><body>test</body></html>")
        Path(tmpdir, "data.json").write_text('{"test": true}')

        # Create nested assets directory
        assets_dir = Path(tmpdir, "assets")
        assets_dir.mkdir()
        Path(assets_dir, "app.js").write_text("console.log('test')")
        Path(assets_dir, "style.css").write_text("body { color: red; }")

        yield tmpdir


@pytest.fixture
def server(temp_dashboard_dir):
    """Create and start a DashboardServer for testing."""
    srv = DashboardServer(temp_dashboard_dir)
    srv.start()
    # Give server time to start
    time.sleep(0.1)
    yield srv
    srv.stop()


# ============================================================
# Tests
# ============================================================


class TestServerBasics:
    """Test basic server functionality."""

    def test_server_starts_and_returns_port(self, temp_dashboard_dir):
        """Server should start and return a valid port."""
        srv = DashboardServer(temp_dashboard_dir)
        port = srv.start()
        try:
            assert port >= 8765
            assert port < 8865  # Within range of 100 ports
        finally:
            srv.stop()

    def test_server_returns_correct_url(self, server):
        """Server should return correct localhost URL."""
        url = server.get_url()
        assert url.startswith("http://localhost:")
        assert str(server.port) in url

    def test_server_stops_cleanly(self, temp_dashboard_dir):
        """Server should stop without errors."""
        srv = DashboardServer(temp_dashboard_dir)
        srv.start()
        srv.stop()
        assert srv.server is None


class TestFileServing:
    """Test file serving functionality - PRIMARY FOCUS."""

    def test_serves_index_html(self, server):
        """Server should serve index.html from root."""
        resp = requests.get(f"{server.get_url()}/index.html", timeout=5)
        assert resp.status_code == 200
        assert "<html>" in resp.text
        assert "test" in resp.text

    def test_serves_data_json(self, server):
        """Server should serve data.json with correct content."""
        resp = requests.get(f"{server.get_url()}/data.json", timeout=5)
        assert resp.status_code == 200
        data = resp.json()
        assert data == {"test": True}

    def test_serves_nested_js_file(self, server):
        """Server should serve nested JavaScript files."""
        resp = requests.get(f"{server.get_url()}/assets/app.js", timeout=5)
        assert resp.status_code == 200
        assert "console.log" in resp.text

    def test_serves_nested_css_file(self, server):
        """Server should serve nested CSS files."""
        resp = requests.get(f"{server.get_url()}/assets/style.css", timeout=5)
        assert resp.status_code == 200
        assert "body" in resp.text

    def test_404_for_nonexistent_file(self, server):
        """Server should return 404 for missing files."""
        resp = requests.get(f"{server.get_url()}/nonexistent.txt", timeout=5)
        assert resp.status_code == 404

    def test_serves_from_correct_directory(self, server, temp_dashboard_dir):
        """Server should serve from specified directory, not cwd."""
        # Create a file that only exists in temp dir
        unique_file = Path(temp_dashboard_dir) / "unique_test_file.txt"
        unique_file.write_text("unique content 12345")

        resp = requests.get(f"{server.get_url()}/unique_test_file.txt", timeout=5)
        assert resp.status_code == 200
        assert "unique content 12345" in resp.text


class TestDirectoryHandling:
    """Test directory change handling."""

    def test_get_dashboard_server_creates_server(self, temp_dashboard_dir):
        """get_dashboard_server should create a new server."""
        if dashboard_server_module._dashboard_server:
            dashboard_server_module._dashboard_server.stop()
        dashboard_server_module._dashboard_server = None

        srv = get_dashboard_server(temp_dashboard_dir)
        try:
            assert srv is not None
            assert srv.directory == temp_dashboard_dir
        finally:
            srv.stop()
            dashboard_server_module._dashboard_server = None

    def test_get_dashboard_server_reuses_server(self, temp_dashboard_dir):
        """get_dashboard_server should reuse server for same directory."""
        if dashboard_server_module._dashboard_server:
            dashboard_server_module._dashboard_server.stop()
        dashboard_server_module._dashboard_server = None

        srv1 = get_dashboard_server(temp_dashboard_dir)
        srv2 = get_dashboard_server(temp_dashboard_dir)
        try:
            assert srv1 is srv2
        finally:
            srv1.stop()
            dashboard_server_module._dashboard_server = None

    def test_get_dashboard_server_switches_directory(self):
        """get_dashboard_server should create new server for different directory."""
        if dashboard_server_module._dashboard_server:
            dashboard_server_module._dashboard_server.stop()
        dashboard_server_module._dashboard_server = None

        with tempfile.TemporaryDirectory() as dir1:
            with tempfile.TemporaryDirectory() as dir2:
                Path(dir1, "index.html").write_text("dir1")
                Path(dir2, "index.html").write_text("dir2")

                get_dashboard_server(dir1)  # First server

                srv2 = get_dashboard_server(dir2)

                try:
                    # Should be different server instances
                    assert srv2.directory == dir2
                    # Verify serving from new directory
                    resp = requests.get(f"{srv2.get_url()}/index.html", timeout=5)
                    assert "dir2" in resp.text
                finally:
                    srv2.stop()
                    dashboard_server_module._dashboard_server = None


class TestConcurrentRequests:
    """Test handling of concurrent requests."""

    def test_handles_multiple_requests(self, server):
        """Server should handle multiple sequential requests."""
        for _ in range(10):
            resp = requests.get(f"{server.get_url()}/index.html", timeout=5)
            assert resp.status_code == 200


class TestSPARouting:
    """Test SPA (Single Page Application) routing support."""

    def test_spa_route_returns_index_html(self, server):
        """Non-existent routes should return index.html for SPA routing."""
        # This is what React Router needs - when navigating to /compare,
        # the server should serve index.html so React can handle the route
        resp = requests.get(f"{server.get_url()}/compare", timeout=5)
        assert resp.status_code == 200
        assert "<html>" in resp.text  # Should return index.html content

    def test_spa_nested_route_returns_index_html(self, server):
        """Nested SPA routes should also return index.html."""
        resp = requests.get(f"{server.get_url()}/some/nested/route", timeout=5)
        assert resp.status_code == 200
        assert "<html>" in resp.text

    def test_actual_files_still_served(self, server):
        """Real files should still be served directly, not index.html."""
        resp = requests.get(f"{server.get_url()}/data.json", timeout=5)
        assert resp.status_code == 200
        assert resp.json() == {"test": True}  # Not index.html content
