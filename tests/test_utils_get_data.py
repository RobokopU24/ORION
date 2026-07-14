import pytest

from orion import utils
from orion.utils import GetData, GetDataPullError


class FakeResponse:
    def __init__(self, content=b"", content_type="application/zip"):
        self.content = content
        self.headers = {"Content-Type": content_type}

    def raise_for_status(self):
        return None

    def iter_content(self, chunk_size):
        yield self.content

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False


class FakeSession:
    def __init__(self, download_response):
        self.calls = []
        self.download_response = download_response

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if len(self.calls) == 1:
            return FakeResponse()
        return self.download_response


def test_pull_via_http_session_gate_downloads_file(tmp_path, monkeypatch):
    fake_session = FakeSession(FakeResponse(content=b"zip-content"))
    monkeypatch.setattr(utils.requests, "Session", lambda: fake_session)

    bytes_read = GetData().pull_via_http_session_gate(
        "https://example.org/files/data.zip",
        str(tmp_path),
        "https://example.org/download.jsp",
        gate_params={"download_file": "/files/data.zip"},
        expected_content_type="application/zip",
    )

    assert bytes_read == len(b"zip-content")
    assert (tmp_path / "data.zip").read_bytes() == b"zip-content"
    assert not (tmp_path / "data.zip.part").exists()
    assert fake_session.calls[0] == (
        "https://example.org/download.jsp",
        {"params": {"download_file": "/files/data.zip"}, "timeout": 30},
    )
    assert fake_session.calls[1][0] == "https://example.org/files/data.zip"
    assert fake_session.calls[1][1]["stream"] is True


def test_pull_via_http_session_gate_skips_existing_file(tmp_path, monkeypatch):
    output_path = tmp_path / "renamed.zip"
    output_path.write_bytes(b"existing")

    def fail_if_called():
        raise AssertionError("Session should not be created when the output file exists")

    monkeypatch.setattr(utils.requests, "Session", fail_if_called)

    bytes_read = GetData().pull_via_http_session_gate(
        "https://example.org/files/data.zip",
        str(tmp_path),
        "https://example.org/download.jsp",
        saved_file_name="renamed.zip",
    )

    assert bytes_read == 1
    assert output_path.read_bytes() == b"existing"


def test_pull_via_http_session_gate_fails_on_unexpected_content_type(tmp_path, monkeypatch):
    fake_session = FakeSession(FakeResponse(content=b"<html></html>", content_type="text/html"))
    monkeypatch.setattr(utils.requests, "Session", lambda: fake_session)

    with pytest.raises(GetDataPullError):
        GetData().pull_via_http_session_gate(
            "https://example.org/files/data.zip",
            str(tmp_path),
            "https://example.org/download.jsp",
            expected_content_type="application/zip",
        )

    assert not (tmp_path / "data.zip").exists()
