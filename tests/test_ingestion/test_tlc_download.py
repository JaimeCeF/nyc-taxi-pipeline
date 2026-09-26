import os

import pytest

from ingestion import tlc_download as td

VALID_PARQUET = td.PARQUET_MAGIC + b"fake-body" + td.PARQUET_MAGIC


class FakeResponse:
    def __init__(self, status_code=200, body=b"", content_length=None):
        self.status_code = status_code
        self.body = body
        self.headers = {}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise td.requests.HTTPError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size):
        for i in range(0, len(self.body), chunk_size):
            yield self.body[i : i + chunk_size]


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.urls = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return self.response


def _dirs(tmp_path):
    dest = tmp_path / "landing" / "yellow"
    return str(dest), tmp_path / "landing" / "_tmp"


def test_build_url():
    assert td.build_url("yellow", 2024, 1) == (
        "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
    )


@pytest.mark.parametrize("trip_type, month", [("purple", 1), ("yellow", 0), ("yellow", 13)])
def test_build_filename_rejects_invalid(trip_type, month):
    with pytest.raises(ValueError):
        td.build_filename(trip_type, 2024, month)


def test_download_writes_file_and_cleans_tmp(tmp_path):
    dest, tmp_dir = _dirs(tmp_path)
    session = FakeSession(FakeResponse(body=VALID_PARQUET, content_length=len(VALID_PARQUET)))

    path = td.download_month(2024, 1, dest, session=session)

    assert path == os.path.join(dest, "yellow_tripdata_2024-01.parquet")
    with open(path, "rb") as f:
        assert f.read() == VALID_PARQUET
    assert list(tmp_dir.iterdir()) == []


def test_download_skips_existing_file(tmp_path):
    dest, _ = _dirs(tmp_path)
    os.makedirs(dest)
    existing = os.path.join(dest, "yellow_tripdata_2024-01.parquet")
    with open(existing, "wb") as f:
        f.write(VALID_PARQUET)
    session = FakeSession(FakeResponse(body=b"should not be fetched"))

    assert td.download_month(2024, 1, dest, session=session) == existing
    assert session.urls == []


@pytest.mark.parametrize("status", [403, 404])
def test_unpublished_month_returns_none(tmp_path, status):
    dest, tmp_dir = _dirs(tmp_path)
    session = FakeSession(FakeResponse(status_code=status))

    assert td.download_month(2099, 1, dest, session=session) is None
    assert os.listdir(dest) == []
    assert list(tmp_dir.iterdir()) == []


def test_server_error_raises(tmp_path):
    dest, _ = _dirs(tmp_path)
    with pytest.raises(td.requests.HTTPError):
        td.download_month(2024, 1, dest, session=FakeSession(FakeResponse(status_code=500)))


@pytest.mark.parametrize(
    "body, content_length",
    [
        (VALID_PARQUET[:-3], len(VALID_PARQUET)),  # truncated transfer
        (b"<html>not parquet</html>", None),  # wrong content
    ],
)
def test_invalid_download_leaves_no_file(tmp_path, body, content_length):
    dest, tmp_dir = _dirs(tmp_path)
    session = FakeSession(FakeResponse(body=body, content_length=content_length))

    with pytest.raises(td.DownloadError):
        td.download_month(2024, 1, dest, session=session)
    assert os.listdir(dest) == []
    assert list(tmp_dir.iterdir()) == []


def test_month_range_crosses_year_boundary():
    assert td.month_range((2023, 11), (2024, 2)) == [
        (2023, 11),
        (2023, 12),
        (2024, 1),
        (2024, 2),
    ]


def test_month_range_rejects_reversed():
    with pytest.raises(ValueError):
        td.month_range((2024, 2), (2024, 1))
