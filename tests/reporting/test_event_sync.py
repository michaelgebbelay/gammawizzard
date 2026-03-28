"""Tests for reporting.ingest S3 event sync."""

from __future__ import annotations

from pathlib import Path

from reporting.ingest import sync_events_from_s3


class _FakePaginator:
    def paginate(self, Bucket, Prefix):
        yield {
            "Contents": [
                {"Key": f"{Prefix}constantstable_schwab_run1.jsonl", "Size": 13},
                {"Key": f"{Prefix}ignore.txt", "Size": 4},
            ]
        }


class _FakeS3:
    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator()

    def download_file(self, bucket, key, dest):
        Path(dest).write_text("hello world!\n")


def test_sync_events_from_s3_downloads_jsonl(monkeypatch, tmp_path):
    monkeypatch.setenv("GAMMA_EVENT_BUCKET", "unit-test-bucket")
    monkeypatch.setenv("GAMMA_EVENT_PREFIX", "reporting/events")
    monkeypatch.setenv("GAMMA_EVENT_DIR", str(tmp_path))
    monkeypatch.setattr("reporting.ingest.boto3.client", lambda service: _FakeS3())

    stats = sync_events_from_s3("2026-03-13")

    downloaded = tmp_path / "2026-03-13" / "constantstable_schwab_run1.jsonl"
    assert downloaded.exists()
    assert downloaded.read_text() == "hello world!\n"
    assert stats["bucket"] == "unit-test-bucket"
    assert stats["dates"] == 1
    assert stats["objects"] == 1
    assert stats["downloaded"] == 1
    assert stats["errors"] == 0


def test_sync_events_from_s3_skips_same_size_file(monkeypatch, tmp_path):
    local_dir = tmp_path / "2026-03-13"
    local_dir.mkdir(parents=True)
    existing = local_dir / "constantstable_schwab_run1.jsonl"
    existing.write_text("hello world!\n")

    monkeypatch.setenv("GAMMA_EVENT_BUCKET", "unit-test-bucket")
    monkeypatch.setenv("GAMMA_EVENT_PREFIX", "reporting/events")
    monkeypatch.setenv("GAMMA_EVENT_DIR", str(tmp_path))
    monkeypatch.setattr("reporting.ingest.boto3.client", lambda service: _FakeS3())

    stats = sync_events_from_s3("2026-03-13")

    assert stats["objects"] == 1
    assert stats["downloaded"] == 0
    assert stats["skipped"] == 1
