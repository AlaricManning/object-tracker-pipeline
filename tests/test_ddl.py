"""Tests for the generated Athena DDL."""

import pyarrow as pa
import pytest

from object_tracker_pipeline import ddl
from object_tracker_pipeline.transform import SCHEMA

LOCATION = "s3://bucket/catalog/detections/"


def test_every_non_partition_column_appears():
    generated = ddl.generate_ddl(LOCATION)
    for field in SCHEMA:
        if field.name != ddl.PARTITION_COLUMN:
            assert f"`{field.name}`" in generated


def test_type_mapping():
    generated = ddl.generate_ddl(LOCATION)
    assert "`session_id` string" in generated
    assert "`frame_id` int" in generated
    assert "`track_id` bigint" in generated
    assert "`confidence` float" in generated
    assert "`timestamp` timestamp" in generated


def test_date_is_partition_not_column():
    generated = ddl.generate_ddl(LOCATION)
    assert "PARTITIONED BY (`date` date)" in generated
    columns_block = generated.split("PARTITIONED BY")[0]
    assert "`date`" not in columns_block


def test_partition_projection_properties():
    generated = ddl.generate_ddl("s3://bucket/catalog/detections")
    assert "'projection.enabled' = 'true'" in generated
    assert "'projection.date.range' = '2026-07-01,NOW'" in generated
    assert (
        "'storage.location.template' = 's3://bucket/catalog/detections/date=${date}'"
    ) in generated


def test_location_gets_exactly_one_trailing_slash():
    with_slash = ddl.generate_ddl("s3://bucket/detections/")
    without = ddl.generate_ddl("s3://bucket/detections")
    assert with_slash == without
    assert "LOCATION 's3://bucket/detections/'" in with_slash


def test_unmapped_arrow_type_raises():
    with pytest.raises(ValueError, match="no Athena type mapping"):
        ddl.athena_type(pa.field("mystery", pa.float64()))


def test_cli_prints_ddl(capsys):
    assert ddl.main(["--location", LOCATION]) == 0
    out = capsys.readouterr().out
    assert out.startswith("CREATE EXTERNAL TABLE IF NOT EXISTS")
    assert "`object_tracker`.`detections`" in out


def test_cli_location_falls_back_to_env(capsys, monkeypatch):
    monkeypatch.setenv("OBJECT_TRACKER_BUCKET", "env-bucket")
    assert ddl.main([]) == 0
    out = capsys.readouterr().out
    assert "LOCATION 's3://env-bucket/catalog/detections/'" in out


def test_cli_errors_without_location_or_env(monkeypatch, capsys):
    monkeypatch.delenv("OBJECT_TRACKER_BUCKET", raising=False)
    with pytest.raises(SystemExit):
        ddl.main([])
    assert "OBJECT_TRACKER_BUCKET" in capsys.readouterr().err
