from __future__ import annotations

import asyncio
import importlib
import logging
from dataclasses import dataclass

import pytest
from PySide6.QtGui import QPaintEvent
from PySide6.QtWidgets import QApplication

from wip_management.presentation.ui.components import SparklineWidget


@dataclass
class MockSqlConnection:
    """Simple async SQL connection fixture for regression tests."""

    closed: bool = False

    async def fetchall(self, _query: str, *_params: object) -> list[dict[str, object]]:
        await asyncio.sleep(0)
        return []

    async def close(self) -> None:
        await asyncio.sleep(0)
        self.closed = True


@pytest.fixture
def qt_app() -> QApplication:
    app = QApplication.instance()
    if app is None:
        app = QApplication([])
    return app


@pytest.fixture
def mock_sql_connection() -> MockSqlConnection:
    return MockSqlConnection()


@pytest.fixture
def async_loop(event_loop):
    return event_loop


def test_bug_001_sparkline_paint_no_nameerror(qt_app, qtbot) -> None:
    widget = SparklineWidget(color="#2563eb")
    qtbot.addWidget(widget)
    widget.resize(180, 36)
    widget.set_data([1, 3, 2, 5, 4])

    event = QPaintEvent(widget.rect())
    widget.paintEvent(event)


def test_bug_002_widgets_import_theme_alias_compatible() -> None:
    module = importlib.import_module("wip_management.presentation.ui.widgets")
    assert module is not None


def test_bug_003_sparkline_internal_error_is_contained(qt_app, qtbot, monkeypatch, caplog) -> None:
    import wip_management.presentation.ui.components as components_module

    widget = SparklineWidget(color="#16a34a")
    qtbot.addWidget(widget)
    widget.resize(180, 36)
    widget.set_data([10, 20, 15, 25])

    def _raise_name_error(*_args, **_kwargs):
        raise NameError("QPointF is not defined")

    monkeypatch.setattr(components_module, "QPointF", _raise_name_error)

    with caplog.at_level(logging.ERROR):
        widget.paintEvent(QPaintEvent(widget.rect()))

    assert any("SparklineWidget paintEvent failed" in rec.getMessage() for rec in caplog.records)


@pytest.mark.asyncio
async def test_bug_004_mock_sql_fixture_async_works(mock_sql_connection: MockSqlConnection, async_loop) -> None:
    rows = await mock_sql_connection.fetchall("SELECT 1")
    await mock_sql_connection.close()
    assert rows == []
    assert mock_sql_connection.closed is True
