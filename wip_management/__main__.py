# wip_management/__main__.py
"""
WIP Management - Modern UI with Enhanced User Experience
"""
from __future__ import annotations

import asyncio
import contextlib
import faulthandler
import logging
import logging.handlers
import os
from pathlib import Path
import re
import sys
import threading
import time
from datetime import datetime, timedelta
from typing import Any, Callable

if os.name == "nt":
    import msvcrt

from PySide6.QtCore import (
    QEvent, QModelIndex, QObject, QPoint, QPointF, QRectF, QSize, QSignalBlocker, Qt, QTimer, Signal
)
from PySide6.QtGui import (
    QColor, QFont, QFontMetrics, QKeySequence, QPainter, QPen, QShortcut, QIcon
)
from PySide6.QtWidgets import (
    QAbstractItemView,
    QApplication,
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QDoubleSpinBox,
    QFrame,
    QFormLayout,
    QGraphicsDropShadowEffect,
    QGridLayout,
    QHeaderView,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListView,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMessageBox,
    QProgressBar,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QSpacerItem,
    QSpinBox,
    QSplitter,
    QStackedWidget,
    QStatusBar,
    QStyledItemDelegate,
    QStyle,
    QStyleOptionViewItem,
    QTabWidget,
    QTableView,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from wip_management.application.services.orchestrator import OrchestratorService
from wip_management.application.state.state_store import SingleWriterStateStore
from wip_management.config import settings
from wip_management.domain.events import SnapshotReady, TrolleyUpdated, TrayUpdated
from wip_management.domain.models.trolley import Column
from wip_management.infrastructure.messaging.event_bus import AsyncEventBus
from wip_management.infrastructure.persistence.state_repo import SharedGroupingStateRepository
from wip_management.infrastructure.sqlserver.ccu_repo import CcuRepo
from wip_management.infrastructure.sqlserver.connection import SQLServerConnection, build_driver_candidates
from wip_management.infrastructure.sqlserver.dashboard_repo import DashboardRepo
from wip_management.infrastructure.sqlserver.delta_tracker import InMemoryDeltaTracker
from wip_management.infrastructure.sqlserver.fpc_repo import FpcRepo
from wip_management.presentation.ui.viewmodels import BoardViewModel, TrolleyListModel, UNGROUP_CHECK_VISUAL_ROLE
from wip_management.presentation.ui.mapper import parse_datetime

log = logging.getLogger(__name__)
_MAX_UI_WINDOW_DAYS = 365
_GRACEFUL_STOP_TIMEOUT_SECONDS = 150.0
_FORCE_STOP_TIMEOUT_SECONDS = 10.0

# ═══════════════════════════════════════════════════════════════════════════════
# THEME SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

class ThemeColors:
    """Modern color palette"""
    # Primary
    PRIMARY = "#2563eb"
    PRIMARY_HOVER = "#1d4ed8"
    PRIMARY_LIGHT = "#dbeafe"
    
    # Status
    SUCCESS = "#16a34a"
    SUCCESS_BG = "#dcfce7"
    WARNING = "#d97706"
    WARNING_BG = "#fef3c7"
    DANGER = "#dc2626"
    DANGER_BG = "#fee2e2"
    INFO = "#0891b2"
    INFO_BG = "#cffafe"
    
    # Neutral
    BG = "#f8fafc"
    SURFACE = "#ffffff"
    SURFACE_HOVER = "#f1f5f9"
    BORDER = "#e2e8f0"
    BORDER_LIGHT = "#f1f5f9"
    
    # Text
    TEXT = "#0f172a"
    TEXT_SECONDARY = "#475569"
    TEXT_MUTED = "#94a3b8"
    TEXT_INVERSE = "#ffffff"
    
    # Shadows
    SHADOW = "rgba(0, 0, 0, 0.08)"


def get_modern_stylesheet() -> str:
    """Generate complete modern stylesheet"""
    c = ThemeColors
    return f"""
    /* ═══════════════════════════════════════════════════════════════
       GLOBAL STYLES
       ═══════════════════════════════════════════════════════════════ */
    
    QMainWindow {{
        background: {c.BG};
    }}
    
    QWidget {{
        font-family: 'Segoe UI', 'SF Pro Display', -apple-system, sans-serif;
        font-size: 13px;
        color: {c.TEXT};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       METRIC CARDS
       ═══════════════════════════════════════════════════════════════ */
    
    QFrame#metricCard {{
        background: {c.SURFACE};
        border: 1px solid {c.BORDER};
        border-radius: 16px;
    }}
    
    QFrame#metricCard:hover {{
        border-color: {c.PRIMARY};
    }}
    
    QLabel#metricIcon {{
        font-size: 24px;
        padding: 0;
    }}
    
    QLabel#metricValue {{
        color: {c.TEXT};
        font-size: 32px;
        font-weight: 700;
        letter-spacing: -1px;
    }}
    
    QLabel#metricTitle {{
        color: {c.TEXT_SECONDARY};
        font-size: 12px;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }}
    
    QLabel#metricSubtitle {{
        color: {c.TEXT_MUTED};
        font-size: 11px;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       COLUMN CARDS
       ═══════════════════════════════════════════════════════════════ */
    
    QFrame#columnCard {{
        background: {c.SURFACE};
        border: 1px solid {c.BORDER};
        border-radius: 16px;
    }}
    
    QLabel#columnHeader {{
        color: {c.TEXT};
        font-size: 16px;
        font-weight: 600;
    }}
    
    QLabel#columnStats {{
        color: {c.TEXT_MUTED};
        font-size: 12px;
    }}
    
    QFrame#columnHeaderFrame {{
        background: transparent;
        border-bottom: 1px solid {c.BORDER};
        border-radius: 0;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       BUTTONS
       ═══════════════════════════════════════════════════════════════ */
    
    QPushButton {{
        background: {c.PRIMARY};
        color: {c.TEXT_INVERSE};
        border: none;
        border-radius: 10px;
        padding: 10px 20px;
        font-weight: 600;
        font-size: 13px;
        min-height: 20px;
    }}
    
    QPushButton:hover {{
        background: {c.PRIMARY_HOVER};
    }}
    
    QPushButton:pressed {{
        background: #1e40af;
    }}
    
    QPushButton:disabled {{
        background: {c.BORDER};
        color: {c.TEXT_MUTED};
    }}
    
    QPushButton#secondaryBtn {{
        background: {c.SURFACE};
        color: {c.TEXT};
        border: 1px solid {c.BORDER};
    }}
    
    QPushButton#secondaryBtn:hover {{
        background: {c.SURFACE_HOVER};
        border-color: {c.PRIMARY};
    }}
    
    QPushButton#iconBtn {{
        background: transparent;
        border: none;
        border-radius: 8px;
        padding: 8px;
        min-width: 36px;
        max-width: 36px;
        min-height: 36px;
        max-height: 36px;
        font-size: 16px;
    }}
    
    QPushButton#iconBtn:hover {{
        background: {c.SURFACE_HOVER};
    }}
    
    QPushButton#successBtn {{
        background: {c.SUCCESS};
    }}
    
    QPushButton#successBtn:hover {{
        background: #15803d;
    }}
    
    QPushButton#dangerBtn {{
        background: {c.DANGER};
    }}
    
    QPushButton#dangerBtn:hover {{
        background: #b91c1c;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       INPUT FIELDS
       ═══════════════════════════════════════════════════════════════ */
    
    QLineEdit {{
        background: {c.SURFACE};
        border: 1px solid {c.BORDER};
        border-radius: 10px;
        padding: 10px 14px;
        font-size: 13px;
        color: {c.TEXT};
        selection-background-color: {c.PRIMARY_LIGHT};
    }}
    
    QLineEdit:focus {{
        border-color: {c.PRIMARY};
    }}
    
    QLineEdit:disabled {{
        background: {c.BORDER_LIGHT};
        color: {c.TEXT_MUTED};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       CHECKBOXES
       ═══════════════════════════════════════════════════════════════ */
    
    QCheckBox {{
        spacing: 8px;
        color: {c.TEXT};
        font-weight: 500;
    }}
    
    QCheckBox::indicator {{
        width: 20px;
        height: 20px;
        border-radius: 6px;
        border: 2px solid {c.BORDER};
        background: {c.SURFACE};
    }}
    
    QCheckBox::indicator:hover {{
        border-color: {c.PRIMARY};
    }}
    
    QCheckBox::indicator:checked {{
        background: {c.PRIMARY};
        border-color: {c.PRIMARY};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       LIST VIEWS
       ═══════════════════════════════════════════════════════════════ */
    
    QListView {{
        background: transparent;
        border: none;
        outline: none;
        padding: 4px;
    }}
    
    QListView::item {{
        background: {c.SURFACE};
        border: 1px solid {c.BORDER_LIGHT};
        border-radius: 12px;
        padding: 14px 16px;
        margin: 4px 8px;
        color: {c.TEXT};
    }}
    
    QListView::item:hover {{
        background: {c.SURFACE_HOVER};
        border-color: {c.BORDER};
    }}
    
    QListView::item:selected {{
        background: {c.PRIMARY_LIGHT};
        border-color: {c.PRIMARY};
        color: {c.TEXT};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       TABLE VIEWS
       ═══════════════════════════════════════════════════════════════ */
    
    QTableView {{
        background: transparent;
        border: none;
        gridline-color: {c.BORDER_LIGHT};
        selection-background-color: {c.PRIMARY_LIGHT};
        selection-color: {c.TEXT};
    }}
    
    QTableView::item {{
        padding: 10px 12px;
        border: none;
    }}
    
    QTableView::item:hover {{
        background: {c.SURFACE_HOVER};
    }}
    
    QTableView::item:selected {{
        background: {c.PRIMARY_LIGHT};
    }}
    
    QHeaderView {{
        background: transparent;
        border: none;
    }}
    
    QHeaderView::section {{
        background: {c.SURFACE};
        color: {c.TEXT_SECONDARY};
        font-weight: 600;
        font-size: 11px;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        padding: 12px 8px;
        border: none;
        border-bottom: 2px solid {c.BORDER};
    }}
    
    QHeaderView::section:hover {{
        background: {c.SURFACE_HOVER};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       SCROLLBARS
       ═══════════════════════════════════════════════════════════════ */
    
    QScrollBar:vertical {{
        background: transparent;
        width: 8px;
        margin: 0;
    }}
    
    QScrollBar::handle:vertical {{
        background: {c.BORDER};
        border-radius: 4px;
        min-height: 40px;
    }}
    
    QScrollBar::handle:vertical:hover {{
        background: {c.TEXT_MUTED};
    }}
    
    QScrollBar::add-line:vertical,
    QScrollBar::sub-line:vertical {{
        height: 0;
    }}
    
    QScrollBar:horizontal {{
        background: transparent;
        height: 8px;
        margin: 0;
    }}
    
    QScrollBar::handle:horizontal {{
        background: {c.BORDER};
        border-radius: 4px;
        min-width: 40px;
    }}
    
    QScrollBar::handle:horizontal:hover {{
        background: {c.TEXT_MUTED};
    }}
    
    QScrollBar::add-line:horizontal,
    QScrollBar::sub-line:horizontal {{
        width: 0;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       STATUS BAR
       ═══════════════════════════════════════════════════════════════ */
    
    QStatusBar {{
        background: {c.SURFACE};
        border-top: 1px solid {c.BORDER};
    }}
    
    QStatusBar QLabel {{
        color: {c.TEXT_SECONDARY};
        font-size: 12px;
        padding: 4px 8px;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       TOOLTIPS
       ═══════════════════════════════════════════════════════════════ */
    
    QToolTip {{
        background: {c.TEXT};
        color: {c.TEXT_INVERSE};
        border: none;
        border-radius: 8px;
        padding: 8px 12px;
        font-size: 12px;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       MENUS
       ═══════════════════════════════════════════════════════════════ */
    
    QMenu {{
        background: {c.SURFACE};
        border: 1px solid {c.BORDER};
        border-radius: 12px;
        padding: 8px;
    }}
    
    QMenu::item {{
        padding: 10px 32px 10px 16px;
        border-radius: 8px;
        color: {c.TEXT};
    }}
    
    QMenu::item:selected {{
        background: {c.SURFACE_HOVER};
    }}
    
    QMenu::separator {{
        height: 1px;
        background: {c.BORDER};
        margin: 6px 12px;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       DIALOGS
       ═══════════════════════════════════════════════════════════════ */
    
    QDialog {{
        background: {c.BG};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       SPIN BOXES
       ═══════════════════════════════════════════════════════════════ */
    
    QSpinBox, QDoubleSpinBox {{
        background: {c.SURFACE};
        border: 1px solid {c.BORDER};
        border-radius: 8px;
        padding: 8px 12px;
        color: {c.TEXT};
    }}
    
    QSpinBox:focus, QDoubleSpinBox:focus {{
        border-color: {c.PRIMARY};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       PROGRESS BAR
       ═══════════════════════════════════════════════════════════════ */
    
    QProgressBar {{
        background: {c.BORDER_LIGHT};
        border: none;
        border-radius: 4px;
        height: 8px;
        text-align: center;
    }}
    
    QProgressBar::chunk {{
        background: {c.PRIMARY};
        border-radius: 4px;
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       TAB WIDGET
       ═══════════════════════════════════════════════════════════════ */
    
    QTabWidget::pane {{
        border: 1px solid {c.BORDER};
        border-radius: 12px;
        background: {c.SURFACE};
    }}
    
    QTabBar::tab {{
        background: transparent;
        color: {c.TEXT_SECONDARY};
        padding: 12px 24px;
        font-weight: 500;
        border: none;
        border-bottom: 2px solid transparent;
    }}
    
    QTabBar::tab:hover {{
        color: {c.TEXT};
    }}
    
    QTabBar::tab:selected {{
        color: {c.PRIMARY};
        border-bottom-color: {c.PRIMARY};
    }}
    
    /* ═══════════════════════════════════════════════════════════════
       SPLITTER
       ═══════════════════════════════════════════════════════════════ */
    
    QSplitter::handle {{
        background: {c.BORDER};
        width: 2px;
        margin: 4px 2px;
        border-radius: 1px;
    }}
    
    QSplitter::handle:hover {{
        background: {c.PRIMARY};
    }}
    """


# ═══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS & CLASSES (Keep original implementations)
# ═══════════════════════════════════════════════════════════════════════════════

def _configure_logging() -> None:
    level_name = settings.log_level.upper().strip() or "INFO"
    level = getattr(logging, level_name, logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(name)s [%(threadName)s] %(message)s",
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level)
    if settings.log_to_console:
        console_handler = logging.StreamHandler(stream=sys.stdout)
        console_handler.setFormatter(formatter)
        root.addHandler(console_handler)
    log_path = settings.log_file.strip()
    if log_path:
        path = Path(log_path)
        if not path.is_absolute():
            path = Path.cwd() / path
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            filename=path,
            maxBytes=20 * 1024 * 1024,
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        root.addHandler(file_handler)
    logging.captureWarnings(True)
    log.info("Logging configured level=%s console=%s file=%s", level_name, settings.log_to_console, log_path or "<disabled>")


def _install_crash_hooks() -> None:
    def _global_excepthook(exc_type, exc_value, exc_traceback) -> None:
        log.critical("Unhandled exception on main thread", exc_info=(exc_type, exc_value, exc_traceback))

    def _thread_excepthook(args) -> None:
        log.critical("Unhandled exception on thread=%s", args.thread.name if args.thread else "<unknown>", exc_info=(args.exc_type, args.exc_value, args.exc_traceback))

    sys.excepthook = _global_excepthook
    threading.excepthook = _thread_excepthook
    with contextlib.suppress(Exception):
        faulthandler.enable(all_threads=True)


class _SystemClock:
    def now(self) -> datetime:
        return datetime.now()


def _lookback_hours_for_days(days: int) -> float:
    normalized_days = max(1, min(_MAX_UI_WINDOW_DAYS, int(days)))
    return float(normalized_days * 24 + 2)


def _sync_data_window_settings() -> None:
    days = int(getattr(settings, "ui_data_window_days", 1))
    days = max(1, min(_MAX_UI_WINDOW_DAYS, days))
    settings.ui_data_window_days = days
    current = float(getattr(settings, "initial_load_lookback_hours", 0.0))
    min_reasonable = float(days * 24)
    max_reasonable = float(days * 24 + 4)
    if not (min_reasonable <= current <= max_reasonable):
        lookback = _lookback_hours_for_days(days)
        settings.initial_load_lookback_hours = lookback
        settings.ccu_backfill_lookback_hours = lookback
    else:
        settings.ccu_backfill_lookback_hours = current


def _format_timedelta(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _signature(value: Any) -> Any:
    if isinstance(value, dict):
        return tuple((str(key), _signature(val)) for key, val in sorted(value.items(), key=lambda item: str(item[0])))
    if isinstance(value, list):
        return tuple(_signature(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_signature(item) for item in value)
    if isinstance(value, set):
        return tuple(sorted(_signature(item) for item in value))
    return value


def _aging_status_text(delta: timedelta) -> str:
    age_hours = max(delta.total_seconds(), 0.0) / 3600.0
    target = max(float(settings.target_aging_hours), 0.0)
    tolerance = max(float(settings.target_aging_tolerance_hours), 0.0)
    min_target = max(target - tolerance, 0.0)
    max_target = target + tolerance
    if age_hours < min_target:
        return "Aging"
    if age_hours <= max_target:
        return "Aged"
    return "Aged Out"


def _upsert_env_values(path: Path, values: dict[str, str]) -> None:
    normalized = {key.strip(): value for key, value in values.items() if key.strip()}
    if not normalized:
        return
    existing_lines: list[str] = []
    if path.exists():
        existing_lines = path.read_text(encoding="utf-8").splitlines()
    updated_lines: list[str] = []
    seen_keys: set[str] = set()
    for line in existing_lines:
        raw = line.strip()
        if not raw or raw.startswith("#") or "=" not in line:
            updated_lines.append(line)
            continue
        key, _, _ = line.partition("=")
        env_key = key.strip()
        if env_key in normalized:
            updated_lines.append(f"{env_key}={normalized[env_key]}")
            seen_keys.add(env_key)
        else:
            updated_lines.append(line)
    for env_key, value in normalized.items():
        if env_key in seen_keys:
            continue
        updated_lines.append(f"{env_key}={value}")
    path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")


_TRO_ID_PATTERN = re.compile(r"^TRO-(\d+)$", re.IGNORECASE)


class _UiBridge(QObject):
    event_ready = Signal(object)


class _UiActionBridge(QObject):
    action_done = Signal(str, object, object)


class _LeaderFileLock:
    def __init__(self, lock_path: Path) -> None:
        self._path = lock_path
        self._fh = None

    def try_acquire(self) -> bool:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self._path, "a+b")
        try:
            fh.seek(0, os.SEEK_END)
            if fh.tell() == 0:
                fh.write(b" ")
                fh.flush()
            if os.name == "nt":
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fh.seek(0)
            fh.truncate()
            fh.write(f"pid={os.getpid()} ts={time.time():.3f}\n".encode("ascii", errors="ignore"))
            fh.flush()
            self._fh = fh
            return True
        except Exception:
            with contextlib.suppress(Exception):
                fh.close()
            return False

    def release(self) -> None:
        fh = self._fh
        self._fh = None
        if fh is None:
            return
        with contextlib.suppress(Exception):
            if os.name == "nt":
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        with contextlib.suppress(Exception):
            fh.close()


# ═══════════════════════════════════════════════════════════════════════════════
# RUNTIME CLASS (Keep original - no changes needed)
# ═══════════════════════════════════════════════════════════════════════════════

class _Runtime:
    """Async runtime for background operations - unchanged from original"""
    
    def __init__(self, bridge: _UiBridge) -> None:
        self._bridge = bridge
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._stop_event: asyncio.Event | None = None
        self._orchestrator: OrchestratorService | None = None
        self._started = threading.Event()
        self._start_error: BaseException | None = None
        self._tray_detail_submit_lock = threading.Lock()
        self._runtime_role = "leader"
        self._leader_lock: _LeaderFileLock | None = None

    def start(self) -> None:
        log.info("Runtime start requested")
        if self._thread is not None and self._thread.is_alive():
            log.warning("Runtime already started, skipping duplicate start")
            return
        self._started.clear()
        self._start_error = None
        self._thread = threading.Thread(target=self._thread_main, name="wip-runtime", daemon=True)
        self._thread.start()
        if not self._started.wait(timeout=20):
            log.error("Runtime bootstrap wait timed out after 20s")
            raise TimeoutError("Timed out while bootstrapping runtime thread")
        if self._start_error is not None:
            log.error("Runtime start failed error=%r", self._start_error)
            raise RuntimeError("Failed to start runtime") from self._start_error
        log.info("Runtime thread bootstrapped successfully")

    def is_viewer_mode(self) -> bool:
        return self._runtime_role == "viewer"

    def stop(self) -> None:
        log.info("Runtime stop requested")
        if self._thread is None:
            return
        if self._loop is not None and not self._loop.is_closed():
            if self._stop_event is not None:
                self._loop.call_soon_threadsafe(self._stop_event.set)
        self._thread.join(timeout=_GRACEFUL_STOP_TIMEOUT_SECONDS)
        if not self._thread.is_alive():
            log.info("Runtime thread stopped (graceful)")
            self._thread = None
            return
        log.warning("Runtime thread still running after %.0fs graceful wait — forcing loop stop", _GRACEFUL_STOP_TIMEOUT_SECONDS)
        if self._loop is not None and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(timeout=_FORCE_STOP_TIMEOUT_SECONDS)
        if self._thread.is_alive():
            log.error("Runtime thread did not stop within %.0fs + %.0fs total", _GRACEFUL_STOP_TIMEOUT_SECONDS, _FORCE_STOP_TIMEOUT_SECONDS)
        else:
            log.info("Runtime thread stopped (forced)")
        self._thread = None

    # All other runtime methods remain unchanged...
    def manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        log.info("Manual refresh requested full_scan=%s", full_scan)
        return self._submit(self._manual_refresh(full_scan=full_scan))

    def manual_group(self, tray_id: str, trolley_id: str, column: Column) -> None:
        log.info("Manual group requested tray_id=%s trolley_id=%s column=%s", tray_id, trolley_id, column.value)
        self._submit(self._manual_group(tray_id=tray_id, trolley_id=trolley_id, column=column))

    def manual_group_many(self, tray_ids: list[str], trolley_id: str, column: Column) -> dict[str, object]:
        log.info("Manual group many requested tray_count=%s trolley_id=%s column=%s", len(tray_ids), trolley_id, column.value)
        return self._submit(self._manual_group_many(tray_ids=tray_ids, trolley_id=trolley_id, column=column))

    def manual_ungroup(self, tray_id: str) -> None:
        log.info("Manual ungroup requested tray_id=%s", tray_id)
        self._submit(self._manual_ungroup(tray_id=tray_id))

    def manual_ungroup_many(self, tray_ids: list[str]) -> dict[str, object]:
        log.info("Manual ungroup many requested tray_count=%s", len(tray_ids))
        return self._submit(self._manual_ungroup_many(tray_ids=tray_ids))

    def rename_trolley(self, old_trolley_id: str, new_trolley_id: str) -> dict[str, object]:
        log.info("Rename trolley requested old=%s new=%s", old_trolley_id, new_trolley_id)
        return self._submit(self._rename_trolley(old_trolley_id=old_trolley_id, new_trolley_id=new_trolley_id))

    def clear_trolley(self, trolley_id: str) -> dict[str, object]:
        log.info("Clear trolley requested trolley_id=%s", trolley_id)
        return self._submit(self._clear_trolley(trolley_id=trolley_id))

    def delete_trolley(self, trolley_id: str) -> dict[str, object]:
        log.info("Delete trolley requested trolley_id=%s", trolley_id)
        return self._submit(self._delete_trolley(trolley_id=trolley_id))

    def delete_trolleys(self, trolley_ids: list[str]) -> dict[str, object]:
        log.info("Delete trolleys requested trolley_count=%s", len(trolley_ids))
        return self._submit(self._delete_trolleys(trolley_ids=trolley_ids))

    def set_auto_group_enabled(self, enabled: bool) -> dict[str, object]:
        log.info("Set auto group requested enabled=%s", enabled)
        return self._submit(self._set_auto_group_enabled(enabled=enabled))

    def update_grouping_settings(self, *, max_trays_per_trolley: int, total_trolley_count: int, refresh_interval_seconds: float, data_window_days: int) -> dict[str, object]:
        log.info("Update grouping settings requested")
        return self._submit(self._update_grouping_settings(max_trays_per_trolley=max_trays_per_trolley, total_trolley_count=total_trolley_count, refresh_interval_seconds=refresh_interval_seconds, data_window_days=data_window_days))

    def data_window_loading_status(self, *, data_window_days: int | None = None) -> dict[str, object]:
        return self._submit(self._data_window_loading_status(data_window_days=data_window_days))

    def tray_cells(self, tray_id: str) -> list[dict[str, str | None]]:
        log.info("Tray detail requested tray_id=%s", tray_id)
        if not self._tray_detail_submit_lock.acquire(timeout=120.0):
            raise RuntimeError("Tray detail request timed out. Database is busy.")
        try:
            return self._submit(self._tray_cells(tray_id=tray_id))
        finally:
            self._tray_detail_submit_lock.release()

    def cell_owner(self, cell_id: str) -> dict[str, str | None] | None:
        log.info("Cell owner requested cell_id=%s", cell_id)
        return self._submit(self._cell_owner(cell_id=cell_id))

    def _submit(self, coroutine: Any) -> Any:
        if self._loop is None:
            raise RuntimeError("Runtime loop is not available")
        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return future.result(timeout=300)

    def _thread_main(self) -> None:
        log.info("Runtime thread main started")
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._run())
        except RuntimeError as exc:
            stop_was_set = self._stop_event is not None and self._stop_event.is_set()
            if "Event loop stopped before Future completed" in str(exc) and stop_was_set:
                log.info("Runtime loop force-stopped after graceful timeout (expected)")
            else:
                self._start_error = exc
                log.exception("Runtime crashed")
                self._started.set()
        except BaseException as exc:
            self._start_error = exc
            log.exception("Runtime crashed")
            self._started.set()
        finally:
            if self._loop is not None:
                with contextlib.suppress(Exception):
                    self._loop.close()
            self._loop = None
            log.info("Runtime thread main exited")

    async def _run(self) -> None:
        log.info("Runtime async bootstrap started")
        _sync_data_window_settings()
        self._stop_event = asyncio.Event()
        grouping_state_repo = None

        if settings.shared_state_enabled:
            grouping_state_repo = SharedGroupingStateRepository(
                directory=settings.shared_state_dir,
                file_name=settings.shared_state_file,
            )
            log.info("Shared grouping state enabled path=%s", Path(settings.shared_state_dir) / settings.shared_state_file)

        self._runtime_role = "leader"
        self._leader_lock = None

        if grouping_state_repo is not None:
            lock_path = Path(settings.shared_state_dir) / ".leader.lock"
            lock = _LeaderFileLock(lock_path)
            acquired = await asyncio.to_thread(lock.try_acquire)
            if acquired:
                self._leader_lock = lock
                self._runtime_role = "leader"
                log.info("Runtime role=leader lock_path=%s", lock_path)
            else:
                self._runtime_role = "viewer"
                log.info("Runtime role=viewer")

        if self._runtime_role == "viewer":
            self._started.set()
            try:
                await self._viewer_loop(grouping_state_repo)
            finally:
                if self._leader_lock is not None:
                    self._leader_lock.release()
            return

        connection = SQLServerConnection()
        ccu_repo = CcuRepo(connection)
        fpc_repo = FpcRepo(connection)
        dashboard_repo = DashboardRepo(connection)
        delta_tracker = InMemoryDeltaTracker()
        event_bus = AsyncEventBus(default_queue_size=settings.event_queue_size)
        state_store = SingleWriterStateStore(queue_size=settings.event_queue_size)

        driver_candidates = build_driver_candidates(settings.sql_driver)
        preview_driver = driver_candidates[0] if driver_candidates else settings.sql_driver
        legacy_driver = preview_driver.strip().lower() == "sql server"
        refresh_peek_enabled = bool(settings.refresh_peek_enabled)
        if settings.refresh_peek_disable_for_legacy_driver and legacy_driver:
            refresh_peek_enabled = False

        orchestrator = OrchestratorService(
            ccu_repo=ccu_repo,
            fpc_repo=fpc_repo,
            delta_tracker=delta_tracker,
            event_bus=event_bus,
            state_store=state_store,
            clock=_SystemClock(),
            initial_load_start_hour=settings.initial_load_start_hour,
            initial_load_lookback_hours=settings.initial_load_lookback_hours,
            ui_data_window_days=settings.ui_data_window_days,
            delta_poll_interval_seconds=settings.delta_poll_interval_seconds,
            delta_poll_idle_interval_seconds=settings.delta_poll_idle_interval_seconds,
            backfill_cooldown_seconds=settings.ccu_backfill_cooldown_seconds,
            max_parallel_workers=settings.max_parallel_workers,
            snapshot_limit=settings.ui_snapshot_limit,
            max_trays_per_trolley=settings.trolley_max_trays,
            total_trolley_count=settings.total_trolley_count,
            assembly_auto_trolley_count=settings.assembly_auto_trolley_count,
            auto_group_enabled=settings.auto_group_default_enabled,
            grouping_sync_interval_seconds=settings.grouping_sync_interval_seconds,
            refresh_peek_enabled=refresh_peek_enabled,
            ccu_backfill_allow_targeted_lookup=settings.ccu_backfill_allow_targeted_lookup,
            grouping_state_repo=grouping_state_repo,
            dashboard_repo=dashboard_repo,
        )
        self._orchestrator = orchestrator

        queue = await event_bus.subscribe(maxsize=settings.event_queue_size)
        log.info("Runtime subscribed to event bus")

        ui_task = asyncio.create_task(self._ui_event_loop(queue), name="ui-event-coalescer")

        self._started.set()
        log.info("Runtime bootstrap signal emitted to UI thread")

        try:
            log.info("Starting orchestrator service")
            await orchestrator.start()
            log.info("Orchestrator started successfully")
            await self._stop_event.wait()
        finally:
            log.info("Runtime shutdown sequence started")
            if not ui_task.done():
                ui_task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await ui_task
            with contextlib.suppress(Exception):
                await orchestrator.stop()
            self._orchestrator = None
            with contextlib.suppress(Exception):
                await event_bus.unsubscribe(queue)
            if self._leader_lock is not None:
                self._leader_lock.release()
            log.info("Runtime shutdown sequence completed")

    async def _viewer_loop(self, grouping_state_repo) -> None:
        if grouping_state_repo is None:
            if self._stop_event is not None:
                await self._stop_event.wait()
            return
        log.info("Viewer loop started")
        last_signature: Any = None
        emit_seq = 0
        poll_seconds = max(0.5, float(settings.grouping_sync_interval_seconds))
        while self._stop_event is not None and not self._stop_event.is_set():
            payload = None
            try:
                projection = await grouping_state_repo.load_projection()
                payload = self._shared_projection_to_ui_payload(projection)
            except Exception:
                log.exception("Viewer loop failed to load projection")
            if payload is not None:
                payload_signature = _signature(payload)
                if payload_signature != last_signature:
                    emit_seq += 1
                    event_payload = dict(payload)
                    event_payload["seq"] = emit_seq
                    self._bridge.event_ready.emit(event_payload)
                    last_signature = payload_signature
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=poll_seconds)
            except asyncio.TimeoutError:
                continue
        log.info("Viewer loop stopped")

    def _shared_projection_to_ui_payload(self, projection: dict[str, Any]) -> dict[str, Any] | None:
        if not projection:
            return None
        assembly = projection.get("assembly_trolleys")
        queue = projection.get("queue_trolleys")
        precharge = projection.get("precharge_trolleys")
        ungrouped = projection.get("assembly_ungrouped")
        trays = projection.get("trays")
        if not isinstance(assembly, list) or not isinstance(queue, list) or not isinstance(precharge, list):
            return None
        payload: dict[str, Any] = {
            "assembly_trolleys": [r for r in assembly if isinstance(r, dict)],
            "queue_trolleys": [r for r in queue if isinstance(r, dict)],
            "precharge_trolleys": [r for r in precharge if isinstance(r, dict)],
            "assembly_ungrouped": [r for r in (ungrouped or []) if isinstance(r, dict)],
        }
        if isinstance(trays, list):
            payload["type"] = "snapshot"
            payload["trays"] = [r for r in trays if isinstance(r, dict)]
        else:
            payload["type"] = "projection"
        return payload

    async def _ui_event_loop(self, queue: asyncio.Queue[Any]) -> None:
        log.info("UI event coalescer loop started")
        pending: dict[str, dict[str, Any]] = {}
        pending_signatures: dict[str, Any] = {}
        emitted_tray_signatures: dict[str, Any] = {}
        last_snapshot_signature: Any = None
        last_projection_signature: Any = None
        window_seconds = settings.ui_coalesce_window_ms / 1000.0
        max_batch = settings.ui_coalesce_max_batch
        last_flush = datetime.now()
        emit_seq = 0

        def _emit(payload: dict[str, Any]) -> None:
            nonlocal emit_seq
            emit_seq += 1
            event_payload = dict(payload)
            event_payload["seq"] = emit_seq
            self._bridge.event_ready.emit(event_payload)

        try:
            while True:
                timeout = window_seconds if pending else None
                try:
                    if timeout is None:
                        event = await queue.get()
                    else:
                        event = await asyncio.wait_for(queue.get(), timeout=timeout)
                except asyncio.TimeoutError:
                    self._flush_pending(pending, pending_signatures=pending_signatures, emitted_tray_signatures=emitted_tray_signatures, emit_event=_emit)
                    last_flush = datetime.now()
                    continue

                if isinstance(event, SnapshotReady):
                    if pending:
                        pending.clear()
                        pending_signatures.clear()
                    snapshot_payload = {
                        "type": "snapshot",
                        "trays": [tray.to_dict() for tray in event.trays],
                        "assembly_trolleys": [t.to_dict() for t in event.assembly_trolleys],
                        "queue_trolleys": [t.to_dict() for t in event.queue_trolleys],
                        "precharge_trolleys": [t.to_dict() for t in event.precharge_trolleys],
                        "assembly_ungrouped": [tray.to_dict() for tray in event.assembly_ungrouped],
                    }
                    snapshot_signature = _signature(snapshot_payload)
                    if snapshot_signature == last_snapshot_signature:
                        last_flush = datetime.now()
                        continue
                    last_snapshot_signature = snapshot_signature
                    projection_payload = {
                        "type": "projection",
                        "assembly_trolleys": snapshot_payload["assembly_trolleys"],
                        "queue_trolleys": snapshot_payload["queue_trolleys"],
                        "precharge_trolleys": snapshot_payload["precharge_trolleys"],
                        "assembly_ungrouped": snapshot_payload["assembly_ungrouped"],
                    }
                    last_projection_signature = _signature(projection_payload)
                    emitted_tray_signatures = {
                        str(row.get("tray_id", "")).strip(): _signature(row)
                        for row in snapshot_payload["trays"]
                        if str(row.get("tray_id", "")).strip()
                    }
                    log.info("UI snapshot event trays=%s", len(event.trays))
                    _emit(snapshot_payload)
                    last_flush = datetime.now()
                    continue

                if isinstance(event, TrayUpdated):
                    row = event.tray.to_dict()
                    tray_id = str(row.get("tray_id", "")).strip()
                    if not tray_id:
                        continue
                    row_signature = _signature(row)
                    if emitted_tray_signatures.get(tray_id) == row_signature:
                        continue
                    pending[tray_id] = row
                    pending_signatures[tray_id] = row_signature
                    if len(pending) >= max_batch:
                        self._flush_pending(pending, pending_signatures=pending_signatures, emitted_tray_signatures=emitted_tray_signatures, emit_event=_emit)
                        last_flush = datetime.now()
                    continue

                if isinstance(event, TrolleyUpdated):
                    self._flush_pending(pending, pending_signatures=pending_signatures, emitted_tray_signatures=emitted_tray_signatures, emit_event=_emit)
                    projection_payload = {
                        "type": "projection",
                        "assembly_trolleys": [t.to_dict() for t in event.assembly_trolleys],
                        "queue_trolleys": [t.to_dict() for t in event.queue_trolleys],
                        "precharge_trolleys": [t.to_dict() for t in event.precharge_trolleys],
                        "assembly_ungrouped": [tray.to_dict() for tray in event.assembly_ungrouped],
                    }
                    projection_signature = _signature(projection_payload)
                    if projection_signature == last_projection_signature:
                        last_flush = datetime.now()
                        continue
                    last_projection_signature = projection_signature
                    log.info("UI projection event")
                    _emit(projection_payload)
                    last_flush = datetime.now()
                    continue

                if pending and (datetime.now() - last_flush).total_seconds() >= window_seconds:
                    self._flush_pending(pending, pending_signatures=pending_signatures, emitted_tray_signatures=emitted_tray_signatures, emit_event=_emit)
                    last_flush = datetime.now()

        except asyncio.CancelledError:
            log.info("UI event coalescer loop cancelled")
            if pending:
                with contextlib.suppress(Exception):
                    self._flush_pending(pending, pending_signatures=pending_signatures, emitted_tray_signatures=emitted_tray_signatures, emit_event=_emit)
            raise

    def _flush_pending(self, pending, *, pending_signatures, emitted_tray_signatures, emit_event=None) -> None:
        if not pending:
            return
        rows = list(pending.values())
        rows.sort(key=lambda item: (item.get("latest_collected_time") is not None, item.get("latest_collected_time"), item.get("tray_id")), reverse=True)
        pending.clear()
        filtered_rows: list[dict[str, Any]] = []
        for row in rows:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            sig = pending_signatures.pop(tray_id, None) or _signature(row)
            if emitted_tray_signatures.get(tray_id) == sig:
                continue
            emitted_tray_signatures[tray_id] = sig
            filtered_rows.append(row)
        pending_signatures.clear()
        if not filtered_rows:
            return
        payload = {"type": "trays_delta", "rows": filtered_rows}
        if emit_event is not None:
            emit_event(payload)
        else:
            self._bridge.event_ready.emit(payload)

    def _require_orchestrator(self) -> OrchestratorService:
        if self._orchestrator is not None:
            return self._orchestrator
        if self._runtime_role == "viewer":
            raise RuntimeError("Viewer mode - use leader instance for actions")
        raise RuntimeError("Runtime is not started")

    async def _manual_refresh(self, *, full_scan: bool) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.manual_refresh(full_scan=full_scan)
        return result

    async def _manual_group(self, *, tray_id: str, trolley_id: str, column: Column) -> None:
        orchestrator = self._require_orchestrator()
        await orchestrator.group_tray_manual(tray_id=tray_id, trolley_id=trolley_id, column=column)

    async def _manual_group_many(self, *, tray_ids: list[str], trolley_id: str, column: Column) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.group_trays_manual(tray_ids=tray_ids, trolley_id=trolley_id, column=column)
        return result

    async def _manual_ungroup(self, *, tray_id: str) -> None:
        orchestrator = self._require_orchestrator()
        await orchestrator.ungroup_tray_manual(tray_id=tray_id)

    async def _manual_ungroup_many(self, *, tray_ids: list[str]) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.ungroup_trays_manual(tray_ids=tray_ids)
        return result

    async def _rename_trolley(self, *, old_trolley_id: str, new_trolley_id: str) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.rename_trolley_manual(old_trolley_id, new_trolley_id)
        return result

    async def _clear_trolley(self, *, trolley_id: str) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.clear_trolley_manual(trolley_id)
        return result

    async def _delete_trolley(self, *, trolley_id: str) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.delete_trolley_manual(trolley_id)
        return result

    async def _delete_trolleys(self, *, trolley_ids: list[str]) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.delete_trolleys_manual(trolley_ids)
        return result

    async def _set_auto_group_enabled(self, *, enabled: bool) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.set_auto_group_enabled(enabled=enabled)
        return result

    async def _update_grouping_settings(self, *, max_trays_per_trolley: int, total_trolley_count: int, refresh_interval_seconds: float, data_window_days: int) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        result = await orchestrator.update_grouping_settings(max_trays_per_trolley=max_trays_per_trolley, total_trolley_count=total_trolley_count, refresh_interval_seconds=refresh_interval_seconds, data_window_days=data_window_days)
        return result

    async def _data_window_loading_status(self, *, data_window_days: int | None = None) -> dict[str, object]:
        orchestrator = self._require_orchestrator()
        return await orchestrator.data_window_loading_status(data_window_days=data_window_days)

    async def _tray_cells(self, *, tray_id: str) -> list[dict[str, str | None]]:
        orchestrator = self._require_orchestrator()
        return await orchestrator.fetch_tray_cells(tray_id=tray_id)

    async def _cell_owner(self, *, cell_id: str) -> dict[str, str | None] | None:
        orchestrator = self._require_orchestrator()
        return await orchestrator.fetch_cell_owner(cell_id=cell_id)
    
# ═══════════════════════════════════════════════════════════════════════════════
# MODERN UI WIDGETS
# ═══════════════════════════════════════════════════════════════════════════════

class _ModernMetricCard(QFrame):
    """Modern metric card with icon, animated value, and trend indicator"""
    
    clicked = Signal()
    
    def __init__(
        self,
        title: str,
        icon: str = "📊",
        color: str = ThemeColors.PRIMARY,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("metricCard")
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self._color = color
        self._current_value = 0
        self._target_value = 0
        
        self._setup_ui(title, icon)
        self._setup_shadow()
        self._setup_animation()
    
    def _setup_ui(self, title: str, icon: str) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(6)
        
        # Top row: icon + trend
        top_row = QHBoxLayout()
        top_row.setSpacing(0)
        
        self._icon = QLabel(icon)
        self._icon.setObjectName("metricIcon")
        self._icon.setFont(QFont("Segoe UI Emoji", 20))
        top_row.addWidget(self._icon)
        
        top_row.addStretch()
        
        # Trend indicator
        self._trend = QLabel("")
        self._trend.setStyleSheet(f"font-size: 12px; font-weight: 600;")
        self._trend.setVisible(False)
        top_row.addWidget(self._trend)
        
        layout.addLayout(top_row)
        
        # Value
        self._value_label = QLabel("0")
        self._value_label.setObjectName("metricValue")
        layout.addWidget(self._value_label)
        
        # Title
        self._title_label = QLabel(title)
        self._title_label.setObjectName("metricTitle")
        layout.addWidget(self._title_label)
        
        # Subtitle (optional)
        self._subtitle = QLabel("")
        self._subtitle.setObjectName("metricSubtitle")
        self._subtitle.setVisible(False)
        layout.addWidget(self._subtitle)
    
    def _setup_shadow(self) -> None:
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(20)
        shadow.setOffset(0, 4)
        shadow.setColor(QColor(0, 0, 0, 20))
        self.setGraphicsEffect(shadow)
    
    def _setup_animation(self) -> None:
        self._anim_timer = QTimer(self)
        self._anim_timer.setInterval(16)
        self._anim_timer.timeout.connect(self._animate_value)
    
    def _animate_value(self) -> None:
        if self._current_value == self._target_value:
            self._anim_timer.stop()
            return
        
        diff = self._target_value - self._current_value
        step = max(1, abs(diff) // 8) * (1 if diff > 0 else -1)
        
        if abs(diff) <= abs(step):
            self._current_value = self._target_value
        else:
            self._current_value += step
        
        self._value_label.setText(f"{self._current_value:,}")
    
    def set_value(self, value: int, *, animate: bool = True) -> None:
        old_value = self._target_value
        self._target_value = value
        
        if animate and abs(value - old_value) > 0:
            self._anim_timer.start()
        else:
            self._current_value = value
            self._value_label.setText(f"{value:,}")
        
        # Update trend indicator
        if old_value > 0:
            diff = value - old_value
            if diff > 0:
                self._trend.setText(f"↑ +{diff}")
                self._trend.setStyleSheet(f"color: {ThemeColors.SUCCESS}; font-size: 12px; font-weight: 600;")
                self._trend.setVisible(True)
            elif diff < 0:
                self._trend.setText(f"↓ {diff}")
                self._trend.setStyleSheet(f"color: {ThemeColors.DANGER}; font-size: 12px; font-weight: 600;")
                self._trend.setVisible(True)
            else:
                self._trend.setVisible(False)
    
    def set_subtitle(self, text: str) -> None:
        self._subtitle.setText(text)
        self._subtitle.setVisible(bool(text))
    
    def enterEvent(self, event) -> None:
        shadow = self.graphicsEffect()
        if isinstance(shadow, QGraphicsDropShadowEffect):
            shadow.setBlurRadius(30)
            shadow.setOffset(0, 8)
        super().enterEvent(event)
    
    def leaveEvent(self, event) -> None:
        shadow = self.graphicsEffect()
        if isinstance(shadow, QGraphicsDropShadowEffect):
            shadow.setBlurRadius(20)
            shadow.setOffset(0, 4)
        super().leaveEvent(event)
    
    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class _TrolleyItemDelegate(QStyledItemDelegate):
    """Custom delegate for trolley list items with rich visualization"""
    
    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        rect = option.rect
        is_selected = bool(option.state & QStyle.StateFlag.State_Selected)
        is_hovered = bool(option.state & QStyle.StateFlag.State_MouseOver)
        
        # Background
        if is_selected:
            bg_color = QColor(ThemeColors.PRIMARY_LIGHT)
            border_color = QColor(ThemeColors.PRIMARY)
        elif is_hovered:
            bg_color = QColor(ThemeColors.SURFACE_HOVER)
            border_color = QColor(ThemeColors.BORDER)
        else:
            bg_color = QColor(ThemeColors.SURFACE)
            border_color = QColor(ThemeColors.BORDER_LIGHT)
        
        # Draw rounded rectangle
        from PySide6.QtGui import QPainterPath
        path = QPainterPath()
        adjusted_rect = QRectF(rect).adjusted(6, 3, -6, -3)
        path.addRoundedRect(adjusted_rect, 12, 12)
        painter.fillPath(path, bg_color)
        painter.setPen(QPen(border_color, 1))
        painter.drawPath(path)
        
        # Get data
        model = index.model()
        if model is None:
            painter.restore()
            return
        
        trolley_id = str(model.data(index, TrolleyListModel.TrolleyIdRole) or "")
        tray_count = int(model.data(index, TrolleyListModel.TrayCountRole) or 0)
        cell_count = int(model.data(index, TrolleyListModel.CellCountRole) or 0)
        mode = str(model.data(index, TrolleyListModel.ModeRole) or "auto")
        state = str(model.data(index, TrolleyListModel.StateRole) or "")
        aging_state = str(model.data(index, TrolleyListModel.AgingStateRole) or "")
        aging_time = str(model.data(index, TrolleyListModel.AgingTimeRole) or "-")
        
        # Layout
        inner_rect = adjusted_rect.adjusted(14, 10, -14, -10)
        
        # Trolley ID (main text)
        painter.setFont(QFont("Segoe UI", 13, QFont.Weight.DemiBold))
        painter.setPen(QColor(ThemeColors.TEXT))
        painter.drawText(
            QRectF(inner_rect.left(), inner_rect.top(), inner_rect.width() * 0.5, 20),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            trolley_id
        )
        
        # State badge (top right)
        state_colors = {
            "Stacking": (ThemeColors.INFO_BG, ThemeColors.INFO),
            "Aging": (ThemeColors.WARNING_BG, ThemeColors.WARNING),
            "Waiting": (ThemeColors.BORDER_LIGHT, ThemeColors.TEXT_MUTED),
            "Completed": (ThemeColors.SUCCESS_BG, ThemeColors.SUCCESS),
        }
        if state in state_colors:
            bg, fg = state_colors[state]
            self._draw_badge(painter, inner_rect.right() - 80, inner_rect.top(), state, bg, fg)
        
        # Bottom row
        bottom_y = inner_rect.bottom() - 18
        painter.setFont(QFont("Segoe UI", 11))
        
        # Mode badge
        mode_bg = ThemeColors.PRIMARY_LIGHT if mode == "manual" else ThemeColors.BORDER_LIGHT
        mode_fg = ThemeColors.PRIMARY if mode == "manual" else ThemeColors.TEXT_MUTED
        self._draw_badge(painter, inner_rect.left(), bottom_y, mode.upper(), mode_bg, mode_fg, small=True)
        
        # Counts
        painter.setPen(QColor(ThemeColors.TEXT_SECONDARY))
        count_text = f"📦 {tray_count}  •  🔋 {cell_count:,}"
        painter.drawText(
            QRectF(inner_rect.left() + 75, bottom_y, 150, 20),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            count_text
        )
        
        # Aging (if applicable)
        if aging_time != "-":
            aging_colors = {
                "waiting": (ThemeColors.WARNING_BG, ThemeColors.WARNING),
                "ready": (ThemeColors.SUCCESS_BG, ThemeColors.SUCCESS),
                "exceed": (ThemeColors.DANGER_BG, ThemeColors.DANGER),
            }
            ag_bg, ag_fg = aging_colors.get(aging_state, (ThemeColors.BORDER_LIGHT, ThemeColors.TEXT_MUTED))
            self._draw_badge(painter, inner_rect.right() - 95, bottom_y, f"⏱ {aging_time}", ag_bg, ag_fg, small=True)
        
        painter.restore()
    
    def _draw_badge(
        self,
        painter: QPainter,
        x: float,
        y: float,
        text: str,
        bg_color: str,
        fg_color: str,
        *,
        small: bool = False,
    ) -> None:
        font = QFont("Segoe UI", 10 if small else 11, QFont.Weight.DemiBold)
        painter.setFont(font)
        fm = QFontMetrics(font)
        
        padding = 8
        height = 20 if small else 24
        width = fm.horizontalAdvance(text) + padding * 2
        
        rect = QRectF(x, y, width, height)
        from PySide6.QtGui import QPainterPath
        path = QPainterPath()
        path.addRoundedRect(rect, height / 2, height / 2)
        painter.fillPath(path, QColor(bg_color))
        
        painter.setPen(QColor(fg_color))
        painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, text)
    
    def sizeHint(self, option: QStyleOptionViewItem, index: QModelIndex) -> QSize:
        return QSize(option.rect.width(), 76)


class _TraySelectIconPainter:
    @staticmethod
    def draw(painter: QPainter, rect, state: str) -> None:
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
        
        color = QColor(ThemeColors.TEXT_MUTED)
        pen = QPen(color)
        pen.setWidth(2)
        painter.setPen(pen)
        painter.setBrush(Qt.BrushStyle.NoBrush)
        
        diameter = max(12, min(rect.width(), rect.height()) - 10)
        cx = rect.center().x()
        cy = rect.center().y()
        radius = diameter / 2.0
        outer_rect = QRectF(cx - radius, cy - radius, diameter, diameter)
        painter.drawEllipse(outer_rect)
        
        if state == "on":
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(ThemeColors.PRIMARY))
            inner_d = max(4.0, diameter - 6.0)
            inner_r = inner_d / 2.0
            inner_rect = QRectF(cx - inner_r, cy - inner_r, inner_d, inner_d)
            painter.drawEllipse(inner_rect)
        elif state == "partial":
            painter.setBrush(QColor(ThemeColors.PRIMARY))
            half = max(3.0, diameter * 0.3)
            painter.drawRect(QRectF(cx - half, cy - 1.5, half * 2, 3))
        
        painter.restore()


class _TraySelectItemDelegate(QStyledItemDelegate):
    def paint(self, painter: QPainter, option: QStyleOptionViewItem, index: QModelIndex) -> None:
        if index.column() != 0:
            super().paint(painter, option, index)
            return
        opt = QStyleOptionViewItem(option)
        self.initStyleOption(opt, index)
        opt.text = ""
        style = opt.widget.style() if opt.widget else QApplication.style()
        style.drawControl(QStyle.ControlElement.CE_ItemViewItem, opt, painter, opt.widget)
        state = str(index.data(UNGROUP_CHECK_VISUAL_ROLE) or "off")
        _TraySelectIconPainter.draw(painter, option.rect, state)


class _TraySelectHeaderView(QHeaderView):
    def paintSection(self, painter: QPainter, rect, logical_index: int) -> None:
        super().paintSection(painter, rect, logical_index)
        if logical_index != 0:
            return
        model = self.model()
        if model is None:
            return
        state = str(model.headerData(logical_index, self.orientation(), UNGROUP_CHECK_VISUAL_ROLE) or "off")
        _TraySelectIconPainter.draw(painter, rect, state)


class _ModernColumnCard(QFrame):
    """Modern column card with header gradient and rich content"""
    
    def __init__(
        self,
        title: str,
        icon: str,
        gradient_color: str,
        trolley_model,
        *,
        tray_model=None,
        tray_title: str | None = None,
        tray_actions_widget: QWidget | None = None,
        on_trolley_context: Callable[[QListView, QModelIndex], None] | None = None,
        on_trolley_double_click: Callable[[QListView, QModelIndex], None] | None = None,
        on_trolley_click: Callable[[QListView, QModelIndex], None] | None = None,
        on_tray_header_click: Callable[[int], None] | None = None,
    ) -> None:
        super().__init__()
        self.setObjectName("columnCard")
        self.tray_table: QTableView | None = None
        self._gradient_color = gradient_color
        self._tray_resize_pending = False
        self._last_tray_resize_at: datetime | None = None
        self._last_tray_resize_shape: tuple[int, int] | None = None
        self._interaction_hold_active = False
        self._interaction_release_timer: QTimer | None = None
        
        self._setup_ui(title, icon, trolley_model, tray_model, tray_title, tray_actions_widget)
        self._setup_connections(on_trolley_context, on_trolley_double_click, on_trolley_click, on_tray_header_click)
        self._setup_shortcuts()
    
    def _setup_ui(
        self,
        title: str,
        icon: str,
        trolley_model,
        tray_model,
        tray_title: str | None,
        tray_actions_widget: QWidget | None,
    ) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        # Header with gradient
        header = QFrame()
        header.setObjectName("columnHeaderFrame")
        header.setStyleSheet(f"""
            QFrame#columnHeaderFrame {{
                background: qlineargradient(
                    x1:0, y1:0, x2:1, y2:0,
                    stop:0 {self._gradient_color}20,
                    stop:1 transparent
                );
                border-bottom: 1px solid {ThemeColors.BORDER};
                border-radius: 16px 16px 0 0;
            }}
        """)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(20, 16, 20, 12)
        
        # Icon + Title
        icon_label = QLabel(icon)
        icon_label.setFont(QFont("Segoe UI Emoji", 18))
        header_layout.addWidget(icon_label)
        
        title_label = QLabel(title)
        title_label.setObjectName("columnHeader")
        header_layout.addWidget(title_label)
        
        header_layout.addStretch()
        
        # Stats badge
        self._stats_label = QLabel("")
        self._stats_label.setStyleSheet(f"""
            background: {ThemeColors.SURFACE_HOVER};
            color: {ThemeColors.TEXT_SECONDARY};
            padding: 6px 14px;
            border-radius: 14px;
            font-size: 12px;
            font-weight: 500;
        """)
        header_layout.addWidget(self._stats_label)
        
        layout.addWidget(header)
        
        # Trolley list
        self.trolley_list = QListView()
        self.trolley_list.setModel(trolley_model)
        self.trolley_list.setItemDelegate(_TrolleyItemDelegate(self))
        self.trolley_list.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.trolley_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.trolley_list.setSpacing(2)
        layout.addWidget(self.trolley_list, 1)
        
        # Tray section (if model provided)
        if tray_model is not None:
            self._setup_tray_section(tray_model, tray_title, tray_actions_widget, layout)
    
    def _setup_tray_section(
        self,
        tray_model,
        tray_title: str | None,
        tray_actions_widget: QWidget | None,
        parent_layout: QVBoxLayout,
    ) -> None:
        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setStyleSheet(f"background: {ThemeColors.BORDER};")
        sep.setMaximumHeight(1)
        parent_layout.addWidget(sep)
        
        # Tray header
        tray_header = QFrame()
        tray_header.setStyleSheet(f"background: {ThemeColors.SURFACE_HOVER};")
        tray_header_layout = QHBoxLayout(tray_header)
        tray_header_layout.setContentsMargins(16, 10, 16, 10)
        tray_header_layout.setSpacing(12)
        
        tray_label = QLabel(f"📥 {tray_title or 'Ungrouped Trays'}")
        tray_label.setStyleSheet(f"font-weight: 600; font-size: 13px; color: {ThemeColors.TEXT};")
        tray_header_layout.addWidget(tray_label)
        
        tray_header_layout.addStretch()
        
        if tray_actions_widget is not None:
            tray_actions_widget.setParent(tray_header)
            tray_header_layout.addWidget(tray_actions_widget)
        
        parent_layout.addWidget(tray_header)
        
        # Tray table
        self.tray_table = QTableView()
        self.tray_table.setModel(tray_model)
        tray_model.modelReset.connect(self._schedule_resize_tray_columns)
        self.tray_table.setAlternatingRowColors(True)
        self.tray_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectItems)
        self.tray_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tray_table.setEditTriggers(
            QAbstractItemView.EditTrigger.CurrentChanged |
            QAbstractItemView.EditTrigger.SelectedClicked |
            QAbstractItemView.EditTrigger.DoubleClicked
        )
        self.tray_table.verticalHeader().setVisible(False)
        self.tray_table.verticalHeader().setDefaultSectionSize(36)
        self.tray_table.setShowGrid(False)
        
        header = _TraySelectHeaderView(Qt.Orientation.Horizontal, self.tray_table)
        self.tray_table.setHorizontalHeader(header)
        header.setSectionsClickable(True)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        header.setMinimumSectionSize(24)
        header.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter | Qt.AlignmentFlag.AlignVCenter)
        
        self.tray_table.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.tray_table.setWordWrap(False)
        self.tray_table.setTextElideMode(Qt.TextElideMode.ElideNone)
        self.tray_table.setMouseTracking(True)
        self.tray_table.setItemDelegateForColumn(0, _TraySelectItemDelegate(self.tray_table))
        self.tray_table.setFrameShape(QFrame.Shape.NoFrame)
        
        self._interaction_release_timer = QTimer(self)
        self._interaction_release_timer.setSingleShot(True)
        self._interaction_release_timer.timeout.connect(self._release_tray_interaction_hold)
        
        self.tray_table.viewport().installEventFilter(self)
        self.tray_table.installEventFilter(self)
        
        vbar = self.tray_table.verticalScrollBar()
        hbar = self.tray_table.horizontalScrollBar()
        vbar.sliderPressed.connect(lambda: self._set_tray_interaction_hold(True))
        vbar.sliderReleased.connect(lambda: self._queue_tray_interaction_release(80))
        hbar.sliderPressed.connect(lambda: self._set_tray_interaction_hold(True))
        hbar.sliderReleased.connect(lambda: self._queue_tray_interaction_release(80))
        
        self._schedule_resize_tray_columns()
        parent_layout.addWidget(self.tray_table, 1)
    
    def _setup_connections(
        self,
        on_trolley_context,
        on_trolley_double_click,
        on_trolley_click,
        on_tray_header_click,
    ) -> None:
        self._on_trolley_context_cb = on_trolley_context
        
        if on_trolley_context is not None:
            self.trolley_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            self.trolley_list.customContextMenuRequested.connect(self._handle_trolley_context)
        
        if on_trolley_click is not None:
            self.trolley_list.clicked.connect(lambda idx: on_trolley_click(self.trolley_list, idx))
        
        if on_trolley_double_click is not None:
            self.trolley_list.doubleClicked.connect(lambda idx: on_trolley_double_click(self.trolley_list, idx))
        
        if self.tray_table is not None:
            self.tray_table.clicked.connect(self._on_tray_table_clicked)
            if on_tray_header_click is not None:
                self.tray_table.horizontalHeader().sectionClicked.connect(on_tray_header_click)
    
    def _setup_shortcuts(self) -> None:
        # Trolley list shortcuts
        self._trolley_copy_shortcut = QShortcut(QKeySequence.StandardKey.Copy, self.trolley_list)
        self._trolley_copy_shortcut.setContext(Qt.ShortcutContext.WidgetWithChildrenShortcut)
        self._trolley_copy_shortcut.activated.connect(self._copy_selected_trolley_rows)
        
        self._trolley_select_all_shortcut = QShortcut(QKeySequence.StandardKey.SelectAll, self.trolley_list)
        self._trolley_select_all_shortcut.setContext(Qt.ShortcutContext.WidgetWithChildrenShortcut)
        self._trolley_select_all_shortcut.activated.connect(self._select_all_trolley_rows)
        
        self.trolley_list.installEventFilter(self)
        self.trolley_list.viewport().installEventFilter(self)
        
        # Tray table shortcuts
        if self.tray_table is not None:
            self._tray_copy_shortcut = QShortcut(QKeySequence.StandardKey.Copy, self.tray_table)
            self._tray_copy_shortcut.setContext(Qt.ShortcutContext.WidgetWithChildrenShortcut)
            self._tray_copy_shortcut.activated.connect(self._copy_selected_tray_cells)
            
            self._tray_select_all_shortcut = QShortcut(QKeySequence.StandardKey.SelectAll, self.tray_table)
            self._tray_select_all_shortcut.setContext(Qt.ShortcutContext.WidgetWithChildrenShortcut)
            self._tray_select_all_shortcut.activated.connect(self._select_all_tray_cells)
    
    def set_stats_text(self, text: str) -> None:
        self._stats_label.setText(text)
    
    def _handle_trolley_context(self, pos: QPoint) -> None:
        if self._on_trolley_context_cb is None:
            return
        index = self.trolley_list.indexAt(pos)
        if not index.isValid():
            return
        self._on_trolley_context_cb(self.trolley_list, index)
    
    def _on_tray_table_clicked(self, index: QModelIndex) -> None:
        if self.tray_table is None or not index.isValid():
            return
        model = self.tray_table.model()
        if model is None or index.column() != 0:
            return
        toggle_row = getattr(model, "toggle_row", None)
        if callable(toggle_row):
            toggle_row(index.row())
    
    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        # Handle keyboard shortcuts for trolley list
        if watched in {self.trolley_list, self.trolley_list.viewport()} and event.type() == QEvent.Type.KeyPress:
            key = getattr(event, "key", lambda: None)()
            modifiers = getattr(event, "modifiers", lambda: Qt.KeyboardModifier.NoModifier)()
            if modifiers & Qt.KeyboardModifier.ControlModifier:
                if key == Qt.Key.Key_A:
                    self._select_all_trolley_rows()
                    return True
                if key == Qt.Key.Key_C:
                    self._copy_selected_trolley_rows()
                    return True
        
        # Handle interaction hold for tray table
        if self.tray_table is not None and watched in {self.tray_table, self.tray_table.viewport()}:
            event_type = event.type()
            if event_type in {QEvent.Type.Wheel, QEvent.Type.MouseButtonPress, QEvent.Type.MouseMove}:
                self._set_tray_interaction_hold(True)
                self._queue_tray_interaction_release(220)
            elif event_type in {QEvent.Type.MouseButtonRelease, QEvent.Type.Leave}:
                self._queue_tray_interaction_release(100)
        
        return super().eventFilter(watched, event)
    
    def _queue_tray_interaction_release(self, delay_ms: int) -> None:
        if self._interaction_release_timer is None:
            self._set_tray_interaction_hold(False)
            return
        self._interaction_release_timer.start(max(1, int(delay_ms)))
    
    def _release_tray_interaction_hold(self) -> None:
        self._set_tray_interaction_hold(False)
    
    def _set_tray_interaction_hold(self, hold: bool) -> None:
        if self._interaction_hold_active == hold:
            return
        self._interaction_hold_active = hold
        if self.tray_table is not None:
            model = self.tray_table.model()
            set_hold = getattr(model, "set_interaction_hold", None)
            if callable(set_hold):
                with contextlib.suppress(Exception):
                    set_hold(hold)
        if not hold:
            self._schedule_resize_tray_columns()
    
    def _select_all_tray_cells(self) -> None:
        if self.tray_table is not None:
            self.tray_table.selectAll()
    
    def _select_all_trolley_rows(self) -> None:
        self.trolley_list.selectAll()
    
    def _copy_selected_trolley_rows(self) -> None:
        model = self.trolley_list.model()
        selection_model = self.trolley_list.selectionModel()
        if model is None or selection_model is None:
            return
        selected_indexes = list(selection_model.selectedIndexes())
        if not selected_indexes:
            current = self.trolley_list.currentIndex()
            if current.isValid():
                selected_indexes = [current]
        if not selected_indexes:
            return
        row_ids = sorted({index.row() for index in selected_indexes if index.isValid()})
        lines: list[str] = []
        for row in row_ids:
            index = model.index(row, 0)
            value = model.data(index, Qt.ItemDataRole.DisplayRole)
            lines.append("" if value is None else str(value))
        QApplication.clipboard().setText("\n".join(lines))
    
    def _copy_selected_tray_cells(self) -> None:
        if self.tray_table is None:
            return
        model = self.tray_table.model()
        selection_model = self.tray_table.selectionModel()
        if model is None or selection_model is None:
            return
        selected_indexes = list(selection_model.selectedIndexes())
        if not selected_indexes:
            current = self.tray_table.currentIndex()
            if current.isValid():
                selected_indexes = [current]
        if not selected_indexes:
            return
        
        values_by_cell: dict[tuple[int, int], str] = {}
        row_min = min(index.row() for index in selected_indexes)
        row_max = max(index.row() for index in selected_indexes)
        col_min = min(index.column() for index in selected_indexes)
        col_max = max(index.column() for index in selected_indexes)
        
        for index in selected_indexes:
            value = model.data(index, Qt.ItemDataRole.DisplayRole)
            values_by_cell[(index.row(), index.column())] = "" if value is None else str(value)
        
        lines: list[str] = []
        for row in range(row_min, row_max + 1):
            row_values = [values_by_cell.get((row, col), "") for col in range(col_min, col_max + 1)]
            lines.append("\t".join(row_values))
        QApplication.clipboard().setText("\n".join(lines))
    
    def _schedule_resize_tray_columns(self, *_args) -> None:
        if self._tray_resize_pending:
            return
        self._tray_resize_pending = True
        QTimer.singleShot(220, self._flush_resize_tray_columns)
    
    def _flush_resize_tray_columns(self) -> None:
        if not self._tray_resize_pending:
            return
        if self.tray_table is None:
            self._tray_resize_pending = False
            return
        model = self.tray_table.model()
        if model is None:
            self._tray_resize_pending = False
            return
        if self._interaction_hold_active:
            QTimer.singleShot(220, self._flush_resize_tray_columns)
            return
        
        vbar = self.tray_table.verticalScrollBar()
        hbar = self.tray_table.horizontalScrollBar()
        if (
            self.tray_table.state() != QAbstractItemView.State.NoState
            or (vbar is not None and vbar.isSliderDown())
            or (hbar is not None and hbar.isSliderDown())
        ):
            QTimer.singleShot(220, self._flush_resize_tray_columns)
            return
        
        now = datetime.now()
        row_count = model.rowCount()
        column_count = model.columnCount()
        current_shape = (row_count, column_count)
        
        if (
            self._last_tray_resize_shape == current_shape
            and self._last_tray_resize_at is not None
            and (now - self._last_tray_resize_at).total_seconds() < 6.0
        ):
            self._tray_resize_pending = False
            return
        
        if self._last_tray_resize_at is not None and (now - self._last_tray_resize_at).total_seconds() < 0.8:
            QTimer.singleShot(220, self._flush_resize_tray_columns)
            return
        
        self._tray_resize_pending = False
        self._last_tray_resize_at = now
        self._last_tray_resize_shape = current_shape
        self._resize_tray_columns()
    
    def _resize_tray_columns(self, *_args) -> None:
        if self.tray_table is None:
            return
        model = self.tray_table.model()
        if model is None:
            return
        
        header = self.tray_table.horizontalHeader()
        header.setStretchLastSection(False)
        row_count = model.rowCount()
        column_count = model.columnCount()
        
        for col in range(column_count):
            header.setSectionResizeMode(col, QHeaderView.ResizeMode.Interactive)
        
        # Base widths for columns
        base_widths = {1: 50, 2: 130, 3: 60, 4: 150, 5: 150, 6: 90, 7: 90, 8: 90}
        for col, width in base_widths.items():
            if col < column_count:
                header.resizeSection(col, width)
        
        # Auto-size tray ID column
        if row_count > 0 and column_count > 2:
            fm: QFontMetrics = self.tray_table.fontMetrics()
            max_tray_width = 0
            sample_limit = min(row_count, 50)
            for row_idx in range(sample_limit):
                idx = model.index(row_idx, 2)
                text = str(model.data(idx, Qt.ItemDataRole.DisplayRole) or "")
                w = fm.horizontalAdvance(text) + 20
                if w > max_tray_width:
                    max_tray_width = w
            if max_tray_width > 0:
                header.resizeSection(2, max(110, min(max_tray_width, 260)))
        
        # Fixed size for checkbox column
        if column_count > 0:
            row_height = max(20, self.tray_table.verticalHeader().defaultSectionSize())
            icon_width = max(24, min(28, row_height - 8))
            header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
            header.resizeSection(0, icon_width)


# ═══════════════════════════════════════════════════════════════════════════════
# MODERN SETTINGS DIALOG
# ═══════════════════════════════════════════════════════════════════════════════

class _ModernSettingsDialog(QDialog):
    """Modern settings dialog with tabs and better organization"""
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setWindowTitle("⚙️ Settings")
        self.setModal(True)
        self.resize(580, 520)
        self._setup_ui()
    
    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(20)
        
        # Header
        header = QLabel("Settings")
        header.setStyleSheet(f"font-size: 24px; font-weight: 700; color: {ThemeColors.TEXT};")
        layout.addWidget(header)
        
        # Tab widget
        tabs = QTabWidget()
        tabs.addTab(self._create_connection_tab(), "🔌 Connection")
        tabs.addTab(self._create_grouping_tab(), "📦 Grouping")
        tabs.addTab(self._create_performance_tab(), "⚡ Performance")
        layout.addWidget(tabs, 1)
        
        # Hint
        hint = QLabel("💡 Refresh & data window apply immediately. Database settings require restart.")
        hint.setStyleSheet(f"color: {ThemeColors.TEXT_MUTED}; font-size: 12px; padding: 8px 0;")
        hint.setWordWrap(True)
        layout.addWidget(hint)
        
        # Buttons
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        
        # Style buttons
        save_btn = button_box.button(QDialogButtonBox.StandardButton.Save)
        save_btn.setObjectName("successBtn")
        
        layout.addWidget(button_box)
    
    def _create_connection_tab(self) -> QWidget:
        widget = QWidget()
        form = QFormLayout(widget)
        form.setSpacing(16)
        form.setContentsMargins(16, 20, 16, 20)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        
        self._host_input = QLineEdit()
        self._host_input.setText(settings.sql_server)
        self._host_input.setPlaceholderText("e.g., 10.148.144.75")
        form.addRow("🖥️ Database Host", self._host_input)
        
        self._user_input = QLineEdit()
        self._user_input.setText(settings.sql_user)
        self._user_input.setPlaceholderText("e.g., sa")
        form.addRow("👤 Username", self._user_input)
        
        self._pass_input = QLineEdit()
        self._pass_input.setText(settings.sql_password)
        self._pass_input.setEchoMode(QLineEdit.EchoMode.Password)
        self._pass_input.setPlaceholderText("••••••••")
        form.addRow("🔒 Password", self._pass_input)
        
        return widget
    
    def _create_grouping_tab(self) -> QWidget:
        widget = QWidget()
        form = QFormLayout(widget)
        form.setSpacing(16)
        form.setContentsMargins(16, 20, 16, 20)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        
        self._max_trays_input = QSpinBox()
        self._max_trays_input.setRange(1, 500)
        self._max_trays_input.setValue(settings.trolley_max_trays)
        self._max_trays_input.setSuffix(" trays")
        form.addRow("📦 Max Trays per Trolley", self._max_trays_input)
        
        self._total_trolley_input = QSpinBox()
        self._total_trolley_input.setRange(1, 999)
        self._total_trolley_input.setValue(int(getattr(settings, "total_trolley_count", 99)))
        self._total_trolley_input.setSuffix(" trolleys")
        form.addRow("🚗 Total Trolleys", self._total_trolley_input)
        
        self._auto_group_gap_input = QSpinBox()
        self._auto_group_gap_input.setRange(0, 240)
        self._auto_group_gap_input.setValue(int(getattr(settings, "auto_group_gap_minutes", 10)))
        self._auto_group_gap_input.setSuffix(" minutes")
        form.addRow("⏱️ Auto Group Gap", self._auto_group_gap_input)
        
        self._target_aging_input = QDoubleSpinBox()
        self._target_aging_input.setRange(0.1, 72.0)
        self._target_aging_input.setDecimals(1)
        self._target_aging_input.setSingleStep(0.5)
        self._target_aging_input.setValue(settings.target_aging_hours)
        self._target_aging_input.setSuffix(" hours")
        form.addRow("🎯 Target Aging", self._target_aging_input)
        
        self._aging_tolerance_input = QDoubleSpinBox()
        self._aging_tolerance_input.setRange(0.0, 24.0)
        self._aging_tolerance_input.setDecimals(1)
        self._aging_tolerance_input.setSingleStep(0.5)
        self._aging_tolerance_input.setValue(settings.target_aging_tolerance_hours)
        self._aging_tolerance_input.setSuffix(" hours")
        form.addRow("±️ Aging Tolerance", self._aging_tolerance_input)
        
        return widget
    
    def _create_performance_tab(self) -> QWidget:
        widget = QWidget()
        form = QFormLayout(widget)
        form.setSpacing(16)
        form.setContentsMargins(16, 20, 16, 20)
        form.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)
        
        self._refresh_interval_input = QDoubleSpinBox()
        self._refresh_interval_input.setRange(0.5, 60.0)
        self._refresh_interval_input.setDecimals(1)
        self._refresh_interval_input.setSingleStep(0.5)
        self._refresh_interval_input.setValue(settings.delta_poll_interval_seconds)
        self._refresh_interval_input.setSuffix(" seconds")
        form.addRow("🔄 Refresh Interval", self._refresh_interval_input)
        
        self._data_window_days_input = QSpinBox()
        self._data_window_days_input.setRange(1, _MAX_UI_WINDOW_DAYS)
        self._data_window_days_input.setValue(int(getattr(settings, "ui_data_window_days", 1)))
        self._data_window_days_input.setSuffix(" day(s)")
        form.addRow("📅 Data Window", self._data_window_days_input)
        
        self._lookback_label = QLabel()
        self._lookback_label.setStyleSheet(f"color: {ThemeColors.TEXT_MUTED}; font-size: 12px;")
        self._data_window_days_input.valueChanged.connect(self._on_data_window_changed)
        self._on_data_window_changed(self._data_window_days_input.value())
        form.addRow("", self._lookback_label)
        
        return widget
    
    def _on_data_window_changed(self, value: int) -> None:
        days = max(1, min(_MAX_UI_WINDOW_DAYS, int(value)))
        lookback_hours = int(_lookback_hours_for_days(days))
        self._lookback_label.setText(f"📊 Data lookback: {lookback_hours} hours ({days} day window)")
    
    def payload(self) -> dict[str, object]:
        days = int(self._data_window_days_input.value())
        lookback_hours = _lookback_hours_for_days(days)
        return {
            "sql_server": self._host_input.text().strip(),
            "sql_user": self._user_input.text().strip(),
            "sql_password": self._pass_input.text(),
            "trolley_max_trays": int(self._max_trays_input.value()),
            "total_trolley_count": int(self._total_trolley_input.value()),
            "auto_group_gap_minutes": int(self._auto_group_gap_input.value()),
            "target_aging_hours": float(self._target_aging_input.value()),
            "target_aging_tolerance_hours": float(self._aging_tolerance_input.value()),
            "refresh_interval_seconds": float(self._refresh_interval_input.value()),
            "data_window_days": days,
            "initial_load_lookback_hours": lookback_hours,
        }


# ═══════════════════════════════════════════════════════════════════════════════
# TROLLEY DETAIL DIALOG
# ═══════════════════════════════════════════════════════════════════════════════

class _TrayDetailBridge(QObject):
    loaded = Signal(str, int, object, object)


class _ModernTrolleyDetailDialog(QDialog):
    """Modern trolley detail dialog with better data visualization"""
    
    _worker_lock = threading.Lock()
    
    def __init__(
        self,
        runtime: _Runtime,
        trolley_id: str,
        tray_rows: list[dict[str, str]],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._runtime = runtime
        self._bridge = _TrayDetailBridge()
        self._closed = False
        self._active_tray_id = ""
        self._active_request_id = 0
        self._request_seq = 0
        self._loading = False
        self._pending_tray_id: str | None = None
        self._tray_cells_cache: dict[str, list[dict[str, str | None]]] = {}
        self._bridge.loaded.connect(self._on_cells_loaded)
        
        self.setWindowTitle(f"🚗 Trolley Detail - {trolley_id}")
        self.resize(1000, 700)
        self._setup_ui(trolley_id, tray_rows)
    
    def _setup_ui(self, trolley_id: str, tray_rows: list[dict[str, str]]) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        layout.setSpacing(20)
        
        # Header
        header_layout = QHBoxLayout()
        
        title = QLabel(f"🚗 {trolley_id}")
        title.setStyleSheet(f"font-size: 24px; font-weight: 700; color: {ThemeColors.TEXT};")
        header_layout.addWidget(title)
        
        header_layout.addStretch()
        
        # Stats badges
        tray_count = len(tray_rows)
        total_cells = sum(int(row.get("quantity", 0) or 0) for row in tray_rows if row.get("quantity"))
        
        tray_badge = QLabel(f"📦 {tray_count} trays")
        tray_badge.setStyleSheet(f"""
            background: {ThemeColors.PRIMARY_LIGHT};
            color: {ThemeColors.PRIMARY};
            padding: 8px 16px;
            border-radius: 16px;
            font-weight: 600;
        """)
        header_layout.addWidget(tray_badge)
        
        cell_badge = QLabel(f"🔋 {total_cells:,} cells")
        cell_badge.setStyleSheet(f"""
            background: {ThemeColors.SUCCESS_BG};
            color: {ThemeColors.SUCCESS};
            padding: 8px 16px;
            border-radius: 16px;
            font-weight: 600;
        """)
        header_layout.addWidget(cell_badge)
        
        layout.addLayout(header_layout)
        
        # Splitter for two tables
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.setHandleWidth(8)
        
        # Tray summary section
        summary_widget = QFrame()
        summary_widget.setStyleSheet(f"""
            QFrame {{
                background: {ThemeColors.SURFACE};
                border: 1px solid {ThemeColors.BORDER};
                border-radius: 12px;
            }}
        """)
        summary_layout = QVBoxLayout(summary_widget)
        summary_layout.setContentsMargins(16, 16, 16, 16)
        
        summary_header = QLabel("📋 Tray Summary")
        summary_header.setStyleSheet(f"font-size: 14px; font-weight: 600; color: {ThemeColors.TEXT}; border: none;")
        summary_layout.addWidget(summary_header)
        
        self._summary_table = QTableWidget()
        self._summary_table.setColumnCount(5)
        self._summary_table.setHorizontalHeaderLabels(["Tray ID", "Start Time", "End Time", "Aging", "Status"])
        self._summary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._summary_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._summary_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._summary_table.verticalHeader().setVisible(False)
        self._summary_table.setShowGrid(False)
        self._summary_table.setAlternatingRowColors(True)
        
        summary_h = self._summary_table.horizontalHeader()
        summary_h.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        summary_h.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        
        summary_layout.addWidget(self._summary_table)
        splitter.addWidget(summary_widget)
        
        # Cell detail section
        cell_widget = QFrame()
        cell_widget.setStyleSheet(f"""
            QFrame {{
                background: {ThemeColors.SURFACE};
                border: 1px solid {ThemeColors.BORDER};
                border-radius: 12px;
            }}
        """)
        cell_layout = QVBoxLayout(cell_widget)
        cell_layout.setContentsMargins(16, 16, 16, 16)
        
        cell_header_layout = QHBoxLayout()
        cell_header = QLabel("🔋 Cell Detail")
        cell_header.setStyleSheet(f"font-size: 14px; font-weight: 600; color: {ThemeColors.TEXT}; border: none;")
        cell_header_layout.addWidget(cell_header)
        
        cell_header_layout.addStretch()
        
        self._loading_indicator = QLabel("⏳ Loading...")
        self._loading_indicator.setStyleSheet(f"color: {ThemeColors.WARNING}; font-size: 12px; border: none;")
        self._loading_indicator.setVisible(False)
        cell_header_layout.addWidget(self._loading_indicator)
        
        cell_layout.addLayout(cell_header_layout)
        
        self._cell_table = QTableWidget()
        self._cell_table.setColumnCount(3)
        self._cell_table.setHorizontalHeaderLabels(["Cell ID", "Start Time", "End Time"])
        self._cell_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._cell_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._cell_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._cell_table.verticalHeader().setVisible(False)
        self._cell_table.setShowGrid(False)
        self._cell_table.setAlternatingRowColors(True)
        
        cell_h = self._cell_table.horizontalHeader()
        cell_h.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        cell_h.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)
        
        cell_layout.addWidget(self._cell_table)
        splitter.addWidget(cell_widget)
        
        splitter.setSizes([300, 350])
        layout.addWidget(splitter, 1)
        
        # Status bar
        self._status = QLabel("Select a tray to view cell details")
        self._status.setStyleSheet(f"color: {ThemeColors.TEXT_MUTED}; font-size: 12px;")
        layout.addWidget(self._status)
        
        # Fill data and connect
        self._fill_summary(tray_rows)
        self._summary_table.itemSelectionChanged.connect(self._on_summary_selection_changed)
        if self._summary_table.rowCount() > 0:
            self._summary_table.selectRow(0)
    
    def _fill_summary(self, tray_rows: list[dict[str, str]]) -> None:
        self._summary_table.setRowCount(len(tray_rows))
        for row_idx, row in enumerate(tray_rows):
            tray_item = QTableWidgetItem(row.get("tray_id", ""))
            tray_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            tray_item.setData(Qt.ItemDataRole.UserRole, row.get("tray_id", ""))
            self._summary_table.setItem(row_idx, 0, tray_item)
            self._summary_table.setItem(row_idx, 1, self._center_item(row.get("start_time", "-")))
            self._summary_table.setItem(row_idx, 2, self._center_item(row.get("end_time", "-")))
            self._summary_table.setItem(row_idx, 3, self._center_item(row.get("aging_time", "-")))
            
            # Status with color
            status = row.get("status", "-")
            status_item = QTableWidgetItem(status)
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if status == "Aged":
                status_item.setForeground(QColor(ThemeColors.SUCCESS))
            elif status == "Aging":
                status_item.setForeground(QColor(ThemeColors.WARNING))
            elif status == "Aged Out":
                status_item.setForeground(QColor(ThemeColors.DANGER))
            self._summary_table.setItem(row_idx, 4, status_item)
    
    @staticmethod
    def _center_item(text: str) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        return item
    
    def _on_summary_selection_changed(self) -> None:
        if self._closed or not self.isVisible():
            return
        selected = self._summary_table.selectedItems()
        if not selected:
            return
        row = selected[0].row()
        item = self._summary_table.item(row, 0)
        if item is None:
            return
        tray_id = str(item.data(Qt.ItemDataRole.UserRole) or "").strip()
        if not tray_id:
            return
        
        if self._loading and tray_id == self._active_tray_id:
            return
        if self._pending_tray_id == tray_id:
            return
        
        cached = self._tray_cells_cache.get(tray_id)
        if cached is not None:
            self._active_tray_id = tray_id
            self._render_cells(tray_id, cached)
            self._status.setText(f"✅ Loaded {len(cached)} cells for {tray_id} (cached)")
            return
        
        if self._loading:
            self._pending_tray_id = tray_id
            self._status.setText(f"⏳ Queued: {tray_id}")
            return
        
        self._start_tray_cells_query(tray_id)
    
    def _start_tray_cells_query(self, tray_id: str) -> None:
        self._request_seq += 1
        request_id = self._request_seq
        self._active_request_id = request_id
        self._active_tray_id = tray_id
        self._loading = True
        self._pending_tray_id = None
        self._loading_indicator.setVisible(True)
        self._status.setText(f"⏳ Loading cells for {tray_id}...")
        self._cell_table.setRowCount(0)
        
        def _worker() -> None:
            acquired = type(self)._worker_lock.acquire(timeout=60.0)
            if not acquired:
                self._bridge.loaded.emit(tray_id, request_id, None, "Request timed out")
                return
            try:
                rows = self._runtime.tray_cells(tray_id)
                self._bridge.loaded.emit(tray_id, request_id, rows, None)
            except Exception as exc:
                self._bridge.loaded.emit(tray_id, request_id, None, str(exc))
            finally:
                type(self)._worker_lock.release()
        
        threading.Thread(target=_worker, name=f"tray-detail-{tray_id}", daemon=True).start()
    
    def _render_cells(self, tray_id: str, rows: list[dict[str, str | None]]) -> None:
        self._cell_table.setRowCount(len(rows))
        for row_idx, row_data in enumerate(rows):
            self._cell_table.setItem(row_idx, 0, self._center_item(str(row_data.get("cell_id", ""))))
            self._cell_table.setItem(row_idx, 1, self._center_item(str(row_data.get("start_time") or "-")))
            self._cell_table.setItem(row_idx, 2, self._center_item(str(row_data.get("end_time") or "-")))
    
    def _on_cells_loaded(self, tray_id: str, request_id: int, rows: object, error: object) -> None:
        if self._closed:
            return
        if request_id != self._active_request_id:
            return
        
        self._loading = False
        self._loading_indicator.setVisible(False)
        
        if error:
            self._status.setText(f"❌ Failed: {error}")
            pending = self._pending_tray_id
            self._pending_tray_id = None
            if pending and pending != tray_id:
                self._start_tray_cells_query(pending)
            return
        
        values = list(rows or [])
        self._tray_cells_cache[tray_id] = [dict(item) for item in values]
        self._render_cells(tray_id, self._tray_cells_cache[tray_id])
        
        if not values:
            self._status.setText("⚠️ No cell data found for this tray")
        else:
            self._status.setText(f"✅ Loaded {len(values)} cells for {tray_id}")
        
        pending = self._pending_tray_id
        self._pending_tray_id = None
        if pending and pending != tray_id:
            self._start_tray_cells_query(pending)
    
    def closeEvent(self, event) -> None:
        self._closed = True
        super().closeEvent(event)

# ═══════════════════════════════════════════════════════════════════════════════
# MODERN MAIN WINDOW
# ═══════════════════════════════════════════════════════════════════════════════

class _MainWindow(QMainWindow):
    """Modern main window with enhanced UX"""
    
    def __init__(self, vm: BoardViewModel, runtime: _Runtime) -> None:
        super().__init__()
        self._vm = vm
        self._runtime = runtime
        self._action_bridge = _UiActionBridge()
        self._action_inflight = False
        self._action_locked_ui = False
        self._settings_inflight = False
        self._pending_db_restart_notice = False
        self._pending_settings_context: dict[str, Any] | None = None
        self._window_load_monitor_days: int | None = None
        self._window_status_check_inflight = False
        self._suggested_trolley_id = ""
        self._centered_once = False
        self._last_update_time: datetime | None = None
        self._viewer_mode = self._runtime.is_viewer_mode()
        
        self._window_status_timer = QTimer(self)
        self._window_status_timer.setInterval(2000)
        self._window_status_timer.timeout.connect(self._on_window_status_timer)
        
        self._clock_timer = QTimer(self)
        self._clock_timer.setInterval(1000)
        self._clock_timer.timeout.connect(self._update_clock)
        self._clock_timer.start()
        
        self._event_filter_installed = False
        
        self.setWindowTitle("🏭 WIP Management")
        self.resize(1500, 900)
        self.setStyleSheet(get_modern_stylesheet())
        
        self._setup_ui()
        self._auto_group_last_state = bool(self._auto_group_checkbox.isChecked())
        self._setup_connections()
        self._setup_shortcuts()
        self._apply_runtime_mode()
        
        app = QApplication.instance()
        if app is not None:
            app.installEventFilter(self)
            self._event_filter_installed = True
    
    def _setup_ui(self) -> None:
        root = QWidget(self)
        layout = QVBoxLayout(root)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(20)
        
        # ═══════════════════════════════════════════════════════════════
        # TOP SECTION: Metrics
        # ═══════════════════════════════════════════════════════════════
        metrics_row = QHBoxLayout()
        metrics_row.setSpacing(16)
        
        self._metric_boxes = {
            "tray_count": _ModernMetricCard("Total Trays", "📦", ThemeColors.PRIMARY),
            "group_count": _ModernMetricCard("Grouped", "✓", ThemeColors.SUCCESS),
            "assembly_ungroup_count": _ModernMetricCard("Ungrouped", "⏳", ThemeColors.WARNING),
            "assembly_trolley_count": _ModernMetricCard("Assembly", "🏭", "#8b5cf6"),
            "queue_trolley_count": _ModernMetricCard("Queue", "📋", ThemeColors.INFO),
            "precharge_trolley_count": _ModernMetricCard("Precharge", "⚡", "#f59e0b"),
        }
        
        for key in ["tray_count", "group_count", "assembly_ungroup_count", 
                    "assembly_trolley_count", "queue_trolley_count", "precharge_trolley_count"]:
            card = self._metric_boxes[key]
            card.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
            metrics_row.addWidget(card, 1)
        
        layout.addLayout(metrics_row)
        
        # ═══════════════════════════════════════════════════════════════
        # TOOLBAR
        # ═══════════════════════════════════════════════════════════════
        toolbar = QFrame()
        toolbar.setStyleSheet(f"""
            QFrame {{
                background: {ThemeColors.SURFACE};
                border: 1px solid {ThemeColors.BORDER};
                border-radius: 12px;
            }}
        """)
        toolbar_layout = QHBoxLayout(toolbar)
        toolbar_layout.setContentsMargins(16, 12, 16, 12)
        toolbar_layout.setSpacing(12)
        
        # Search
        search_container = QFrame()
        search_container.setStyleSheet(f"""
            QFrame {{
                background: {ThemeColors.BG};
                border: 1px solid {ThemeColors.BORDER};
                border-radius: 10px;
            }}
        """)
        search_layout = QHBoxLayout(search_container)
        search_layout.setContentsMargins(12, 0, 4, 0)
        search_layout.setSpacing(8)
        
        search_icon = QLabel("🔍")
        search_layout.addWidget(search_icon)
        
        self._search_input = QLineEdit()
        self._search_input.setPlaceholderText("Search Tray/Cell ID...")
        self._search_input.setFrame(False)
        self._search_input.setStyleSheet("background: transparent; border: none; padding: 10px 0;")
        self._search_input.setMinimumWidth(180)
        search_layout.addWidget(self._search_input)
        
        self._search_btn = QPushButton("Search")
        self._search_btn.setObjectName("secondaryBtn")
        self._search_btn.setMaximumWidth(80)
        search_layout.addWidget(self._search_btn)
        
        toolbar_layout.addWidget(search_container)
        toolbar_layout.addStretch()
        
        # Action buttons
        self._quick_refresh_btn = QPushButton("⟳ Quick")
        self._quick_refresh_btn.setObjectName("secondaryBtn")
        self._quick_refresh_btn.setToolTip("Quick refresh (delta sync)")
        toolbar_layout.addWidget(self._quick_refresh_btn)
        
        self._full_refresh_btn = QPushButton("↻ Full")
        self._full_refresh_btn.setObjectName("secondaryBtn")
        self._full_refresh_btn.setToolTip("Full refresh (rescan all)")
        toolbar_layout.addWidget(self._full_refresh_btn)
        
        # Separator
        sep1 = QFrame()
        sep1.setFrameShape(QFrame.Shape.VLine)
        sep1.setStyleSheet(f"background: {ThemeColors.BORDER};")
        sep1.setMaximumWidth(1)
        toolbar_layout.addWidget(sep1)
        
        # Auto group toggle
        self._auto_group_checkbox = QCheckBox("Auto Group")
        self._auto_group_checkbox.setChecked(settings.auto_group_default_enabled)
        self._auto_group_checkbox.setToolTip("Enable automatic tray grouping")
        toolbar_layout.addWidget(self._auto_group_checkbox)
        
        # Separator
        sep2 = QFrame()
        sep2.setFrameShape(QFrame.Shape.VLine)
        sep2.setStyleSheet(f"background: {ThemeColors.BORDER};")
        sep2.setMaximumWidth(1)
        toolbar_layout.addWidget(sep2)
        
        # Settings button
        self._settings_btn = QPushButton("⚙")
        self._settings_btn.setObjectName("iconBtn")
        self._settings_btn.setToolTip("Open settings")
        toolbar_layout.addWidget(self._settings_btn)
        
        layout.addWidget(toolbar)
        
        # ═══════════════════════════════════════════════════════════════
        # MAIN CONTENT: 3 Columns
        # ═══════════════════════════════════════════════════════════════
        
        # Ungroup actions widget (for Assembly card)
        ungroup_actions = QWidget()
        ungroup_actions_layout = QHBoxLayout(ungroup_actions)
        ungroup_actions_layout.setContentsMargins(0, 0, 0, 0)
        ungroup_actions_layout.setSpacing(8)
        
        self._add_trolley_input = QLineEdit()
        self._add_trolley_input.setPlaceholderText("Trolley ID")
        self._add_trolley_input.setMaximumWidth(120)
        self._refresh_suggested_trolley_id(force=True)
        ungroup_actions_layout.addWidget(self._add_trolley_input)
        
        self._add_trolley_btn = QPushButton("+ Add")
        self._add_trolley_btn.setObjectName("successBtn")
        self._add_trolley_btn.setMaximumWidth(80)
        ungroup_actions_layout.addWidget(self._add_trolley_btn)
        
        # Board with 3 columns
        board = QHBoxLayout()
        board.setSpacing(16)
        
        self._assembly_card = _ModernColumnCard(
            "Assembly", "🏭", "#8b5cf6",
            self._vm.assembly_trolley_model,
            tray_model=self._vm.assembly_ungrouped_model,
            tray_title="Ungrouped Trays",
            tray_actions_widget=ungroup_actions,
            on_trolley_context=self._on_trolley_item_context,
            on_trolley_double_click=self._on_trolley_item_double_click,
            on_trolley_click=self._on_trolley_item_clicked,
            on_tray_header_click=self._on_ungroup_header_clicked,
        )
        
        self._queue_card = _ModernColumnCard(
            "Queue", "📋", ThemeColors.INFO,
            self._vm.queue_trolley_model,
            on_trolley_context=self._on_trolley_item_context,
            on_trolley_double_click=self._on_trolley_item_double_click,
            on_trolley_click=self._on_trolley_item_clicked,
        )
        
        self._precharge_card = _ModernColumnCard(
            "Precharge", "⚡", "#f59e0b",
            self._vm.precharge_trolley_model,
            on_trolley_context=self._on_trolley_item_context,
            on_trolley_double_click=self._on_trolley_item_double_click,
            on_trolley_click=self._on_trolley_item_clicked,
        )
        
        board.addWidget(self._assembly_card, 1)
        board.addWidget(self._queue_card, 1)
        board.addWidget(self._precharge_card, 1)
        
        layout.addLayout(board, 1)
        
        self.setCentralWidget(root)
        
        # ═══════════════════════════════════════════════════════════════
        # STATUS BAR
        # ═══════════════════════════════════════════════════════════════
        status_bar = self.statusBar()
        status_bar.setSizeGripEnabled(False)
        
        self._status = QLabel("Ready")
        self._status.setStyleSheet(f"color: {ThemeColors.TEXT_SECONDARY}; padding: 4px 8px;")
        status_bar.addWidget(self._status, 1)
        
        self._connection_status = QLabel("🟢 Connected")
        self._connection_status.setStyleSheet(f"color: {ThemeColors.SUCCESS}; font-weight: 500; padding: 4px 12px;")
        status_bar.addPermanentWidget(self._connection_status)
        
        self._clock_label = QLabel("")
        self._clock_label.setStyleSheet(f"color: {ThemeColors.TEXT_MUTED}; padding: 4px 12px;")
        status_bar.addPermanentWidget(self._clock_label)
        self._update_clock()
    
    def _setup_connections(self) -> None:
        self._vm.updated.connect(self._on_updated)
        self._quick_refresh_btn.clicked.connect(self._on_quick_refresh)
        self._full_refresh_btn.clicked.connect(self._on_full_refresh)
        self._add_trolley_btn.clicked.connect(self._on_add_trolley)
        self._search_btn.clicked.connect(self._on_search)
        self._search_input.returnPressed.connect(self._on_search)
        self._settings_btn.clicked.connect(self._on_settings)
        self._auto_group_checkbox.toggled.connect(self._on_auto_group_toggled)
        self._action_bridge.action_done.connect(self._on_action_done)
    
    def _setup_shortcuts(self) -> None:
        # Ctrl+R: Quick refresh
        QShortcut(QKeySequence("Ctrl+R"), self, self._on_quick_refresh)
        # Ctrl+Shift+R: Full refresh
        QShortcut(QKeySequence("Ctrl+Shift+R"), self, self._on_full_refresh)
        # Ctrl+F: Focus search
        QShortcut(QKeySequence("Ctrl+F"), self, lambda: self._search_input.setFocus())
        # Ctrl+,: Settings
        QShortcut(QKeySequence("Ctrl+,"), self, self._on_settings)
        # Escape: Clear selection
        QShortcut(QKeySequence("Escape"), self, self._clear_all_selections)
    
    def _update_clock(self) -> None:
        now = datetime.now()
        self._clock_label.setText(now.strftime("%H:%M:%S"))

    def _set_auto_group_checkbox_safely(self, checked: bool) -> None:
        blocker = QSignalBlocker(self._auto_group_checkbox)
        self._auto_group_checkbox.setChecked(bool(checked))
        del blocker

    def _apply_runtime_mode(self) -> None:
        if not self._viewer_mode:
            return
        self._quick_refresh_btn.setEnabled(False)
        self._full_refresh_btn.setEnabled(False)
        self._auto_group_checkbox.setEnabled(False)
        self._settings_btn.setEnabled(False)
        self._add_trolley_input.setEnabled(False)
        self._add_trolley_btn.setEnabled(False)
        self._connection_status.setText("🟡 Viewer mode")
        self._connection_status.setStyleSheet(f"color: {ThemeColors.WARNING}; font-weight: 500; padding: 4px 12px;")
        self._show_status("⚠️ Viewer mode: use leader instance for Auto group and manual grouping", "warning")
    
    def showEvent(self, event) -> None:
        super().showEvent(event)
        if self._centered_once:
            return
        self._centered_once = True
        self._center_in_screen()
    
    def _center_in_screen(self) -> None:
        screen = self.screen() or QApplication.primaryScreen()
        if screen is None:
            return
        available = screen.availableGeometry()
        max_width = max(800, int(available.width() * 0.95))
        max_height = max(600, int(available.height() * 0.90))
        if self.width() > max_width or self.height() > max_height:
            self.resize(min(self.width(), max_width), min(self.height(), max_height))
        frame = self.frameGeometry()
        frame.moveCenter(available.center())
        self.move(frame.topLeft())
    
    def _on_updated(self, payload: dict[str, int]) -> None:
        self._last_update_time = datetime.now()
        
        # Update metric cards
        for key, box in self._metric_boxes.items():
            box.set_value(int(payload.get(key, 0)))
        
        # Update column stats
        self._assembly_card.set_stats_text(
            f"🚗 {payload.get('assembly_trolley_count', 0)} • "
            f"📦 {payload.get('assembly_tray_count', 0)} • "
            f"🔋 {payload.get('assembly_cell_count', 0):,}"
        )
        self._queue_card.set_stats_text(
            f"🚗 {payload.get('queue_trolley_count', 0)} • "
            f"📦 {payload.get('queue_tray_count', 0)} • "
            f"🔋 {payload.get('queue_cell_count', 0):,}"
        )
        self._precharge_card.set_stats_text(
            f"🚗 {payload.get('precharge_trolley_count', 0)} • "
            f"📦 {payload.get('precharge_tray_count', 0)} • "
            f"🔋 {payload.get('precharge_cell_count', 0):,}"
        )
        
        self._refresh_suggested_trolley_id(force=False)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ACTION HANDLERS (Keep original logic, updated styling)
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _on_quick_refresh(self) -> None:
        self._run_action(
            action_name="Quick refresh",
            action=lambda: self._runtime.manual_refresh(full_scan=False),
            lock_ui=False,
        )
    
    def _on_full_refresh(self) -> None:
        self._run_action(
            action_name="Full refresh",
            action=lambda: self._runtime.manual_refresh(full_scan=True),
            lock_ui=False,
        )
    
    def _on_search(self) -> None:
        raw = self._search_input.text().strip()
        if not raw:
            self._show_status("⚠️ Search input is empty", "warning")
            return
        lookup = raw.upper()
        tray_payload = self._vm.tray_payload_by_id(lookup)
        if tray_payload is not None:
            self._show_status(self._format_tray_search_result(lookup, tray_payload), "success")
            return
        self._pending_search_query = lookup
        self._run_action(
            action_name="Search",
            action=lambda: self._runtime.cell_owner(lookup),
        )
    
    def _format_tray_search_result(self, tray_id: str, tray_payload: dict[str, Any]) -> str:
        location, trolley_id = self._find_tray_location(tray_id)
        ccu_payload = tray_payload.get("ccu_payload") or {}
        fpc_payload = tray_payload.get("fpc_payload") or {}
        start_dt = parse_datetime(ccu_payload.get("start_time"))
        end_dt = parse_datetime(ccu_payload.get("end_time"))
        qty = 0
        try:
            qty = int(str(ccu_payload.get("quantity") or fpc_payload.get("cell_count") or 0).strip())
        except Exception:
            pass
        aging_text = "-"
        if end_dt is not None:
            aging_text = _format_timedelta(datetime.now() - end_dt)
        return (
            f"📦 {tray_id} | 🚗 {trolley_id or '-'} | 📍 {location} | "
            f"🔋 {qty} cells | ⏱️ {aging_text}"
        )
    
    def _find_tray_location(self, tray_id: str) -> tuple[str, str]:
        tray_key = tray_id.strip()
        if not tray_key:
            return "Unknown", ""
        precharge_row = self._vm.precharge_trolley_model.find_by_tray_id(tray_key)
        if precharge_row is not None:
            return "Precharge", precharge_row.trolley_id
        queue_row = self._vm.queue_trolley_model.find_by_tray_id(tray_key)
        if queue_row is not None:
            return "Queue", queue_row.trolley_id
        assembly_row = self._vm.assembly_trolley_model.find_by_tray_id(tray_key)
        if assembly_row is not None:
            return "Assembly", assembly_row.trolley_id
        if self._vm.assembly_ungrouped_model.has_tray(tray_key):
            return "Assembly (Ungrouped)", ""
        return "Unknown", ""
    
    def _on_add_trolley(self) -> None:
        tray_ids = self._selected_ungroup_tray_ids()
        if not tray_ids:
            self._show_status("⚠️ Select trays in Ungrouped list first", "warning")
            return
        trolley_id = self._add_trolley_input.text().strip()
        if not trolley_id:
            self._refresh_suggested_trolley_id(force=True)
            trolley_id = self._add_trolley_input.text().strip()
            if not trolley_id:
                self._show_status("⚠️ Enter a Trolley ID", "warning")
                return
        self._run_action(
            action_name="Add trolley",
            action=lambda: self._runtime.manual_group_many(
                tray_ids=tray_ids,
                trolley_id=trolley_id,
                column=Column.QUEUE,
            ),
        )
    
    def _on_ungroup_header_clicked(self, section: int) -> None:
        if section == 0:
            self._vm.assembly_ungrouped_model.toggle_all_checked()
    
    def _selected_ungroup_tray_ids(self) -> list[str]:
        tray_ids = self._vm.assembly_ungrouped_model.checked_tray_ids()
        if tray_ids:
            return tray_ids
        tray_table = self._assembly_card.tray_table
        if tray_table is None or tray_table.selectionModel() is None:
            return []
        selected_rows = {index.row() for index in tray_table.selectionModel().selectedIndexes()}
        out: list[str] = []
        for row in sorted(selected_rows):
            tray_index = self._vm.assembly_ungrouped_model.index(row, 2)
            tray_id = str(self._vm.assembly_ungrouped_model.data(tray_index, Qt.ItemDataRole.DisplayRole) or "").strip()
            if tray_id:
                out.append(tray_id)
        return out
    
    def _on_auto_group_toggled(self, checked: bool) -> None:
        if self._viewer_mode:
            self._set_auto_group_checkbox_safely(self._auto_group_last_state)
            self._show_status("⚠️ Viewer mode: Auto group can only be changed on leader instance", "warning")
            return
        self._run_action(
            action_name="Auto group",
            action=lambda: self._runtime.set_auto_group_enabled(checked),
        )
    
    def _on_settings(self) -> None:
        dialog = _ModernSettingsDialog(self)
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        payload = dialog.payload()
        
        # Apply settings (same logic as original)
        sql_server = str(payload["sql_server"]).strip()
        sql_user = str(payload["sql_user"]).strip()
        sql_password = str(payload["sql_password"])
        
        if not sql_server or not sql_user:
            self._show_status("❌ Database host/user cannot be empty", "danger")
            return
        
        # Store previous values for comparison
        prev_max_trays = settings.trolley_max_trays
        prev_total_trolley = getattr(settings, "total_trolley_count", 99)
        prev_refresh = settings.delta_poll_interval_seconds
        prev_window_days = settings.ui_data_window_days
        
        # Apply new settings
        db_changed = (
            sql_server != settings.sql_server or
            sql_user != settings.sql_user or
            sql_password != settings.sql_password
        )
        
        settings.sql_server = sql_server
        settings.sql_user = sql_user
        settings.sql_password = sql_password
        settings.trolley_max_trays = int(payload["trolley_max_trays"])
        settings.total_trolley_count = int(payload["total_trolley_count"])
        settings.auto_group_gap_minutes = int(payload["auto_group_gap_minutes"])
        settings.target_aging_hours = float(payload["target_aging_hours"])
        settings.target_aging_tolerance_hours = float(payload["target_aging_tolerance_hours"])
        settings.delta_poll_interval_seconds = float(payload["refresh_interval_seconds"])
        settings.ui_data_window_days = int(payload["data_window_days"])
        _sync_data_window_settings()
        
        # Save to .env
        try:
            _upsert_env_values(Path.cwd() / ".env", {
                "SQL_SERVER": settings.sql_server,
                "SQL_USER": settings.sql_user,
                "SQL_PASSWORD": settings.sql_password,
                "TROLLEY_MAX_TRAYS": str(settings.trolley_max_trays),
                "TOTAL_TROLLEY_COUNT": str(settings.total_trolley_count),
                "AUTO_GROUP_GAP_MINUTES": str(settings.auto_group_gap_minutes),
                "TARGET_AGING_HOURS": str(settings.target_aging_hours),
                "TARGET_AGING_TOLERANCE_HOURS": str(settings.target_aging_tolerance_hours),
                "DELTA_POLL_INTERVAL_SECONDS": str(settings.delta_poll_interval_seconds),
                "UI_DATA_WINDOW_DAYS": str(settings.ui_data_window_days),
            })
        except Exception as exc:
            QMessageBox.critical(self, "Settings", f"Failed to save settings: {exc}")
            return
        
        # Check if runtime update needed
        runtime_changed = (
            prev_max_trays != settings.trolley_max_trays or
            prev_total_trolley != settings.total_trolley_count or
            abs(prev_refresh - settings.delta_poll_interval_seconds) > 0.01 or
            prev_window_days != settings.ui_data_window_days
        )
        
        self._pending_db_restart_notice = db_changed
        
        if not runtime_changed:
            msg = "✅ Settings saved"
            if db_changed:
                msg += " (restart required for DB changes)"
            self._show_status(msg, "success")
            return
        
        self._pending_settings_context = {
            "max_trays_changed": prev_max_trays != settings.trolley_max_trays,
            "total_trolley_changed": prev_total_trolley != settings.total_trolley_count,
            "refresh_changed": abs(prev_refresh - settings.delta_poll_interval_seconds) > 0.01,
            "window_days_changed": prev_window_days != settings.ui_data_window_days,
            "data_window_days": settings.ui_data_window_days,
        }
        
        self._run_settings_action(
            action=lambda: self._runtime.update_grouping_settings(
                max_trays_per_trolley=settings.trolley_max_trays,
                total_trolley_count=settings.total_trolley_count,
                refresh_interval_seconds=settings.delta_poll_interval_seconds,
                data_window_days=settings.ui_data_window_days,
            )
        )
    
    # ═══════════════════════════════════════════════════════════════════════════
    # TROLLEY INTERACTIONS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _on_trolley_item_clicked(self, list_view: QListView, index: QModelIndex) -> None:
        if not index.isValid():
            return
        self._clear_trolley_selection_except(list_view)
    
    def _clear_trolley_selection_except(self, active_list: QListView) -> None:
        for lv in [self._assembly_card.trolley_list, self._queue_card.trolley_list, self._precharge_card.trolley_list]:
            if lv is not active_list:
                with contextlib.suppress(Exception):
                    lv.clearSelection()
    
    def _on_trolley_item_double_click(self, _: QListView, index: QModelIndex) -> None:
        payload = self._trolley_payload_from_index(index)
        if payload is None:
            return
        self._show_trolley_detail_dialog(payload)
    
    def _on_trolley_item_context(self, list_view: QListView, index: QModelIndex) -> None:
        selected_trolleys = self._selected_trolley_payloads(list_view, fallback_index=index)
        if not selected_trolleys:
            return
        global_pos = list_view.viewport().mapToGlobal(list_view.visualRect(index).bottomRight())
        self._show_trolley_menu(selected_trolleys[0], selected_trolleys, global_pos)
    
    def _selected_trolley_payloads(self, list_view: QListView, *, fallback_index: QModelIndex | None = None) -> list[dict[str, Any]]:
        selection_model = list_view.selectionModel()
        selected_rows = list(selection_model.selectedRows()) if selection_model else []
        
        if fallback_index is not None and fallback_index.isValid():
            if selected_rows and not any(i.row() == fallback_index.row() for i in selected_rows):
                selected_rows = [fallback_index]
                with contextlib.suppress(Exception):
                    list_view.clearSelection()
                    list_view.setCurrentIndex(fallback_index)
            elif not selected_rows:
                selected_rows = [fallback_index]
        
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        for index in sorted(selected_rows, key=lambda i: i.row()):
            payload = self._trolley_payload_from_index(index)
            if payload and payload["trolley_id"] not in seen:
                seen.add(payload["trolley_id"])
                out.append(payload)
        return out
    
    def _trolley_payload_from_index(self, index: QModelIndex) -> dict[str, Any] | None:
        if not index.isValid():
            return None
        model = index.model()
        if model is None:
            return None
        trolley_id = str(model.data(index, TrolleyListModel.TrolleyIdRole) or "").strip()
        if not trolley_id:
            return None
        return {
            "trolley_id": trolley_id,
            "tray_ids": list(model.data(index, TrolleyListModel.TrayIdsRole) or []),
            "mode": str(model.data(index, TrolleyListModel.ModeRole) or ""),
            "column": str(model.data(index, TrolleyListModel.ColumnRole) or ""),
            "state": str(model.data(index, TrolleyListModel.StateRole) or ""),
        }
    
    def _show_trolley_menu(self, trolley: dict[str, Any], selected_trolleys: list[dict[str, Any]], global_pos: QPoint) -> None:
        trolley_id = trolley.get("trolley_id", "")
        mode = str(trolley.get("mode", "")).lower()
        is_manual = mode == "manual"
        auto_group_enabled = self._auto_group_checkbox.isChecked()
        is_editable = is_manual or not auto_group_enabled
        multi_selected = len(selected_trolleys) > 1
        
        menu = QMenu(self)
        menu.setStyleSheet(f"""
            QMenu {{
                background: {ThemeColors.SURFACE};
                border: 1px solid {ThemeColors.BORDER};
                border-radius: 12px;
                padding: 8px;
            }}
            QMenu::item {{
                padding: 10px 20px;
                border-radius: 6px;
            }}
            QMenu::item:selected {{
                background: {ThemeColors.SURFACE_HOVER};
            }}
        """)
        
        view_action = menu.addAction("👁️ View Details")
        menu.addSeparator()
        rename_action = menu.addAction("✏️ Rename Trolley")
        add_action = menu.addAction("➕ Add Selected Trays")
        remove_action = menu.addAction("➖ Remove Trays...")
        menu.addSeparator()
        clear_action = menu.addAction("🗑️ Clear Trolley")
        delete_action = menu.addAction(f"❌ Delete {'Selected' if multi_selected else 'Trolley'}")
        
        # Disable based on state
        if multi_selected:
            rename_action.setEnabled(False)
            add_action.setEnabled(False)
            remove_action.setEnabled(False)
            clear_action.setEnabled(False)
            view_action.setEnabled(False)
        if not is_editable:
            add_action.setEnabled(False)
            remove_action.setEnabled(False)
            clear_action.setEnabled(False)
            delete_action.setEnabled(False)
        
        chosen = menu.exec(global_pos)
        if chosen is None:
            return
        
        if chosen == view_action:
            self._show_trolley_detail_dialog(trolley)
        elif chosen == rename_action:
            new_id, ok = QInputDialog.getText(self, "Rename Trolley", "New Trolley ID:", QLineEdit.EchoMode.Normal, trolley_id)
            if ok and new_id.strip() and new_id.strip() != trolley_id:
                self._run_action(
                    action_name="Rename trolley",
                    action=lambda: self._runtime.rename_trolley(trolley_id, new_id.strip()),
                )
        elif chosen == add_action:
            tray_ids = self._selected_ungroup_tray_ids()
            if not tray_ids:
                self._show_status("⚠️ Select trays first", "warning")
                return
            self._run_action(
                action_name="Add trays",
                action=lambda: self._runtime.manual_group_many(tray_ids, trolley_id, Column.QUEUE),
            )
        elif chosen == remove_action:
            tray_ids = trolley.get("tray_ids", [])
            if not tray_ids:
                self._show_status("⚠️ No trays to remove", "warning")
                return
            self._run_action(
                action_name="Remove trays",
                action=lambda: self._runtime.manual_ungroup_many(tray_ids),
            )
        elif chosen == clear_action:
            self._run_action(
                action_name="Clear trolley",
                action=lambda: self._runtime.clear_trolley(trolley_id),
            )
        elif chosen == delete_action:
            if multi_selected:
                ids = [t["trolley_id"] for t in selected_trolleys]
                self._run_action(
                    action_name="Delete trolleys",
                    action=lambda: self._runtime.delete_trolleys(ids),
                )
            else:
                self._run_action(
                    action_name="Delete trolley",
                    action=lambda: self._runtime.delete_trolley(trolley_id),
                )
    
    def _show_trolley_detail_dialog(self, trolley: dict[str, Any]) -> None:
        tray_ids = [str(t).strip() for t in trolley.get("tray_ids", []) if str(t).strip()]
        if not tray_ids:
            self._show_status("⚠️ Trolley has no trays", "warning")
            return
        tray_payloads = self._vm.tray_payloads(tray_ids)
        rows = self._build_

    def _show_trolley_detail_dialog(self, trolley: dict[str, Any]) -> None:
        tray_ids = [str(t).strip() for t in trolley.get("tray_ids", []) if str(t).strip()]
        if not tray_ids:
            self._show_status("⚠️ Trolley has no trays", "warning")
            return
        tray_payloads = self._vm.tray_payloads(tray_ids)
        rows = self._build_tray_summary_rows(tray_payloads)
        if not rows:
            self._show_status("⚠️ No tray data available", "warning")
            return
        dialog = _ModernTrolleyDetailDialog(
            runtime=self._runtime,
            trolley_id=str(trolley.get("trolley_id", "")),
            tray_rows=rows,
            parent=self,
        )
        dialog.exec()
    
    def _build_tray_summary_rows(self, tray_payloads: list[dict[str, Any]]) -> list[dict[str, str]]:
        now = datetime.now()
        out: list[dict[str, str]] = []
        for row in tray_payloads:
            tray_id = str(row.get("tray_id", "")).strip()
            if not tray_id:
                continue
            ccu_payload = row.get("ccu_payload") or {}
            fpc_payload = row.get("fpc_payload") or {}
            has_ccu = bool(ccu_payload)
            has_fpc = bool(fpc_payload)
            start_dt = parse_datetime(ccu_payload.get("start_time"))
            ccu_end_dt = parse_datetime(ccu_payload.get("end_time"))
            end_dt = ccu_end_dt
            precharge_start_dt = parse_datetime(fpc_payload.get("precharge_start_time")) if (has_ccu and has_fpc) else None
            
            if has_ccu and has_fpc:
                status_text = "Precharged"
                aging_text = _format_timedelta(precharge_start_dt - ccu_end_dt) if (ccu_end_dt and precharge_start_dt) else "-"
            elif has_ccu:
                if end_dt is None:
                    aging_text = "-"
                    status_text = "-"
                else:
                    aging_delta = now - end_dt
                    aging_text = _format_timedelta(aging_delta)
                    status_text = _aging_status_text(aging_delta)
            else:
                aging_text = "-"
                status_text = "-"
            
            out.append({
                "tray_id": tray_id,
                "start_time": start_dt.isoformat(sep=" ", timespec="seconds") if start_dt else "-",
                "end_time": end_dt.isoformat(sep=" ", timespec="seconds") if end_dt else "-",
                "aging_time": aging_text,
                "status": status_text,
                "quantity": str(ccu_payload.get("quantity") or fpc_payload.get("cell_count") or "0"),
            })
        out.sort(key=lambda item: (item.get("end_time", ""), item.get("tray_id", "")), reverse=True)
        return out
    
    # ═══════════════════════════════════════════════════════════════════════════
    # HELPER METHODS
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _format_tro_id_for_ui(self, seq: int, *, total_count: int) -> str:
        safe_total = max(1, int(total_count))
        safe_seq = max(1, int(seq))
        if safe_seq > safe_total:
            safe_seq = ((safe_seq - 1) % safe_total) + 1
        width = max(2, len(str(safe_total)))
        return f"TRO-{safe_seq:0{width}d}"
    
    def _parse_tro_seq_for_ui(self, trolley_id: str, *, total_count: int) -> int | None:
        text = str(trolley_id).strip().upper()
        if not text:
            return None
        match = _TRO_ID_PATTERN.match(text)
        if not match:
            return None
        try:
            seq = int(match.group(1))
        except Exception:
            return None
        if seq < 1 or seq > max(1, int(total_count)):
            return None
        return seq
    
    def _all_visible_trolley_ids(self) -> list[str]:
        return [
            *self._vm.assembly_trolley_model.trolley_ids(),
            *self._vm.queue_trolley_model.trolley_ids(),
            *self._vm.precharge_trolley_model.trolley_ids(),
        ]
    
    def _refresh_suggested_trolley_id(self, *, force: bool = False, used_trolley_id: str | None = None) -> None:
        total_count = max(1, int(getattr(settings, "total_trolley_count", 99)))
        previous_suggestion = self._suggested_trolley_id
        
        base_seq = self._parse_tro_seq_for_ui(str(used_trolley_id or ""), total_count=total_count)
        if base_seq is None:
            parsed_sequences = [
                parsed for trolley_id in self._all_visible_trolley_ids()
                if (parsed := self._parse_tro_seq_for_ui(trolley_id, total_count=total_count)) is not None
            ]
            if parsed_sequences:
                base_seq = max(parsed_sequences)
        
        next_seq = 1 if base_seq is None else (base_seq % total_count) + 1
        next_suggestion = self._format_tro_id_for_ui(next_seq, total_count=total_count)
        self._suggested_trolley_id = next_suggestion
        
        current_text = self._add_trolley_input.text().strip()
        if force or not current_text or current_text.upper() == previous_suggestion.upper():
            self._add_trolley_input.setText(next_suggestion)
    
    def _show_status(self, message: str, status_type: str = "info") -> None:
        """Show status message with appropriate styling"""
        icons = {
            "success": "✅",
            "warning": "⚠️",
            "danger": "❌",
            "info": "ℹ️",
        }
        colors = {
            "success": ThemeColors.SUCCESS,
            "warning": ThemeColors.WARNING,
            "danger": ThemeColors.DANGER,
            "info": ThemeColors.TEXT_SECONDARY,
        }
        icon = icons.get(status_type, "")
        color = colors.get(status_type, ThemeColors.TEXT_SECONDARY)
        
        display_msg = f"{icon} {message}" if icon and not message.startswith(icon) else message
        self._status.setText(display_msg)
        self._status.setStyleSheet(f"color: {color}; padding: 4px 8px;")
    
    def _clear_all_selections(self) -> None:
        """Clear all selections (trolley lists and tray table)"""
        self._clear_all_trolley_selections()
        self._clear_ungroup_selection()
    
    def _clear_all_trolley_selections(self) -> None:
        for lv in [self._assembly_card.trolley_list, self._queue_card.trolley_list, self._precharge_card.trolley_list]:
            with contextlib.suppress(Exception):
                lv.clearSelection()
    
    def _clear_ungroup_selection(self) -> None:
        tray_table = self._assembly_card.tray_table
        if tray_table is None:
            return
        if tray_table.selectionModel() is not None:
            tray_table.clearSelection()
        self._vm.assembly_ungrouped_model.clear_checked()
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ACTION EXECUTION
    # ═══════════════════════════════════════════════════════════════════════════
    
    def _run_action(self, *, action_name: str, action: Callable[[], Any], lock_ui: bool = True) -> None:
        if self._action_inflight:
            self._show_status("⏳ Another action is running...", "warning")
            return
        self._action_inflight = True
        self._action_locked_ui = bool(lock_ui)
        if self._action_locked_ui:
            self._set_controls_enabled(False)
        self._show_status(f"⏳ {action_name}...", "info")
        log.info("UI action started action=%s", action_name)
        
        def _worker() -> None:
            try:
                result = action()
                self._action_bridge.action_done.emit(action_name, result, None)
            except Exception as exc:
                self._action_bridge.action_done.emit(action_name, None, str(exc))
        
        threading.Thread(target=_worker, name=f"ui-action-{action_name.replace(' ', '-').lower()}", daemon=True).start()
    
    def _run_settings_action(self, *, action: Callable[[], Any]) -> None:
        action_name = "Apply settings"
        if self._settings_inflight:
            self._show_status("⏳ Settings update in progress...", "warning")
            return
        self._settings_inflight = True
        self._show_status(f"⏳ {action_name}...", "info")
        log.info("UI action started action=%s", action_name)
        
        def _worker() -> None:
            try:
                result = action()
                self._action_bridge.action_done.emit(action_name, result, None)
            except Exception as exc:
                self._action_bridge.action_done.emit(action_name, None, str(exc))
        
        threading.Thread(target=_worker, name="ui-action-apply-settings", daemon=True).start()
    
    def _on_window_status_timer(self) -> None:
        days = self._window_load_monitor_days
        if days is None:
            self._window_status_timer.stop()
            return
        if self._window_status_check_inflight:
            return
        self._window_status_check_inflight = True
        
        def _worker() -> None:
            try:
                result = self._runtime.data_window_loading_status(data_window_days=int(days))
                self._action_bridge.action_done.emit("Window status", result, None)
            except Exception as exc:
                self._action_bridge.action_done.emit("Window status", None, str(exc))
        
        threading.Thread(target=_worker, name="ui-window-status", daemon=True).start()
    
    def _start_window_load_monitor(self, *, days: int) -> None:
        self._window_load_monitor_days = int(days)
        self._window_status_check_inflight = False
        if not self._window_status_timer.isActive():
            self._window_status_timer.start()
    
    def _stop_window_load_monitor(self) -> None:
        self._window_load_monitor_days = None
        self._window_status_check_inflight = False
        if self._window_status_timer.isActive():
            self._window_status_timer.stop()
    
    def _on_action_done(self, action_name: str, result: object, error: object) -> None:
        # Handle window status polling
        if action_name == "Window status":
            self._window_status_check_inflight = False
            if error:
                log.warning("Window status poll failed error=%s", error)
                return
            payload = dict(result) if isinstance(result, dict) else {}
            if payload.get("ready"):
                days_value = int(payload.get("data_window_days") or self._window_load_monitor_days or 0)
                self._stop_window_load_monitor()
                self._show_status(f"✅ {days_value} day(s) data loaded", "success")
            return
        
        # Handle settings action
        if action_name == "Apply settings":
            self._settings_inflight = False
            if error:
                self._show_status(f"❌ Settings failed: {error}", "danger")
                self._pending_settings_context = None
                return
            
            payload = dict(result) if isinstance(result, dict) else {}
            context = self._pending_settings_context or {}
            self._pending_settings_context = None
            
            messages: list[str] = []
            if context.get("max_trays_changed"):
                messages.append("max trays")
            if context.get("total_trolley_changed"):
                messages.append("trolley count")
            if context.get("refresh_changed"):
                messages.append("refresh interval")
            if context.get("window_days_changed"):
                days = context.get("data_window_days", settings.ui_data_window_days)
                if payload.get("window_backfill_scheduled"):
                    self._show_status(f"⏳ Loading {days} day(s) data...", "info")
                    self._start_window_load_monitor(days=days)
                    return
                messages.append(f"{days} day window")
            
            msg = "✅ Settings applied"
            if messages:
                msg += f": {', '.join(messages)}"
            if self._pending_db_restart_notice:
                msg += " (restart for DB changes)"
                self._pending_db_restart_notice = False
            self._show_status(msg, "success")
            return
        
        # Handle regular actions
        self._action_inflight = False
        if self._action_locked_ui:
            self._set_controls_enabled(True)
        self._action_locked_ui = False
        
        # Handle search
        if action_name == "Search":
            query = getattr(self, "_pending_search_query", "")
            self._pending_search_query = ""
            if error:
                self._show_status(f"❌ Search failed: {error}", "danger")
                return
            owner = dict(result) if isinstance(result, dict) else None
            if owner is None:
                self._show_status(f"⚠️ No data found for '{query}'", "warning")
                return
            tray_id = str(owner.get("tray_id") or "").strip()
            cell_id = query
            location, trolley_id = self._find_tray_location(tray_id) if tray_id else ("Unknown", "")
            self._show_status(
                f"🔋 Cell {cell_id} | 📦 Tray {tray_id or '-'} | 🚗 {trolley_id or '-'} | 📍 {location}",
                "success"
            )
            return
        
        # Handle errors
        if error:
            error_text = str(error)
            if action_name == "Auto group":
                self._set_auto_group_checkbox_safely(self._auto_group_last_state)
                if "Viewer mode - use leader instance for actions" in error_text:
                    self._show_status("⚠️ Viewer mode: Auto group can only be changed on leader instance", "warning")
                    log.warning("UI action blocked in viewer mode action=%s", action_name)
                    return
            self._show_status(f"❌ {action_name} failed: {error}", "danger")
            log.error("UI action failed action=%s error=%s", action_name, error)
            return
        
        # Handle delete trolleys
        if action_name == "Delete trolleys":
            payload = dict(result) if isinstance(result, dict) else {}
            deleted = int(payload.get("deleted") or 0)
            requested = int(payload.get("requested") or 0)
            self._show_status(f"✅ Deleted {deleted}/{requested} trolleys", "success")
            return
        
        # Handle add trolley / add trays
        if action_name in {"Add trolley", "Add trays"}:
            self._vm.assembly_ungrouped_model.clear_checked()
            used_id = ""
            if isinstance(result, dict):
                used_id = str(result.get("trolley_id") or "").strip()
            self._refresh_suggested_trolley_id(force=True, used_trolley_id=used_id)
        
        # Handle auto group
        if action_name == "Auto group":
            self._auto_group_last_state = bool(self._auto_group_checkbox.isChecked())
            QTimer.singleShot(100, self._vm.force_refresh_summary)
        
        # Generic success
        self._show_status(f"✅ {action_name} completed", "success")
        log.info("UI action completed action=%s", action_name)
    
    def _set_controls_enabled(self, enabled: bool) -> None:
        allow_mutation = bool(enabled and not self._viewer_mode)
        self._quick_refresh_btn.setEnabled(allow_mutation)
        self._full_refresh_btn.setEnabled(allow_mutation)
        self._auto_group_checkbox.setEnabled(allow_mutation)
        self._settings_btn.setEnabled(allow_mutation)
        self._search_input.setEnabled(enabled)
        self._search_btn.setEnabled(enabled)
        self._add_trolley_input.setEnabled(allow_mutation)
        self._add_trolley_btn.setEnabled(allow_mutation)
        self._assembly_card.trolley_list.setEnabled(enabled)
        self._queue_card.trolley_list.setEnabled(enabled)
        self._precharge_card.trolley_list.setEnabled(enabled)
        if self._assembly_card.tray_table is not None:
            self._assembly_card.tray_table.setEnabled(enabled)
    
    # ═══════════════════════════════════════════════════════════════════════════
    # EVENT FILTER
    # ═══════════════════════════════════════════════════════════════════════════
    
    def eventFilter(self, watched: QObject, event: QEvent) -> bool:
        if event.type() == QEvent.Type.MouseButtonPress:
            tray_table = self._assembly_card.tray_table
            if tray_table is not None and tray_table.isVisible():
                global_pos_fn = getattr(event, "globalPosition", None)
                if callable(global_pos_fn):
                    global_point = global_pos_fn().toPoint()
                    target_widget = QApplication.widgetAt(global_point)
                    
                    # Widgets that should keep selection
                    keep_widgets = {
                        self._add_trolley_btn, self._add_trolley_input,
                        self._search_btn, self._search_input,
                        self._quick_refresh_btn, self._full_refresh_btn,
                        self._auto_group_checkbox, self._settings_btn,
                    }
                    
                    keep_selection = target_widget is not None and any(
                        target_widget is w or w.isAncestorOf(target_widget) for w in keep_widgets
                    )
                    
                    inside_table = target_widget is not None and (
                        target_widget is tray_table or tray_table.isAncestorOf(target_widget)
                    )
                    
                    trolley_lists = [
                        self._assembly_card.trolley_list,
                        self._queue_card.trolley_list,
                        self._precharge_card.trolley_list,
                    ]
                    inside_trolley = target_widget is not None and any(
                        target_widget is lv or lv.isAncestorOf(target_widget) for lv in trolley_lists
                    )
                    
                    if not inside_trolley:
                        self._clear_all_trolley_selections()
                    if not inside_table and not keep_selection:
                        self._clear_ungroup_selection()
        
        return super().eventFilter(watched, event)
    
    def closeEvent(self, event) -> None:
        self._stop_window_load_monitor()
        self._clock_timer.stop()
        app = QApplication.instance()
        if self._event_filter_installed and app is not None:
            app.removeEventFilter(self)
            self._event_filter_installed = False
        super().closeEvent(event)


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> int:
    _configure_logging()
    _install_crash_hooks()
    log.info("Application starting pid=%s python=%s", os.getpid(), sys.version.split()[0])
    
    # Check for modern ODBC driver
    from wip_management.infrastructure.sqlserver.connection import list_sql_server_drivers, _LEGACY_SQL_DRIVER_NAME
    drivers = list_sql_server_drivers()
    modern_drivers = [d for d in drivers if d.strip().lower() != _LEGACY_SQL_DRIVER_NAME]
    if not modern_drivers:
        log.warning(
            "=" * 70 + "\n"
            "WARNING: Only legacy 'SQL Server' ODBC driver detected!\n"
            "This driver has known thread-safety issues.\n"
            "The application will run in SERIALIZED mode (slower).\n"
            "For better performance, install:\n"
            "  'ODBC Driver 17 for SQL Server' or 'ODBC Driver 18 for SQL Server'\n"
            + "=" * 70
        )
    
    # Create application
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    
    # Set application-wide font
    font = QFont("Segoe UI", 10)
    app.setFont(font)
    
    # Create view model and bridge
    vm = BoardViewModel()
    bridge = _UiBridge()
    bridge.event_ready.connect(vm.on_event)
    
    # Create and start runtime
    runtime = _Runtime(bridge=bridge)
    runtime.start()
    
    # Create main window
    window = _MainWindow(vm=vm, runtime=runtime)
    window.show()
    
    try:
        exit_code = app.exec()
        log.info("Qt event loop exited code=%s", exit_code)
        return exit_code
    finally:
        runtime.stop()
        log.info("Application stopped")


if __name__ == "__main__":
    raise SystemExit(main())
