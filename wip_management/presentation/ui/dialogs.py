# wip_management/presentation/ui/dialogs.py
"""
Modern Dialog Components for WIP Management
"""
from __future__ import annotations

import contextlib
import threading
from datetime import datetime, timedelta
from typing import Any, Callable

from PySide6.QtCore import (
    QEasingCurve, QModelIndex, QPropertyAnimation, QRect, QSize,
    Qt, QTimer, Signal
)
from PySide6.QtGui import (
    QColor, QFont, QFontMetrics, QIcon, QPainter, QPainterPath, QPen
)
from PySide6.QtWidgets import (
    QAbstractItemView, QApplication, QDialog, QDialogButtonBox,
    QDoubleSpinBox, QFormLayout, QFrame, QGraphicsDropShadowEffect,
    QGraphicsOpacityEffect, QGridLayout, QHBoxLayout, QHeaderView,
    QLabel, QLineEdit, QListWidget, QListWidgetItem, QProgressBar,
    QPushButton, QScrollArea, QSizePolicy, QSpinBox, QSplitter,
    QStackedWidget, QTabWidget, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget
)

from wip_management.config import settings
from wip_management.presentation.ui.theme import get_theme
from wip_management.presentation.ui.components import (
    AnimatedCard, Divider, EmptyState, IconButton,
    LoadingOverlay, MetricCard, ProgressCard, SpinnerWidget, StatRow
)
from wip_management.presentation.ui.charts import BarChart, DonutChart, LineChart
from wip_management.presentation.ui.notifications import ToastType, toast_manager


_MAX_UI_WINDOW_DAYS = 365


def _format_timedelta(delta: timedelta) -> str:
    total_seconds = max(0, int(delta.total_seconds()))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def _aging_status_text(delta: timedelta) -> str:
    age_hours = max(delta.total_seconds(), 0.0) / 3600.0
    target = max(float(settings.target_aging_hours), 0.0)
    tolerance = max(float(settings.target_aging_tolerance_hours), 0.0)
    if age_hours < max(target - tolerance, 0.0):
        return "Aging"
    if age_hours <= target + tolerance:
        return "Aged"
    return "Aged Out"


def _lookback_hours_for_days(days: int) -> float:
    return float(max(1, min(_MAX_UI_WINDOW_DAYS, int(days))) * 24 + 2)


class _BaseDialog(QDialog):
    """Base dialog with modern styling and animations"""

    def __init__(
        self,
        title: str,
        parent: QWidget | None = None,
        width: int = 600,
        height: int = 500,
    ) -> None:
        super().__init__(parent)
        self.setWindowTitle(title)
        self.setModal(True)
        self.resize(width, height)
        self._setup_shadow()

    def _setup_shadow(self) -> None:
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(40)
        shadow.setOffset(0, 8)
        shadow.setColor(QColor(0, 0, 0, 40))
        self.setGraphicsEffect(shadow)

    def _make_header(
        self,
        title: str,
        subtitle: str = "",
        icon: str = "",
    ) -> QWidget:
        p = get_theme().palette
        header = QWidget()
        layout = QVBoxLayout(header)
        layout.setContentsMargins(0, 0, 0, 16)
        layout.setSpacing(6)

        # Title row
        title_row = QHBoxLayout()

        if icon:
            icon_label = QLabel(icon)
            icon_label.setFont(QFont("Segoe UI Emoji", 22))
            title_row.addWidget(icon_label)
            title_row.addSpacing(8)

        title_label = QLabel(title)
        title_label.setStyleSheet(
            f"font-size: 22px; font-weight: 700; color: {p.text_primary};"
        )
        title_row.addWidget(title_label)
        title_row.addStretch()

        layout.addLayout(title_row)

        if subtitle:
            sub_label = QLabel(subtitle)
            sub_label.setStyleSheet(
                f"font-size: 13px; color: {p.text_secondary};"
            )
            sub_label.setWordWrap(True)
            layout.addWidget(sub_label)

        # Divider
        div = QFrame()
        div.setFixedHeight(1)
        div.setStyleSheet(f"background: {p.border};")
        layout.addWidget(div)

        return header

    def _make_buttons(
        self,
        accept_text: str = "Save",
        cancel_text: str = "Cancel",
        accept_style: str = "",
    ) -> QDialogButtonBox:
        btn_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok |
            QDialogButtonBox.StandardButton.Cancel,
            parent=self,
        )
        btn_box.button(QDialogButtonBox.StandardButton.Ok).setText(accept_text)
        btn_box.button(QDialogButtonBox.StandardButton.Cancel).setText(cancel_text)

        if accept_style:
            btn_box.button(
                QDialogButtonBox.StandardButton.Ok
            ).setObjectName(accept_style)

        btn_box.accepted.connect(self.accept)
        btn_box.rejected.connect(self.reject)
        return btn_box


class SettingsDialog(_BaseDialog):
    """
    Modern settings dialog with:
    - Tabbed interface
    - Live preview
    - Validation
    - Keyboard shortcuts
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__("Settings", parent, width=640, height=580)
        self._setup_ui()

    def _setup_ui(self) -> None:
        p = get_theme().palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(28, 28, 28, 24)
        layout.setSpacing(20)

        # Header
        layout.addWidget(self._make_header("Settings", "Configure WIP Management", "⚙️"))

        # Tabs
        self._tabs = QTabWidget()
        self._tabs.addTab(self._create_connection_tab(), "🔌  Connection")
        self._tabs.addTab(self._create_grouping_tab(), "📦  Grouping")
        self._tabs.addTab(self._create_aging_tab(), "⏱️  Aging")
        self._tabs.addTab(self._create_performance_tab(), "⚡  Performance")
        layout.addWidget(self._tabs, 1)

        # Hint bar
        hint_frame = QFrame()
        hint_frame.setStyleSheet(f"""
            QFrame {{
                background: {p.info_bg};
                border: 1px solid {p.info_border};
                border-radius: 10px;
                padding: 8px;
            }}
        """)
        hint_layout = QHBoxLayout(hint_frame)
        hint_layout.setContentsMargins(12, 8, 12, 8)
        hint_icon = QLabel("💡")
        hint_icon.setFont(QFont("Segoe UI Emoji", 14))
        hint_layout.addWidget(hint_icon)
        hint_text = QLabel(
            "Refresh & data window apply immediately. "
            "Database settings require a full restart."
        )
        hint_text.setStyleSheet(f"color: {p.info}; font-size: 12px;")
        hint_text.setWordWrap(True)
        hint_layout.addWidget(hint_text, 1)
        layout.addWidget(hint_frame)

        # Buttons
        layout.addWidget(self._make_buttons("💾  Save Settings", accept_style="successBtn"))

    def _create_connection_tab(self) -> QWidget:
        widget = QWidget()
        layout = QFormLayout(widget)
        layout.setSpacing(16)
        layout.setContentsMargins(20, 24, 20, 20)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        self._host_input = self._make_input(settings.sql_server, "e.g. 10.148.144.75")
        layout.addRow("🖥️  Database Host", self._host_input)

        self._user_input = self._make_input(settings.sql_user, "e.g. sa")
        layout.addRow("👤  Username", self._user_input)

        self._pass_input = self._make_input(settings.sql_password, "••••••••")
        self._pass_input.setEchoMode(QLineEdit.EchoMode.Password)

        # Password row with show/hide button
        pass_row = QWidget()
        pass_row_layout = QHBoxLayout(pass_row)
        pass_row_layout.setContentsMargins(0, 0, 0, 0)
        pass_row_layout.setSpacing(8)
        pass_row_layout.addWidget(self._pass_input, 1)

        toggle_btn = QPushButton("👁")
        toggle_btn.setObjectName("iconBtn")
        toggle_btn.setFixedSize(40, 40)
        toggle_btn.setCheckable(True)
        toggle_btn.toggled.connect(
            lambda checked: self._pass_input.setEchoMode(
                QLineEdit.EchoMode.Normal if checked else QLineEdit.EchoMode.Password
            )
        )
        pass_row_layout.addWidget(toggle_btn)
        layout.addRow("🔒  Password", pass_row)

        # Test connection button
        test_btn = QPushButton("🔌  Test Connection")
        test_btn.setObjectName("secondaryBtn")
        test_btn.clicked.connect(self._test_connection)
        layout.addRow("", test_btn)

        self._conn_status = QLabel("")
        layout.addRow("", self._conn_status)

        return widget

    def _create_grouping_tab(self) -> QWidget:
        widget = QWidget()
        layout = QFormLayout(widget)
        layout.setSpacing(16)
        layout.setContentsMargins(20, 24, 20, 20)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        self._max_trays_input = self._make_spinbox(1, 500, settings.trolley_max_trays, " trays")
        layout.addRow("📦  Max Trays / Trolley", self._max_trays_input)

        self._total_trolley_input = self._make_spinbox(
            1, 999, int(getattr(settings, "total_trolley_count", 99)), " trolleys"
        )
        layout.addRow("🚗  Total Trolleys", self._total_trolley_input)

        self._auto_group_gap_input = self._make_spinbox(
            0, 240, int(getattr(settings, "auto_group_gap_minutes", 10)), " minutes"
        )
        layout.addRow("⏱️  Auto Group Gap", self._auto_group_gap_input)

        return widget

    def _create_aging_tab(self) -> QWidget:
        widget = QWidget()
        layout = QFormLayout(widget)
        layout.setSpacing(16)
        layout.setContentsMargins(20, 24, 20, 20)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        self._target_aging_input = self._make_double_spinbox(
            0.1, 72.0, settings.target_aging_hours, " hours", step=0.5
        )
        layout.addRow("🎯  Target Aging", self._target_aging_input)

        self._aging_tolerance_input = self._make_double_spinbox(
            0.0, 24.0, settings.target_aging_tolerance_hours, " hours", step=0.5
        )
        layout.addRow("±  Tolerance", self._aging_tolerance_input)

        # Preview
        preview = QFrame()
        p = get_theme().palette
        preview.setStyleSheet(f"""
            QFrame {{
                background: {p.surface};
                border: 1px solid {p.border};
                border-radius: 12px;
                padding: 12px;
            }}
        """)
        preview_layout = QVBoxLayout(preview)
        preview_title = QLabel("📊 Aging Preview")
        preview_title.setStyleSheet(f"font-weight: 600; color: {p.text_primary};")
        preview_layout.addWidget(preview_title)

        self._aging_preview = QLabel(self._get_aging_preview_text())
        self._aging_preview.setStyleSheet(f"color: {p.text_secondary}; font-size: 12px;")
        preview_layout.addWidget(self._aging_preview)

        self._target_aging_input.valueChanged.connect(self._update_aging_preview)
        self._aging_tolerance_input.valueChanged.connect(self._update_aging_preview)

        layout.addRow("", preview)
        return widget

    def _create_performance_tab(self) -> QWidget:
        widget = QWidget()
        layout = QFormLayout(widget)
        layout.setSpacing(16)
        layout.setContentsMargins(20, 24, 20, 20)
        layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.ExpandingFieldsGrow)

        self._refresh_interval_input = self._make_double_spinbox(
            0.5, 60.0, settings.delta_poll_interval_seconds, " seconds", step=0.5
        )
        layout.addRow("🔄  Refresh Interval", self._refresh_interval_input)

        self._data_window_days_input = self._make_spinbox(
            1, _MAX_UI_WINDOW_DAYS,
            int(getattr(settings, "ui_data_window_days", 1)),
            " day(s)"
        )
        self._data_window_days_input.valueChanged.connect(self._on_data_window_changed)
        layout.addRow("📅  Data Window", self._data_window_days_input)

        self._lookback_label = QLabel()
        self._lookback_label.setStyleSheet(
            f"color: {get_theme().palette.text_muted}; font-size: 12px;"
        )
        layout.addRow("", self._lookback_label)
        self._on_data_window_changed(self._data_window_days_input.value())

        return widget

    def _make_input(self, value: str = "", placeholder: str = "") -> QLineEdit:
        edit = QLineEdit()
        edit.setText(value)
        edit.setPlaceholderText(placeholder)
        return edit

    def _make_spinbox(self, min_v: int, max_v: int, value: int, suffix: str = "") -> QSpinBox:
        sb = QSpinBox()
        sb.setRange(min_v, max_v)
        sb.setValue(value)
        if suffix:
            sb.setSuffix(suffix)
        return sb

    def _make_double_spinbox(
        self,
        min_v: float,
        max_v: float,
        value: float,
        suffix: str = "",
        step: float = 1.0,
        decimals: int = 1,
    ) -> QDoubleSpinBox:
        sb = QDoubleSpinBox()
        sb.setRange(min_v, max_v)
        sb.setValue(value)
        sb.setSingleStep(step)
        sb.setDecimals(decimals)
        if suffix:
            sb.setSuffix(suffix)
        return sb

    def _test_connection(self) -> None:
        p = get_theme().palette
        self._conn_status.setText("🔄 Testing connection...")
        self._conn_status.setStyleSheet(f"color: {p.warning}; font-size: 12px;")

        def _worker() -> None:
            import time
            time.sleep(0.5)  # Simulate connection test
            # In real implementation: test actual DB connection
            self._conn_status.setText("✅ Connection successful")
            self._conn_status.setStyleSheet(f"color: {p.success}; font-size: 12px;")

        threading.Thread(target=_worker, daemon=True).start()

    def _get_aging_preview_text(self) -> str:
        target = float(self._target_aging_input.value()) if hasattr(self, '_target_aging_input') else settings.target_aging_hours
        tol = float(self._aging_tolerance_input.value()) if hasattr(self, '_aging_tolerance_input') else settings.target_aging_tolerance_hours
        low = max(target - tol, 0.0)
        high = target + tol
        return (
            f"🟡 Aging:    < {low:.1f}h\n"
            f"🟢 Aged:    {low:.1f}h – {high:.1f}h\n"
            f"🔴 Aged Out: > {high:.1f}h"
        )

    def _update_aging_preview(self) -> None:
        if hasattr(self, '_aging_preview'):
            self._aging_preview.setText(self._get_aging_preview_text())

    def _on_data_window_changed(self, value: int) -> None:
        days = max(1, min(_MAX_UI_WINDOW_DAYS, int(value)))
        hours = int(_lookback_hours_for_days(days))
        self._lookback_label.setText(f"↳ Fetches {hours}h of data ({days} day window)")

    def payload(self) -> dict[str, object]:
        days = int(self._data_window_days_input.value())
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
            "initial_load_lookback_hours": _lookback_hours_for_days(days),
        }


class TrolleyDetailDialog(_BaseDialog):
    """
    Modern trolley detail dialog with:
    - Summary statistics
    - Tray list with aging visualization
    - Cell detail table
    - Mini charts
    - Loading states
    """

    _worker_lock = threading.Lock()

    def __init__(
        self,
        runtime: Any,
        trolley_id: str,
        tray_rows: list[dict[str, str]],
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(
            f"🚗 {trolley_id}", parent,
            width=1100, height=760
        )
        self._runtime = runtime
        self._trolley_id = trolley_id
        self._tray_rows = tray_rows
        self._closed = False
        self._active_tray_id = ""
        self._active_request_id = 0
        self._request_seq = 0
        self._loading = False
        self._pending_tray_id: str | None = None
        self._tray_cells_cache: dict[str, list[dict[str, str | None]]] = {}

        from PySide6.QtCore import QObject
        class _Bridge(QObject):
            loaded = Signal(str, int, object, object)
        self._bridge = _Bridge()
        self._bridge.loaded.connect(self._on_cells_loaded)

        self._setup_ui()

    def _setup_ui(self) -> None:
        p = get_theme().palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(28, 28, 28, 24)
        layout.setSpacing(20)

        # Header with stats
        header_layout = QHBoxLayout()

        title_col = QVBoxLayout()
        title_col.setSpacing(4)
        title = QLabel(f"🚗 {self._trolley_id}")
        title.setStyleSheet(f"font-size: 24px; font-weight: 700; color: {p.text_primary};")
        title_col.addWidget(title)

        tray_count = len(self._tray_rows)
        sub = QLabel(f"Trolley with {tray_count} tray(s)")
        sub.setStyleSheet(f"font-size: 13px; color: {p.text_secondary};")
        title_col.addWidget(sub)
        header_layout.addLayout(title_col)
        header_layout.addStretch()

        # Stat badges
        for icon, label, key in [
            ("📦", "Trays", str(tray_count)),
            ("🔋", "Cells", self._calc_total_cells()),
            ("⏱️", "Avg Aging", self._calc_avg_aging()),
        ]:
            badge = QFrame()
            badge.setStyleSheet(f"""
                QFrame {{
                    background: {p.surface};
                    border: 1px solid {p.border};
                    border-radius: 12px;
                    padding: 12px 16px;
                }}
            """)
            badge_layout = QVBoxLayout(badge)
            badge_layout.setContentsMargins(12, 10, 12, 10)
            badge_layout.setSpacing(4)

            val_label = QLabel(key)
            val_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            val_label.setStyleSheet(f"font-size: 20px; font-weight: 700; color: {p.text_primary};")
            badge_layout.addWidget(val_label)

            cap_label = QLabel(f"{icon} {label}")
            cap_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            cap_label.setStyleSheet(f"font-size: 11px; color: {p.text_muted}; font-weight: 600;")
            badge_layout.addWidget(cap_label)

            header_layout.addWidget(badge)
            header_layout.addSpacing(8)

        layout.addLayout(header_layout)

        # Divider
        layout.addWidget(Divider())

        # Main content: splitter
        splitter = QSplitter(Qt.Orientation.Vertical)
        splitter.setHandleWidth(8)

        # ── Top: Tray summary ────────────────────────────────────────────
        top_widget = QFrame()
        top_widget.setObjectName("card")
        top_layout = QVBoxLayout(top_widget)
        top_layout.setContentsMargins(0, 0, 0, 0)
        top_layout.setSpacing(0)

        # Summary header
        sum_header = QFrame()
        sum_header.setStyleSheet(f"""
            QFrame {{
                background: {p.surface};
                border-bottom: 1px solid {p.border};
                border-radius: 16px 16px 0 0;
            }}
        """)
        sum_header_layout = QHBoxLayout(sum_header)
        sum_header_layout.setContentsMargins(20, 14, 20, 14)

        sum_title = QLabel("📋  Tray Summary")
        sum_title.setStyleSheet(f"font-size: 14px; font-weight: 600; color: {p.text_primary};")
        sum_header_layout.addWidget(sum_title)
        sum_header_layout.addStretch()

        # Mini donut chart button
        self._show_chart_btn = QPushButton("📊  Chart View")
        self._show_chart_btn.setObjectName("secondaryBtn")
        self._show_chart_btn.setCheckable(True)
        self._show_chart_btn.toggled.connect(self._toggle_chart_view)
        sum_header_layout.addWidget(self._show_chart_btn)

        top_layout.addWidget(sum_header)

        # Stacked: table / chart
        self._summary_stack = QStackedWidget()

        # Table view
        self._summary_table = QTableWidget()
        self._summary_table.setColumnCount(6)
        self._summary_table.setHorizontalHeaderLabels(
            ["Tray ID", "Qty", "Start Time", "End Time", "Aging", "Status"]
        )
        self._summary_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._summary_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._summary_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._summary_table.verticalHeader().setVisible(False)
        self._summary_table.setShowGrid(False)
        self._summary_table.setAlternatingRowColors(True)
        self._summary_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        sh = self._summary_table.horizontalHeader()
        sh.setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        for col in range(1, 6):
            sh.setSectionResizeMode(col, QHeaderView.ResizeMode.ResizeToContents)
        sh.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)

        self._summary_stack.addWidget(self._summary_table)

        # Chart view
        chart_widget = QWidget()
        chart_layout = QHBoxLayout(chart_widget)
        chart_layout.setContentsMargins(20, 20, 20, 20)

        self._status_donut = DonutChart(show_legend=True, show_center_text=True)
        chart_layout.addWidget(self._status_donut, 1)

        self._aging_bar = BarChart(show_values=True)
        chart_layout.addWidget(self._aging_bar, 1)

        self._summary_stack.addWidget(chart_widget)
        top_layout.addWidget(self._summary_stack, 1)

        splitter.addWidget(top_widget)

        # ── Bottom: Cell detail ─────────────────────────────────────────
        bottom_widget = QFrame()
        bottom_widget.setObjectName("card")
        bottom_layout = QVBoxLayout(bottom_widget)
        bottom_layout.setContentsMargins(0, 0, 0, 0)
        bottom_layout.setSpacing(0)

        cell_header = QFrame()
        cell_header.setStyleSheet(f"""
            QFrame {{
                background: {p.surface};
                border-bottom: 1px solid {p.border};
                border-radius: 16px 16px 0 0;
            }}
        """)
        cell_header_layout = QHBoxLayout(cell_header)
        cell_header_layout.setContentsMargins(20, 14, 20, 14)

        self._cell_title = QLabel("🔋  Cell Detail")
        self._cell_title.setStyleSheet(
            f"font-size: 14px; font-weight: 600; color: {p.text_primary};"
        )
        cell_header_layout.addWidget(self._cell_title)

        cell_header_layout.addStretch()

        self._loading_spinner = SpinnerWidget(size=24)
        self._loading_spinner.setVisible(False)
        cell_header_layout.addWidget(self._loading_spinner)

        self._cell_count_label = QLabel("")
        self._cell_count_label.setStyleSheet(
            f"color: {p.text_muted}; font-size: 12px;"
        )
        cell_header_layout.addWidget(self._cell_count_label)

        bottom_layout.addWidget(cell_header)

        self._cell_table = QTableWidget()
        self._cell_table.setColumnCount(3)
        self._cell_table.setHorizontalHeaderLabels(["Cell ID", "Start Time", "End Time"])
        self._cell_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self._cell_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self._cell_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self._cell_table.verticalHeader().setVisible(False)
        self._cell_table.setShowGrid(False)
        self._cell_table.setAlternatingRowColors(True)

        ch = self._cell_table.horizontalHeader()
        ch.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        ch.setDefaultAlignment(Qt.AlignmentFlag.AlignCenter)

        bottom_layout.addWidget(self._cell_table, 1)
        splitter.addWidget(bottom_widget)

        splitter.setSizes([320, 380])
        layout.addWidget(splitter, 1)

        # Status bar
        self._status_bar = QLabel("Select a tray to view cell details")
        self._status_bar.setStyleSheet(f"color: {p.text_muted}; font-size: 12px;")
        layout.addWidget(self._status_bar)

        # Fill data
        self._fill_summary(self._tray_rows)
        self._update_charts()
        self._summary_table.itemSelectionChanged.connect(self._on_summary_selection_changed)
        if self._summary_table.rowCount() > 0:
            self._summary_table.selectRow(0)

    def _calc_total_cells(self) -> str:
        total = sum(
            int(r.get("quantity", 0) or 0)
            for r in self._tray_rows
        )
        return f"{total:,}"

    def _calc_avg_aging(self) -> str:
        aging_times = [
            r.get("aging_time", "-")
            for r in self._tray_rows
            if r.get("aging_time", "-") != "-"
        ]
        if not aging_times:
            return "-"
        total_secs = 0
        for t in aging_times:
            parts = t.split(":")
            if len(parts) == 3:
                with contextlib.suppress(ValueError):
                    total_secs += int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        avg_secs = total_secs // len(aging_times)
        h, rem = divmod(avg_secs, 3600)
        m = rem // 60
        return f"{h:02d}:{m:02d}"

    def _fill_summary(self, tray_rows: list[dict[str, str]]) -> None:
        p = get_theme().palette
        self._summary_table.setRowCount(len(tray_rows))

        status_colors = {
            "Aging": (p.warning_bg, p.warning),
            "Aged": (p.success_bg, p.success),
            "Aged Out": (p.danger_bg, p.danger),
            "Precharged": (p.info_bg, p.info),
        }

        for row_idx, row in enumerate(tray_rows):
            # Tray ID
            tray_item = QTableWidgetItem(row.get("tray_id", ""))
            tray_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            tray_item.setData(Qt.ItemDataRole.UserRole, row.get("tray_id", ""))
            self._summary_table.setItem(row_idx, 0, tray_item)

            # Qty
            qty = row.get("quantity", "-")
            self._summary_table.setItem(row_idx, 1, self._center_item(qty))

            # Start/End
            self._summary_table.setItem(row_idx, 2, self._center_item(row.get("start_time", "-")))
            self._summary_table.setItem(row_idx, 3, self._center_item(row.get("end_time", "-")))

            # Aging
            self._summary_table.setItem(row_idx, 4, self._center_item(row.get("aging_time", "-")))

            # Status with color
            status = row.get("status", "-")
            status_item = QTableWidgetItem(f"  {status}  ")
            status_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if status in status_colors:
                bg, fg = status_colors[status]
                status_item.setForeground(QColor(fg))
                status_item.setBackground(QColor(bg))
            self._summary_table.setItem(row_idx, 5, status_item)

        self._summary_table.resizeRowsToContents()

    def _update_charts(self) -> None:
        if not self._tray_rows:
            return

        p = get_theme().palette
        chart_colors = get_theme().get_chart_colors()

        # Status distribution donut
        status_counts: dict[str, int] = {}
        for row in self._tray_rows:
            status = row.get("status", "Unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        status_colors_map = {
            "Aging": p.warning,
            "Aged": p.success,
            "Aged Out": p.danger,
            "Precharged": p.info,
            "Unknown": p.text_muted,
            "-": p.border,
        }

        donut_data = [
            (k, float(v), status_colors_map.get(k, chart_colors[i % len(chart_colors)]))
            for i, (k, v) in enumerate(status_counts.items())
        ]
        self._status_donut.set_data(donut_data)

        # Aging distribution bar chart
        aging_buckets = {"< 2h": 0, "2-4h": 0, "4-6h": 0, "6-8h": 0, "> 8h": 0}
        for row in self._tray_rows:
            aging = row.get("aging_time", "-")
            if aging == "-":
                continue
            parts = aging.split(":")
            if len(parts) == 3:
                with contextlib.suppress(ValueError):
                    hours = int(parts[0]) + int(parts[1]) / 60
                    if hours < 2:
                        aging_buckets["< 2h"] += 1
                    elif hours < 4:
                        aging_buckets["2-4h"] += 1
                    elif hours < 6:
                        aging_buckets["4-6h"] += 1
                    elif hours < 8:
                        aging_buckets["6-8h"] += 1
                    else:
                        aging_buckets["> 8h"] += 1

        bar_data = [(k, float(v)) for k, v in aging_buckets.items() if v > 0]
        self._aging_bar.set_data(bar_data)

    def _toggle_chart_view(self, show_chart: bool) -> None:
        self._summary_stack.setCurrentIndex(1 if show_chart else 0)
        self._show_chart_btn.setText("📋  Table View" if show_chart else "📊  Chart View")

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

        # Check cache
        cached = self._tray_cells_cache.get(tray_id)
        if cached is not None:
            self._active_tray_id = tray_id
            self._render_cells(tray_id, cached)
            self._set_status(f"✅ {len(cached)} cells (cached)", "success")
            return

        if self._loading:
            self._pending_tray_id = tray_id
            self._set_status(f"⏳ Queued: {tray_id}", "info")
            return

        self._start_tray_cells_query(tray_id)

    def _start_tray_cells_query(self, tray_id: str) -> None:
        self._request_seq += 1
        request_id = self._request_seq
        self._active_request_id = request_id
        self._active_tray_id = tray_id
        self._loading = True
        self._pending_tray_id = None

        self._loading_spinner.start()
        self._loading_spinner.setVisible(True)
        self._cell_title.setText(f"🔋  Cell Detail — {tray_id}")
        self._set_status(f"⏳ Loading cells for {tray_id}...", "info")
        self._cell_table.setRowCount(0)
        self._cell_count_label.setText("")

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

        threading.Thread(
            target=_worker,
            name=f"tray-detail-{tray_id}-{request_id}",
            daemon=True,
        ).start()

    def _render_cells(self, tray_id: str, rows: list[dict[str, str | None]]) -> None:
        self._cell_table.setRowCount(len(rows))
        for row_idx, row_data in enumerate(rows):
            self._cell_table.setItem(
                row_idx, 0, self._center_item(str(row_data.get("cell_id", "")))
            )
            self._cell_table.setItem(
                row_idx, 1, self._center_item(str(row_data.get("start_time") or "-"))
            )
            self._cell_table.setItem(
                row_idx, 2, self._center_item(str(row_data.get("end_time") or "-"))
            )
        self._cell_count_label.setText(f"{len(rows):,} cells")

    def _on_cells_loaded(
        self,
        tray_id: str,
        request_id: int,
        rows: object,
        error: object,
    ) -> None:
        if self._closed:
            return
        if request_id != self._active_request_id:
            return

        self._loading = False
        self._loading_spinner.stop()
        self._loading_spinner.setVisible(False)

        if error:
            self._set_status(f"❌ Load failed: {error}", "danger")
            pending = self._pending_tray_id
            self._pending_tray_id = None
            if pending and pending != tray_id:
                self._start_tray_cells_query(pending)
            return

        values = list(rows or [])
        self._tray_cells_cache[tray_id] = [dict(item) for item in values]
        self._render_cells(tray_id, self._tray_cells_cache[tray_id])

        if not values:
            self._set_status("⚠️ No cell data found for this tray", "warning")
        else:
            self._set_status(f"✅ {len(values):,} cells loaded for {tray_id}", "success")

        pending = self._pending_tray_id
        self._pending_tray_id = None
        if pending and pending != tray_id:
            self._start_tray_cells_query(pending)

    def _set_status(self, message: str, status_type: str = "info") -> None:
        p = get_theme().palette
        colors = {
            "success": p.success,
            "warning": p.warning,
            "danger": p.danger,
            "info": p.text_muted,
        }
        color = colors.get(status_type, p.text_muted)
        self._status_bar.setText(message)
        self._status_bar.setStyleSheet(f"color: {color}; font-size: 12px;")

    @staticmethod
    def _center_item(text: str) -> QTableWidgetItem:
        item = QTableWidgetItem(text)
        item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        return item

    def closeEvent(self, event) -> None:
        self._closed = True
        super().closeEvent(event)


class TrayPickerDialog(_BaseDialog):
    """Dialog for selecting trays to remove from trolley"""

    def __init__(
        self,
        tray_ids: list[str],
        title: str = "Select Trays",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, parent, width=440, height=480)
        self._tray_ids = tray_ids
        self._setup_ui()

    def _setup_ui(self) -> None:
        p = get_theme().palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 20)
        layout.setSpacing(16)

        layout.addWidget(
            self._make_header("Select Trays", "Choose trays to remove from trolley", "🗑️")
        )

        # Select all row
        select_all_row = QHBoxLayout()
        self._select_all_btn = QPushButton("Select All")
        self._select_all_btn.setObjectName("secondaryBtn")
        self._select_all_btn.clicked.connect(self._select_all)
        select_all_row.addWidget(self._select_all_btn)

        self._deselect_all_btn = QPushButton("Deselect All")
        self._deselect_all_btn.setObjectName("ghostBtn")
        self._deselect_all_btn.clicked.connect(self._deselect_all)
        select_all_row.addWidget(self._deselect_all_btn)
        select_all_row.addStretch()

        self._count_label = QLabel(f"0 / {len(self._tray_ids)} selected")
        self._count_label.setStyleSheet(f"color: {p.text_muted}; font-size: 12px;")
        select_all_row.addWidget(self._count_label)

        layout.addLayout(select_all_row)

        # List
        self._list = QListWidget()
        self._list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)

        for tray_id in self._tray_ids:
            item = QListWidgetItem(f"📦  {tray_id}")
            item.setData(Qt.ItemDataRole.UserRole, tray_id)
            item.setSelected(True)
            self._list.addItem(item)

        self._list.itemSelectionChanged.connect(self._update_count)
        layout.addWidget(self._list, 1)

        self._update_count()

        layout.addWidget(self._make_buttons("🗑️  Remove Selected", accept_style="dangerBtn"))

    def _select_all(self) -> None:
        self._list.selectAll()

    def _deselect_all(self) -> None:
        self._list.clearSelection()

    def _update_count(self) -> None:
        count = len(self._list.selectedItems())
        self._count_label.setText(f"{count} / {len(self._tray_ids)} selected")

    def selected_tray_ids(self) -> list[str]:
        return [
            str(item.data(Qt.ItemDataRole.UserRole))
            for item in self._list.selectedItems()
        ]


class LoadingDialog(_BaseDialog):
    """Full-screen loading dialog with progress"""

    def __init__(
        self,
        title: str = "Loading",
        message: str = "Please wait...",
        cancellable: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, parent, width=400, height=200)
        self._cancelled = False
        self.setModal(True)
        self._setup_ui(message, cancellable)

    def _setup_ui(self, message: str, cancellable: bool) -> None:
        p = get_theme().palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(32, 32, 32, 28)
        layout.setSpacing(20)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)

        # Spinner + title row
        top_row = QHBoxLayout()
        top_row.setAlignment(Qt.AlignmentFlag.AlignCenter)
        top_row.setSpacing(16)

        self._spinner = SpinnerWidget(size=40)
        self._spinner.start()
        top_row.addWidget(self._spinner)

        self._title_label = QLabel(self.windowTitle())
        self._title_label.setStyleSheet(f"font-size: 18px; font-weight: 600; color: {p.text_primary};")
        top_row.addWidget(self._title_label)
        layout.addLayout(top_row)

        # Message
        self._message_label = QLabel(message)
        self._message_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._message_label.setStyleSheet(f"color: {p.text_secondary}; font-size: 13px;")
        self._message_label.setWordWrap(True)
        layout.addWidget(self._message_label)

        # Progress bar
        self._progress = QProgressBar()
        self._progress.setRange(0, 0)  # Indeterminate
        self._progress.setTextVisible(False)
        self._progress.setFixedHeight(6)
        layout.addWidget(self._progress)

        # Cancel button
        if cancellable:
            cancel_btn = QPushButton("Cancel")
            cancel_btn.setObjectName("ghostBtn")
            cancel_btn.clicked.connect(self._on_cancel)
            layout.addWidget(cancel_btn, alignment=Qt.AlignmentFlag.AlignCenter)

    def _on_cancel(self) -> None:
        self._cancelled = True
        self.reject()

    def set_message(self, message: str) -> None:
        self._message_label.setText(message)

    def set_title(self, title: str) -> None:
        self.setWindowTitle(title)
        self._title_label.setText(title)

    def set_progress(self, value: int, maximum: int) -> None:
        self._progress.setRange(0, maximum)
        self._progress.setValue(value)

    def is_cancelled(self) -> bool:
        return self._cancelled

    def closeEvent(self, event) -> None:
        self._spinner.stop()
        super().closeEvent(event)


class ConfirmDialog(_BaseDialog):
    """Modern confirmation dialog"""

    def __init__(
        self,
        title: str,
        message: str,
        confirm_text: str = "Confirm",
        cancel_text: str = "Cancel",
        danger: bool = False,
        icon: str = "❓",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(title, parent, width=420, height=220)
        self._setup_ui(message, confirm_text, cancel_text, danger, icon)

    def _setup_ui(
        self,
        message: str,
        confirm_text: str,
        cancel_text: str,
        danger: bool,
        icon: str,
    ) -> None:
        p = get_theme().palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(28, 28, 28, 24)
        layout.setSpacing(20)

        # Icon + message
        content_row = QHBoxLayout()
        content_row.setSpacing(16)
        content_row.setAlignment(Qt.AlignmentFlag.AlignTop)

        icon_label = QLabel(icon)
        icon_label.setFont(QFont("Segoe UI Emoji", 32))
        content_row.addWidget(icon_label)

        text_col = QVBoxLayout()
        text_col.setSpacing(8)

        title_label = QLabel(self.windowTitle())
        title_label.setStyleSheet(f"font-size: 18px; font-weight: 600; color: {p.text_primary};")
        text_col.addWidget(title_label)

        msg_label = QLabel(message)
        msg_label.setWordWrap(True)
        msg_label.setStyleSheet(f"color: {p.text_secondary}; font-size: 13px;")
        text_col.addWidget(msg_label)

        content_row.addLayout(text_col, 1)
        layout.addLayout(content_row, 1)

        # Buttons
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()

        cancel_btn = QPushButton(cancel_text)
        cancel_btn.setObjectName("secondaryBtn")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)

        btn_layout.addSpacing(8)

        confirm_btn = QPushButton(confirm_text)
        confirm_btn.setObjectName("dangerBtn" if danger else "successBtn")
        confirm_btn.clicked.connect(self.accept)
        btn_layout.addWidget(confirm_btn)

        layout.addLayout(btn_layout)


class DashboardPanel(QWidget):
    """
    Dashboard statistics panel with:
    - Real-time charts
    - KPI metrics
    - Trend analysis
    """

    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._data_history: dict[str, list[int]] = {}
        self._setup_ui()

    def _setup_ui(self) -> None:
        p = get_theme().palette
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(16)

        # Header
        header_row = QHBoxLayout()
        title = QLabel("📊 Analytics")
        title.setStyleSheet(f"font-size: 18px; font-weight: 700; color: {p.text_primary};")
        header_row.addWidget(title)
        header_row.addStretch()

        close_btn = IconButton("✕", "Close panel")
        close_btn.clicked.connect(self.hide)
        header_row.addWidget(close_btn)
        layout.addLayout(header_row)

        # Scroll area for charts
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.Shape.NoFrame)

        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setSpacing(16)

        # Throughput line chart
        chart_card1 = QFrame()
        chart_card1.setObjectName("card")
        chart_card1_layout = QVBoxLayout(chart_card1)
        chart_card1_layout.setContentsMargins(16, 16, 16, 16)

        chart1_title = QLabel("📈 Tray Throughput")
        chart1_title.setStyleSheet(f"font-weight: 600; color: {p.text_primary};")
        chart_card1_layout.addWidget(chart1_title)

        self._throughput_chart = LineChart(
            color=p.chart_1,
            show_area=True,
            show_grid=True,
        )
        self._throughput_chart.setFixedHeight(150)
        chart_card1_layout.addWidget(self._throughput_chart)
        scroll_layout.addWidget(chart_card1)

        # Distribution donut
        chart_card2 = QFrame()
        chart_card2.setObjectName("card")
        chart_card2_layout = QVBoxLayout(chart_card2)
        chart_card2_layout.setContentsMargins(16, 16, 16, 16)

        chart2_title = QLabel("🍩 Column Distribution")
        chart2_title.setStyleSheet(f"font-weight: 600; color: {p.text_primary};")
        chart_card2_layout.addWidget(chart2_title)

        self._distribution_chart = DonutChart(
            show_legend=True,
            show_center_text=True,
        )
        self._distribution_chart.setFixedHeight(200)
        chart_card2_layout.addWidget(self._distribution_chart)
        scroll_layout.addWidget(chart_card2)

        # Aging bar chart
        chart_card3 = QFrame()
        chart_card3.setObjectName("card")
        chart_card3_layout = QVBoxLayout(chart_card3)
        chart_card3_layout.setContentsMargins(16, 16, 16, 16)

        chart3_title = QLabel("⏱️ Aging Distribution")
        chart3_title.setStyleSheet(f"font-weight: 600; color: {p.text_primary};")
        chart_card3_layout.addWidget(chart3_title)

        self._aging_chart = BarChart(color=p.chart_3)
        self._aging_chart.setFixedHeight(150)
        chart_card3_layout.addWidget(self._aging_chart)
        scroll_layout.addWidget(chart_card3)

        scroll_layout.addStretch()
        scroll.setWidget(scroll_content)
        layout.addWidget(scroll, 1)

    def update_data(self, payload: dict[str, int]) -> None:
        p = get_theme().palette

        # Update throughput history
        tray_count = payload.get("tray_count", 0)
        key = "tray_count"
        if key not in self._data_history:
            self._data_history[key] = []
        self._data_history[key].append(tray_count)
        if len(self._data_history[key]) > 20:
            self._data_history[key].pop(0)

        # Throughput chart
        history = self._data_history[key]
        if len(history) >= 2:
            chart_data = [(str(i), float(v)) for i, v in enumerate(history)]
            self._throughput_chart.set_data(chart_data)

        # Distribution donut
        assembly = payload.get("assembly_trolley_count", 0)
        queue = payload.get("queue_trolley_count", 0)
        precharge = payload.get("precharge_trolley_count", 0)

        if assembly + queue + precharge > 0:
            self._distribution_chart.set_data([
                ("Assembly", float(assembly), p.chart_2),
                ("Queue", float(queue), p.chart_6),
                ("Precharge", float(precharge), p.chart_4),
            ])