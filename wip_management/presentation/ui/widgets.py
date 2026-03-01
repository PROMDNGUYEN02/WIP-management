# wip_management/presentation/ui/widgets.py
"""
Enhanced UI Widgets for WIP Management
"""
from __future__ import annotations

import contextlib
from datetime import datetime, timedelta
from typing import Any, Callable

from PySide6.QtCore import (
    QAbstractAnimation, QEasingCurve, QModelIndex, QPoint, QPropertyAnimation,
    QRect, QRectF, QSize, Qt, QTimer, Signal
)
from PySide6.QtGui import (
    QBrush, QColor, QFont, QFontMetrics, QIcon, QLinearGradient,
    QPainter, QPainterPath, QPen
)
from PySide6.QtWidgets import (
    QAbstractItemView, QApplication, QFrame, QGraphicsDropShadowEffect,
    QHBoxLayout, QHeaderView, QLabel, QLineEdit, QListView, QMenu,
    QPushButton, QScrollArea, QSizePolicy, QSpacerItem, QStyledItemDelegate,
    QStyleOptionViewItem, QTableView, QVBoxLayout, QWidget
)

from wip_management.presentation.ui.theme import theme
from wip_management.presentation.ui.components import (
    AnimatedCard, IconButton, LoadingOverlay, MetricCard, SearchBar, StatusBadge
)
from wip_management.presentation.ui.notifications import toast_manager


class TrolleyItemDelegate(QStyledItemDelegate):
    """
    Custom delegate for trolley list items with:
    - Status indicator
    - Aging progress bar
    - Mode badge
    """
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
    
    def paint(
        self,
        painter: QPainter,
        option: QStyleOptionViewItem,
        index: QModelIndex,
    ) -> None:
        painter.save()
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        rect = option.rect
        is_selected = option.state & option.state.State_Selected
        is_hovered = option.state & option.state.State_MouseOver
        
        # Background
        if is_selected:
            bg_color = QColor(theme.palette.primary_light)
        elif is_hovered:
            bg_color = QColor(theme.palette.surface_hover)
        else:
            bg_color = QColor(theme.palette.surface)
        
        path = QPainterPath()
        path.addRect(QRectF(rect).adjusted(4, 2, -4, -2))
        painter.fillPath(path, bg_color)
        
        # Border
        border_color = QColor(theme.palette.primary) if is_selected else QColor(theme.palette.border_light)
        painter.setPen(QPen(border_color, 1))
        painter.drawPath(path)
        
        # Get data from model
        model = index.model()
        if model is None:
            painter.restore()
            return
        
        trolley_id = str(model.data(index, model.TrolleyIdRole) or "")
        tray_count = int(model.data(index, model.TrayCountRole) or 0)
        mode = str(model.data(index, model.ModeRole) or "auto")
        state = str(model.data(index, model.StateRole) or "")
        aging_state = str(model.data(index, model.AgingStateRole) or "")
        aging_time = str(model.data(index, model.AgingTimeRole) or "-")
        
        # Layout
        inner_rect = rect.adjusted(8, 2, -8, -2)
        
        # Trolley ID
        painter.setFont(QFont("Segoe UI", 11, QFont.Weight.DemiBold))
        painter.setPen(QColor(theme.palette.text_primary))
        painter.drawText(inner_rect, Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignTop, trolley_id)
        
        # State badge
        state_y = inner_rect.top()
        state_colors = {
            "Stacking": (theme.palette.info_bg, theme.palette.info),
            "Aging": (theme.palette.warning_bg, theme.palette.warning),
            "Waiting": (theme.palette.border_light, theme.palette.text_muted),
            "Completed": (theme.palette.success_bg, theme.palette.success),
        }
        if state in state_colors:
            bg, fg = state_colors[state]
            self._draw_badge(painter, inner_rect.right() - 70, state_y, state, bg, fg)
        
        # Bottom row: mode, tray count, aging
        bottom_y = inner_rect.bottom() - 12
        painter.setFont(QFont("Segoe UI", 9))
        painter.setPen(QColor(theme.palette.text_secondary))
        
        # Mode badge
        mode_bg = theme.palette.primary_light if mode == "manual" else theme.palette.border_light
        mode_fg = theme.palette.primary if mode == "manual" else theme.palette.text_muted
        self._draw_badge(painter, inner_rect.left(), bottom_y - 2, mode.upper(), mode_bg, mode_fg, small=True)
        
        # Tray count
        tray_text = f"{tray_count} trays"
        painter.drawText(
            QRect(inner_rect.left() + 70, bottom_y, 80, 16),
            Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
            tray_text
        )
        
        # Aging (for Queue column)
        if aging_time != "-":
            aging_colors = {
                "waiting": (theme.palette.warning_bg, theme.palette.warning),
                "ready": (theme.palette.success_bg, theme.palette.success),
                "exceed": (theme.palette.danger_bg, theme.palette.danger),
            }
            ag_bg, ag_fg = aging_colors.get(aging_state, (theme.palette.border_light, theme.palette.text_muted))
            self._draw_badge(painter, inner_rect.right() - 90, bottom_y - 2, f"⏱ {aging_time}", ag_bg, ag_fg, small=True)
        
        painter.restore()
    
    def _draw_badge(
        self,
        painter: QPainter,
        x: int,
        y: int,
        text: str,
        bg_color: str,
        fg_color: str,
        *,
        small: bool = False,
    ) -> None:
        fm = painter.fontMetrics()
        padding = 3 if small else 5
        height = 14 if small else 18
        width = fm.horizontalAdvance(text) + padding * 2
        
        rect = QRectF(x, y, width, height)
        path = QPainterPath()
        path.addRect(rect)
        painter.fillPath(path, QColor(bg_color))
        
        painter.setPen(QColor(fg_color))
        font = painter.font()
        font.setPointSize(8 if small else 9)
        font.setWeight(QFont.Weight.DemiBold)
        painter.setFont(font)
        painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, text)
    
    def sizeHint(
        self,
        option: QStyleOptionViewItem,
        index: QModelIndex,
    ) -> QSize:
        return QSize(option.rect.width(), 48)


class EnhancedColumnCard(QFrame):
    """
    Enhanced column card with:
    - Animated header
    - Rich statistics
    - Loading overlay
    - Context menu support
    """
    
    trolley_clicked = Signal(QModelIndex)
    trolley_double_clicked = Signal(QModelIndex)
    trolley_context_menu = Signal(QModelIndex, QPoint)
    
    def __init__(
        self,
        title: str,
        icon: str,
        color: str,
        trolley_model: Any,
        *,
        tray_model: Any | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("columnCard")
        self._title = title
        self._icon = icon
        self._color = color
        
        self._setup_ui(trolley_model, tray_model)
        self._setup_loading()
    
    def _setup_ui(self, trolley_model: Any, tray_model: Any | None) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        # Header
        header = QFrame()
        header.setStyleSheet(f"""
            QFrame {{
                background: qlineargradient(
                    x1:0, y1:0, x2:1, y2:0,
                    stop:0 {self._color}15,
                    stop:1 transparent
                );
                border-bottom: 1px solid {theme.palette.border};
                border-radius: 0px;
            }}
        """)
        header_layout = QHBoxLayout(header)
        header_layout.setContentsMargins(4, 2, 4, 2)
        
        # Icon + Title
        title_widget = QWidget()
        title_layout = QHBoxLayout(title_widget)
        title_layout.setContentsMargins(0, 0, 0, 0)
        title_layout.setSpacing(10)
        
        icon_label = QLabel(self._icon)
        icon_label.setFont(QFont("Segoe UI Emoji", 12))
        title_layout.addWidget(icon_label)
        
        title_label = QLabel(self._title)
        title_label.setObjectName("columnHeader")
        title_label.setStyleSheet(f"""
            font-size: 13px;
            font-weight: 600;
            color: {theme.palette.text_primary};
        """)
        title_layout.addWidget(title_label)
        
        header_layout.addWidget(title_widget)
        header_layout.addStretch()
        
        # Stats badge
        self._stats_badge = QLabel()
        self._stats_badge.setStyleSheet(f"""
            background: {theme.palette.surface};
            color: {theme.palette.text_secondary};
            padding: 1px 6px; border-radius: 0px; font-size: 10px;
            font-weight: 500;
        """)
        header_layout.addWidget(self._stats_badge)
        
        layout.addWidget(header)
        
        # Trolley list
        self.trolley_list = QListView()
        self.trolley_list.setModel(trolley_model)
        self.trolley_list.setItemDelegate(TrolleyItemDelegate(self))
        self.trolley_list.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerPixel)
        self.trolley_list.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.trolley_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        
        self.trolley_list.clicked.connect(self.trolley_clicked.emit)
        self.trolley_list.doubleClicked.connect(self.trolley_double_clicked.emit)
        self.trolley_list.customContextMenuRequested.connect(self._on_context_menu)
        
        layout.addWidget(self.trolley_list, 1)
        
        # Tray table (if provided)
        if tray_model is not None:
            self._setup_tray_section(tray_model, layout)
    
    def _setup_tray_section(self, tray_model: Any, parent_layout: QVBoxLayout) -> None:
        # Separator
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.HLine)
        separator.setStyleSheet(f"background: {theme.palette.border};")
        separator.setMaximumHeight(1)
        parent_layout.addWidget(separator)
        
        # Tray header
        tray_header = QFrame()
        tray_header.setStyleSheet(f"background: {theme.palette.surface_hover};")
        tray_header_layout = QHBoxLayout(tray_header)
        tray_header_layout.setContentsMargins(4, 2, 4, 2)
        
        tray_title = QLabel("📥 Ungrouped Trays")
        tray_title.setStyleSheet(f"""
            font-weight: 600;
            font-size: 13px;
            color: {theme.palette.text_primary};
        """)
        tray_header_layout.addWidget(tray_title)
        
        tray_header_layout.addStretch()
        
        # Action buttons
        self._actions_widget = QWidget()
        self._actions_layout = QHBoxLayout(self._actions_widget)
        self._actions_layout.setContentsMargins(0, 0, 0, 0)
        self._actions_layout.setSpacing(8)
        tray_header_layout.addWidget(self._actions_widget)
        
        parent_layout.addWidget(tray_header)
        
        # Tray table
        self.tray_table = QTableView()
        self.tray_table.setModel(tray_model)
        self.tray_table.setAlternatingRowColors(True)
        self.tray_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tray_table.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tray_table.verticalHeader().setVisible(False)
        self.tray_table.setShowGrid(False)
        
        header = self.tray_table.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(QHeaderView.ResizeMode.Interactive)
        
        parent_layout.addWidget(self.tray_table, 1)
    
    def _setup_loading(self) -> None:
        self._loading = LoadingOverlay(self)
    
    def _on_context_menu(self, pos: QPoint) -> None:
        index = self.trolley_list.indexAt(pos)
        if index.isValid():
            global_pos = self.trolley_list.viewport().mapToGlobal(pos)
            self.trolley_context_menu.emit(index, global_pos)
    
    def set_stats(
        self,
        trolley_count: int,
        tray_count: int,
        cell_count: int,
    ) -> None:
        self._stats_badge.setText(
            f"🚗 {trolley_count}  •  📦 {tray_count}  •  🔋 {cell_count:,}"
        )
    
    def set_loading(self, loading: bool, text: str = "Loading...") -> None:
        if loading:
            self._loading.start(text)
        else:
            self._loading.stop()
    
    def add_action_widget(self, widget: QWidget) -> None:
        if hasattr(self, '_actions_layout'):
            self._actions_layout.addWidget(widget)
    
    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        self._loading.setGeometry(self.rect())


class SummaryBar(QFrame):
    """
    Horizontal summary bar with metric cards
    """
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._cards: dict[str, MetricCard] = {}
        self._setup_ui()
    
    def _setup_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        
        # Define metrics
        metrics = [
            ("total_tray", "Total Trays", "📦", theme.palette.primary),
            ("grouped", "Grouped", "✓", theme.palette.success),
            ("ungrouped", "Ungrouped", "⏳", theme.palette.warning),
            ("assembly", "Assembly", "🏭", "#8b5cf6"),
            ("queue", "Queue", "📋", "#06b6d4"),
            ("precharge", "Precharge", "⚡", "#f59e0b"),
        ]
        
        for key, title, icon, color in metrics:
            card = MetricCard(title, icon, color, self)
            card.setSizePolicy(
                QSizePolicy.Policy.Expanding,
                QSizePolicy.Policy.Preferred
            )
            self._cards[key] = card
            layout.addWidget(card)
    
    def update_metrics(self, data: dict[str, int]) -> None:
        mapping = {
            "total_tray": "tray_count",
            "grouped": "group_count",
            "ungrouped": "assembly_ungroup_count",
            "assembly": "assembly_trolley_count",
            "queue": "queue_trolley_count",
            "precharge": "precharge_trolley_count",
        }
        
        for key, data_key in mapping.items():
            if key in self._cards and data_key in data:
                self._cards[key].set_value(int(data[data_key]))


class ToolBar(QFrame):
    """
    Modern toolbar with actions and search
    """
    
    quick_refresh = Signal()
    full_refresh = Signal()
    settings_clicked = Signal()
    auto_group_toggled = Signal(bool)
    search_triggered = Signal(str)
    theme_toggled = Signal()
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._setup_ui()
    
    def _setup_ui(self) -> None:
        self.setStyleSheet(f"""
            QFrame {{
                background: {theme.palette.surface};
                border: 1px solid {theme.palette.border};
                border-radius: 0px;
                padding: 0;
            }}
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        
        # Search
        self._search = SearchBar("Search Tray/Cell ID...")
        self._search.setFixedWidth(280)
        self._search.search_triggered.connect(self.search_triggered.emit)
        layout.addWidget(self._search)
        
        layout.addStretch()
        
        # Action buttons
        btn_style = f"""
            QPushButton {{
                background: {theme.palette.surface};
                color: {theme.palette.text_primary};
                border: 1px solid {theme.palette.border};
                border-radius: 0px;
                padding: 2px 8px;
                font-weight: 500;
            }}
            QPushButton:hover {{
                background: {theme.palette.surface_hover};
                border-color: {theme.palette.primary};
            }}
        """
        
        self._quick_refresh_btn = QPushButton("⟳ Quick Refresh")
        self._quick_refresh_btn.setStyleSheet(btn_style)
        self._quick_refresh_btn.clicked.connect(self.quick_refresh.emit)
        layout.addWidget(self._quick_refresh_btn)
        
        self._full_refresh_btn = QPushButton("↻ Full Refresh")
        self._full_refresh_btn.setStyleSheet(btn_style)
        self._full_refresh_btn.clicked.connect(self.full_refresh.emit)
        layout.addWidget(self._full_refresh_btn)
        
        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.VLine)
        sep.setStyleSheet(f"background: {theme.palette.border};")
        layout.addWidget(sep)
        
        # Auto group checkbox
        from PySide6.QtWidgets import QCheckBox
        self._auto_group = QCheckBox("Auto Group")
        self._auto_group.toggled.connect(self.auto_group_toggled.emit)
        layout.addWidget(self._auto_group)
        
        # Settings
        self._settings_btn = IconButton("⚙", "Settings")
        self._settings_btn.clicked.connect(self.settings_clicked.emit)
        layout.addWidget(self._settings_btn)
        
        # Theme toggle
        self._theme_btn = IconButton("🌙", "Toggle Theme")
        self._theme_btn.clicked.connect(self.theme_toggled.emit)
        layout.addWidget(self._theme_btn)
    
    def set_auto_group(self, enabled: bool) -> None:
        self._auto_group.setChecked(enabled)
    
    def set_enabled(self, enabled: bool) -> None:
        self._quick_refresh_btn.setEnabled(enabled)
        self._full_refresh_btn.setEnabled(enabled)
        self._auto_group.setEnabled(enabled)
        self._settings_btn.setEnabled(enabled)
        self._search._input.setEnabled(enabled)


class StatusBarWidget(QFrame):
    """Enhanced status bar with multiple sections"""
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self._setup_ui()
    
    def _setup_ui(self) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        
        # Status message
        self._message = QLabel("Ready")
        self._message.setStyleSheet(f"color: {theme.palette.text_secondary};")
        layout.addWidget(self._message, 1)
        
        # Connection status
        self._connection = StatusBadge("Connected", "success")
        layout.addWidget(self._connection)
        
        # Last update time
        self._last_update = QLabel("Last update: --:--:--")
        self._last_update.setStyleSheet(f"""
            color: {theme.palette.text_muted};
            font-size: 11px;
        """)
        layout.addWidget(self._last_update)
    
    def set_message(self, message: str) -> None:
        self._message.setText(message)
    
    def set_connection_status(self, connected: bool) -> None:
        if connected:
            self._connection.setText("Connected")
            self._connection.set_status("success")
        else:
            self._connection.setText("Disconnected")
            self._connection.set_status("danger")
    
    def set_last_update(self, dt: datetime | None = None) -> None:
        if dt is None:
            dt = datetime.now()
        self._last_update.setText(f"Last update: {dt.strftime('%H:%M:%S')}")
