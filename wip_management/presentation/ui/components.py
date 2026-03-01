# wip_management/presentation/ui/components.py
"""
Reusable UI Components
"""
from __future__ import annotations

import contextlib
import logging
from datetime import datetime, timedelta
from typing import Any, Callable

from PySide6.QtCore import (
    Property, QEasingCurve, QParallelAnimationGroup, QPoint, QPointF, QPropertyAnimation,
    QRect, QRectF, QSequentialAnimationGroup, QSize, Qt, QTimer, Signal
)
from PySide6.QtGui import (
    QBrush, QColor, QConicalGradient, QFont, QFontMetrics, QLinearGradient,
    QPainter, QPainterPath, QPen, QRadialGradient
)
from PySide6.QtWidgets import (
    QFrame, QGraphicsDropShadowEffect, QGraphicsOpacityEffect,
    QHBoxLayout, QLabel, QLineEdit, QProgressBar, QPushButton,
    QSizePolicy, QSpacerItem, QStackedWidget, QVBoxLayout, QWidget
)

from wip_management.presentation.ui.theme import get_theme

log = logging.getLogger(__name__)


class AnimatedCard(QFrame):
    """Base card with hover animations and shadow"""
    
    clicked = Signal()
    double_clicked = Signal()
    
    def __init__(self, parent: QWidget | None = None, clickable: bool = True) -> None:
        super().__init__(parent)
        self.setObjectName("card")
        self._clickable = clickable
        self._hovered = False
        
        if clickable:
            self.setCursor(Qt.CursorShape.PointingHandCursor)
        
        self._setup_shadow()
    
    def _setup_shadow(self) -> None:
        self._shadow = QGraphicsDropShadowEffect(self)
        self._shadow.setBlurRadius(20)
        self._shadow.setOffset(0, 4)
        self._shadow.setColor(QColor(0, 0, 0, 20))
        self.setGraphicsEffect(self._shadow)
    
    def enterEvent(self, event) -> None:
        self._hovered = True
        self._shadow.setBlurRadius(30)
        self._shadow.setOffset(0, 8)
        self._shadow.setColor(QColor(0, 0, 0, 30))
        super().enterEvent(event)
    
    def leaveEvent(self, event) -> None:
        self._hovered = False
        self._shadow.setBlurRadius(20)
        self._shadow.setOffset(0, 4)
        self._shadow.setColor(QColor(0, 0, 0, 20))
        super().leaveEvent(event)
    
    def mousePressEvent(self, event) -> None:
        if self._clickable and event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)
    
    def mouseDoubleClickEvent(self, event) -> None:
        if self._clickable and event.button() == Qt.MouseButton.LeftButton:
            self.double_clicked.emit()
        super().mouseDoubleClickEvent(event)


class MetricCard(AnimatedCard):
    """
    Modern metric card with:
    - Animated value transitions
    - Trend indicators
    - Mini sparkline chart
    - Customizable colors
    """
    
    def __init__(
        self,
        title: str,
        icon: str = "📊",
        color: str | None = None,
        show_trend: bool = True,
        show_sparkline: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent, clickable=True)
        self.setObjectName("metricCard")
        self.setMinimumHeight(104)
        self._color = color or get_theme().palette.primary
        self._current_value = 0
        self._target_value = 0
        self._previous_value = 0
        self._show_trend = show_trend
        self._show_sparkline = show_sparkline
        self._history: list[int] = []
        self._max_history = 20
        
        self._setup_ui(title, icon)
        self._setup_animation()
    
    def _setup_ui(self, title: str, icon: str) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(12, 10, 12, 10)
        layout.setSpacing(4)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Top row: icon + trend
        top_row = QHBoxLayout()
        top_row.setContentsMargins(0, 0, 0, 0)
        top_row.setSpacing(6)
        top_row.addStretch(1)
        
        # Icon with colored background
        icon_container = QFrame()
        icon_container.setFixedSize(34, 34)
        icon_container.setStyleSheet(f"""
            background: {self._color}15;
            border-radius: 10px;
        """)
        icon_layout = QVBoxLayout(icon_container)
        icon_layout.setContentsMargins(0, 0, 0, 0)
        icon_layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self._icon = QLabel(icon)
        self._icon.setFont(QFont("Segoe UI Emoji", 15))
        self._icon.setAlignment(Qt.AlignmentFlag.AlignCenter)
        icon_layout.addWidget(self._icon)

        top_row.addWidget(icon_container)

        # Trend indicator
        if self._show_trend:
            self._trend = QLabel("")
            self._trend.setObjectName("metricTrend")
            self._trend.setVisible(False)
            top_row.addWidget(self._trend)
        top_row.addStretch(1)
        
        layout.addLayout(top_row)
        
        # Value
        self._value_label = QLabel("0")
        self._value_label.setObjectName("metricValue")
        self._value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._value_label)
        
        # Title
        self._title_label = QLabel(title)
        self._title_label.setObjectName("metricTitle")
        self._title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._title_label)
        
        # Sparkline (if enabled)
        if self._show_sparkline:
            self._sparkline = SparklineWidget(color=self._color)
            self._sparkline.setFixedHeight(20)
            layout.addWidget(self._sparkline)
        
        # Subtitle (optional)
        self._subtitle = QLabel("")
        self._subtitle.setObjectName("metricSubtitle")
        self._subtitle.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._subtitle.setVisible(False)
        layout.addWidget(self._subtitle)
    
    def _setup_animation(self) -> None:
        self._anim_timer = QTimer(self)
        self._anim_timer.setInterval(16)  # ~60fps
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
        self._previous_value = self._target_value
        self._target_value = value
        
        # Update history
        self._history.append(value)
        if len(self._history) > self._max_history:
            self._history.pop(0)
        
        # Update sparkline
        if self._show_sparkline and hasattr(self, '_sparkline'):
            self._sparkline.set_data(self._history)
        
        if animate and abs(value - self._current_value) > 0:
            self._anim_timer.start()
        else:
            self._current_value = value
            self._value_label.setText(f"{value:,}")
        
        # Update trend
        if self._show_trend and self._previous_value > 0:
            diff = value - self._previous_value
            if diff > 0:
                self._trend.setText(f"↑ +{diff:,}")
                self._trend.setObjectName("metricTrendUp")
                self._trend.setVisible(True)
            elif diff < 0:
                self._trend.setText(f"↓ {diff:,}")
                self._trend.setObjectName("metricTrendDown")
                self._trend.setVisible(True)
            else:
                self._trend.setVisible(False)
            self._trend.style().unpolish(self._trend)
            self._trend.style().polish(self._trend)
    
    def set_subtitle(self, text: str) -> None:
        self._subtitle.setText(text)
        self._subtitle.setVisible(bool(text))
    
    def set_icon(self, icon: str) -> None:
        self._icon.setText(icon)
    
    def get_value(self) -> int:
        return self._target_value


class SparklineWidget(QWidget):
    """Mini sparkline chart for trend visualization"""
    
    def __init__(
        self,
        color: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._color = QColor(color) if color else QColor(get_theme().palette.primary)
        self._data: list[int] = []
        self.setMinimumHeight(20)
    
    def set_data(self, data: list[int]) -> None:
        self._data = list(data)
        self.update()
    
    def paintEvent(self, event) -> None:
        if len(self._data) < 2:
            return
        
        painter = QPainter(self)
        try:
            painter.setRenderHint(QPainter.RenderHint.Antialiasing)
            
            rect = self.rect()
            width = rect.width()
            height = rect.height()
            
            min_val = min(self._data)
            max_val = max(self._data)
            value_range = max(max_val - min_val, 1)
            
            # Calculate points
            points = []
            step = width / (len(self._data) - 1)
            for i, val in enumerate(self._data):
                x = i * step
                y = height - ((val - min_val) / value_range) * (height - 4) - 2
                points.append(QPointF(x, y))
            
            # Draw gradient fill
            gradient = QLinearGradient(0, 0, 0, height)
            gradient.setColorAt(0, QColor(self._color.red(), self._color.green(), self._color.blue(), 50))
            gradient.setColorAt(1, QColor(self._color.red(), self._color.green(), self._color.blue(), 0))
            
            fill_path = QPainterPath()
            fill_path.moveTo(0, height)
            for p in points:
                fill_path.lineTo(p)
            fill_path.lineTo(width, height)
            fill_path.closeSubpath()
            
            painter.fillPath(fill_path, gradient)
            
            # Draw line
            pen = QPen(self._color)
            pen.setWidth(2)
            pen.setCapStyle(Qt.PenCapStyle.RoundCap)
            pen.setJoinStyle(Qt.PenJoinStyle.RoundJoin)
            painter.setPen(pen)
            
            path = QPainterPath()
            path.moveTo(points[0])
            for p in points[1:]:
                path.lineTo(p)
            
            painter.drawPath(path)
            
            # Draw end point
            if points:
                last_point = points[-1]
                painter.setBrush(self._color)
                painter.setPen(Qt.PenStyle.NoPen)
                painter.drawEllipse(last_point, 4, 4)
        except Exception:
            # Never allow paintEvent exceptions to propagate into Qt's paint loop.
            log.exception("SparklineWidget paintEvent failed")
        finally:
            painter.end()


class SpinnerWidget(QWidget):
    """Animated loading spinner"""
    
    def __init__(
        self,
        size: int = 32,
        color: str | None = None,
        thickness: int = 3,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._size = size
        self._color = QColor(color) if color else QColor(get_theme().palette.primary)
        self._thickness = thickness
        self._angle = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._rotate)
        
        self.setFixedSize(size, size)
    
    def start(self) -> None:
        self._timer.start(16)
    
    def stop(self) -> None:
        self._timer.stop()
    
    def is_spinning(self) -> bool:
        return self._timer.isActive()
    
    def _rotate(self) -> None:
        self._angle = (self._angle + 8) % 360
        self.update()
    
    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Create gradient
        gradient = QConicalGradient(
            self._size / 2,
            self._size / 2,
            self._angle
        )
        gradient.setColorAt(0, self._color)
        gradient.setColorAt(0.7, QColor(self._color.red(), self._color.green(), self._color.blue(), 50))
        gradient.setColorAt(1, Qt.GlobalColor.transparent)
        
        pen = QPen(QBrush(gradient), self._thickness)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(pen)
        
        margin = self._thickness + 2
        rect = QRectF(margin, margin, self._size - 2*margin, self._size - 2*margin)
        painter.drawArc(rect, 0, 270 * 16)


class LoadingOverlay(QFrame):
    """Semi-transparent loading overlay"""
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("loadingOverlay")
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)
        self.hide()
        
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.setSpacing(16)
        
        # Spinner
        self._spinner = SpinnerWidget(size=48)
        layout.addWidget(self._spinner, alignment=Qt.AlignmentFlag.AlignCenter)
        
        # Text
        self._text = QLabel("Loading...")
        self._text.setObjectName("loadingText")
        self._text.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(self._text)
    
    def set_text(self, text: str) -> None:
        self._text.setText(text)
    
    def start(self, text: str = "Loading...") -> None:
        self.set_text(text)
        self._spinner.start()
        self.show()
        self.raise_()
    
    def stop(self) -> None:
        self._spinner.stop()
        self.hide()
    
    def resizeEvent(self, event) -> None:
        super().resizeEvent(event)
        if self.parent():
            self.setGeometry(self.parent().rect())


class SearchBar(QWidget):
    """Modern search bar with icon and clear button"""
    
    search_triggered = Signal(str)
    text_changed = Signal(str)
    
    def __init__(
        self,
        placeholder: str = "Search...",
        show_button: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._setup_ui(placeholder, show_button)
    
    def _setup_ui(self, placeholder: str, show_button: bool) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        p = get_theme().palette
        
        # Container
        container = QFrame()
        container.setStyleSheet(f"""
            QFrame {{
                background: {p.surface};
                border: 1px solid {p.border};
                border-radius: 12px;
            }}
            QFrame:focus-within {{
                border-color: {p.primary};
                border-width: 2px;
            }}
        """)
        
        inner_layout = QHBoxLayout(container)
        inner_layout.setContentsMargins(14, 0, 6, 0)
        inner_layout.setSpacing(10)
        
        # Search icon
        icon = QLabel("🔍")
        icon.setStyleSheet(f"color: {p.text_muted}; font-size: 14px;")
        inner_layout.addWidget(icon)
        
        # Input
        self._input = QLineEdit()
        self._input.setPlaceholderText(placeholder)
        self._input.setFrame(False)
        self._input.setStyleSheet(f"""
            QLineEdit {{
                background: transparent;
                border: none;
                padding: 12px 0;
                color: {p.text_primary};
                font-size: 13px;
            }}
        """)
        self._input.returnPressed.connect(self._on_search)
        self._input.textChanged.connect(self.text_changed.emit)
        inner_layout.addWidget(self._input, 1)
        
        # Clear button
        self._clear_btn = QPushButton("✕")
        self._clear_btn.setObjectName("iconBtn")
        self._clear_btn.setFixedSize(28, 28)
        self._clear_btn.setVisible(False)
        self._clear_btn.clicked.connect(self._clear)
        self._input.textChanged.connect(lambda t: self._clear_btn.setVisible(bool(t)))
        inner_layout.addWidget(self._clear_btn)
        
        layout.addWidget(container, 1)
        
        # Search button
        if show_button:
            self._search_btn = QPushButton("Search")
            self._search_btn.setObjectName("secondaryBtn")
            self._search_btn.setMinimumWidth(80)
            self._search_btn.clicked.connect(self._on_search)
            layout.addSpacing(8)
            layout.addWidget(self._search_btn)
    
    def _on_search(self) -> None:
        text = self._input.text().strip()
        if text:
            self.search_triggered.emit(text)
    
    def _clear(self) -> None:
        self._input.clear()
        self._input.setFocus()
    
    def text(self) -> str:
        return self._input.text()
    
    def set_text(self, text: str) -> None:
        self._input.setText(text)
    
    def setFocus(self) -> None:
        self._input.setFocus()


class StatusBadge(QLabel):
    """Colored status badge"""
    
    def __init__(
        self,
        text: str = "",
        status: str = "neutral",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(text, parent)
        self.set_status(status)
    
    def set_status(self, status: str) -> None:
        object_names = {
            "success": "badgeSuccess",
            "warning": "badgeWarning",
            "danger": "badgeDanger",
            "info": "badgeInfo",
            "primary": "badgePrimary",
            "neutral": "badgeNeutral",
        }
        self.setObjectName(object_names.get(status, "badgeNeutral"))
        self.style().unpolish(self)
        self.style().polish(self)
    
    def set_text_with_icon(self, icon: str, text: str) -> None:
        self.setText(f"{icon} {text}")


class IconButton(QPushButton):
    """Icon-only button with tooltip"""
    
    def __init__(
        self,
        icon: str,
        tooltip: str = "",
        size: int = 40,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(icon, parent)
        self.setObjectName("iconBtn")
        self.setToolTip(tooltip)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFixedSize(size, size)
        self.setFont(QFont("Segoe UI Emoji", 14))


class ProgressCard(AnimatedCard):
    """Card with progress indicator"""
    
    def __init__(
        self,
        title: str,
        total: int = 100,
        color: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent, clickable=False)
        self.setObjectName("metricCard")
        self._color = color or get_theme().palette.primary
        self._total = total
        self._current = 0
        
        self._setup_ui(title)
    
    def _setup_ui(self, title: str) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 18)
        layout.setSpacing(12)
        
        # Header
        header = QHBoxLayout()
        
        self._title = QLabel(title)
        self._title.setObjectName("metricTitle")
        header.addWidget(self._title)
        
        header.addStretch()
        
        self._percentage = QLabel("0%")
        self._percentage.setStyleSheet(f"color: {self._color}; font-weight: 600;")
        header.addWidget(self._percentage)
        
        layout.addLayout(header)
        
        # Progress bar
        self._progress = QProgressBar()
        self._progress.setMaximum(self._total)
        self._progress.setValue(0)
        self._progress.setTextVisible(False)
        self._progress.setFixedHeight(8)
        self._progress.setStyleSheet(f"""
            QProgressBar {{
                background: {get_theme().palette.background_secondary};
                border: none;
                border-radius: 4px;
            }}
            QProgressBar::chunk {{
                background: {self._color};
                border-radius: 4px;
            }}
        """)
        layout.addWidget(self._progress)
        
        # Stats
        self._stats = QLabel("0 / 0")
        self._stats.setObjectName("metricSubtitle")
        layout.addWidget(self._stats)
    
    def set_value(self, current: int, total: int | None = None) -> None:
        if total is not None:
            self._total = total
            self._progress.setMaximum(total)
        
        self._current = current
        self._progress.setValue(current)
        
        percentage = (current / self._total * 100) if self._total > 0 else 0
        self._percentage.setText(f"{percentage:.0f}%")
        self._stats.setText(f"{current:,} / {self._total:,}")
    
    def set_title(self, title: str) -> None:
        self._title.setText(title)


class StatRow(QWidget):
    """Horizontal stat display row"""
    
    def __init__(
        self,
        items: list[tuple[str, str, str]] | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._items: dict[str, tuple[QLabel, QLabel]] = {}
        self._setup_ui(items or [])
    
    def _setup_ui(self, items: list[tuple[str, str, str]]) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(24)
        
        for key, label, value in items:
            self._add_stat(layout, key, label, value)
        
        layout.addStretch()
    
    def _add_stat(self, layout: QHBoxLayout, key: str, label: str, value: str) -> None:
        container = QVBoxLayout()
        container.setSpacing(2)
        
        label_widget = QLabel(label)
        label_widget.setStyleSheet(f"color: {get_theme().palette.text_muted}; font-size: 11px;")
        container.addWidget(label_widget)
        
        value_widget = QLabel(value)
        value_widget.setStyleSheet(f"color: {get_theme().palette.text_primary}; font-size: 14px; font-weight: 600;")
        container.addWidget(value_widget)
        
        layout.addLayout(container)
        self._items[key] = (label_widget, value_widget)
    
    def set_value(self, key: str, value: str) -> None:
        if key in self._items:
            self._items[key][1].setText(value)
    
    def add_item(self, key: str, label: str, value: str) -> None:
        layout = self.layout()
        if isinstance(layout, QHBoxLayout):
            # Insert before stretch
            self._add_stat(layout, key, label, value)


class ToggleSwitch(QWidget):
    """Modern toggle switch"""
    
    toggled = Signal(bool)
    
    def __init__(
        self,
        checked: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._checked = checked
        self._handle_position = 1.0 if checked else 0.0
        
        self.setFixedSize(48, 26)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        
        self._animation = QPropertyAnimation(self, b"handle_position")
        self._animation.setDuration(150)
        self._animation.setEasingCurve(QEasingCurve.Type.InOutQuad)
    
    def get_handle_position(self) -> float:
        return self._handle_position
    
    def set_handle_position(self, pos: float) -> None:
        self._handle_position = pos
        self.update()
    
    handle_position = Property(float, get_handle_position, set_handle_position)
    
    def is_checked(self) -> bool:
        return self._checked
    
    def set_checked(self, checked: bool, *, animate: bool = True) -> None:
        if self._checked == checked:
            return
        
        self._checked = checked
        
        if animate:
            self._animation.setEndValue(1.0 if checked else 0.0)
            self._animation.start()
        else:
            self._handle_position = 1.0 if checked else 0.0
            self.update()
        
        self.toggled.emit(checked)
    
    def toggle(self) -> None:
        self.set_checked(not self._checked)
    
    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self.toggle()
        super().mousePressEvent(event)
    
    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        p = get_theme().palette
        
        # Track
        track_rect = QRectF(0, 3, 48, 20)
        track_color = QColor(p.primary) if self._checked else QColor(p.border)
        
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(track_color)
        painter.drawRoundedRect(track_rect, 10, 10)
        
        # Handle
        handle_x = 2 + (self._handle_position * 24)
        handle_rect = QRectF(handle_x, 1, 22, 24)
        
        painter.setBrush(QColor(p.surface))
        painter.drawEllipse(handle_rect)


class Divider(QFrame):
    """Horizontal or vertical divider line"""
    
    def __init__(
        self,
        orientation: Qt.Orientation = Qt.Orientation.Horizontal,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        
        if orientation == Qt.Orientation.Horizontal:
            self.setFrameShape(QFrame.Shape.HLine)
            self.setFixedHeight(1)
        else:
            self.setFrameShape(QFrame.Shape.VLine)
            self.setFixedWidth(1)
        
        self.setStyleSheet(f"background: {get_theme().palette.border};")


class EmptyState(QWidget):
    """Empty state placeholder with icon and message"""
    
    action_clicked = Signal()
    
    def __init__(
        self,
        icon: str = "📭",
        title: str = "No Data",
        message: str = "",
        action_text: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._setup_ui(icon, title, message, action_text)
    
    def _setup_ui(self, icon: str, title: str, message: str, action_text: str) -> None:
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.setSpacing(16)
        
        p = get_theme().palette
        
        # Icon
        icon_label = QLabel(icon)
        icon_label.setFont(QFont("Segoe UI Emoji", 48))
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        icon_label.setStyleSheet(f"color: {p.text_muted};")
        layout.addWidget(icon_label)
        
        # Title
        title_label = QLabel(title)
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setStyleSheet(f"font-size: 18px; font-weight: 600; color: {p.text_primary};")
        layout.addWidget(title_label)
        
        # Message
        if message:
            message_label = QLabel(message)
            message_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            message_label.setWordWrap(True)
            message_label.setStyleSheet(f"color: {p.text_secondary}; max-width: 300px;")
            layout.addWidget(message_label)
        
        # Action button
        if action_text:
            action_btn = QPushButton(action_text)
            action_btn.clicked.connect(self.action_clicked.emit)
            layout.addWidget(action_btn, alignment=Qt.AlignmentFlag.AlignCenter)


class Skeleton(QFrame):
    """Loading skeleton placeholder with shimmer animation"""
    
    def __init__(
        self,
        width: int | None = None,
        height: int = 20,
        rounded: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._shimmer_pos = 0
        
        if width:
            self.setFixedWidth(width)
        if height:
            self.setFixedHeight(height)
        
        self._rounded = rounded
        
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._update_shimmer)
        self._timer.start(30)
    
    def _update_shimmer(self) -> None:
        self._shimmer_pos = (self._shimmer_pos + 5) % (self.width() + 100)
        self.update()
    
    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        p = get_theme().palette
        rect = self.rect()
        
        # Base
        painter.setPen(Qt.PenStyle.NoPen)
        painter.setBrush(QColor(p.background_secondary))
        
        if self._rounded:
            painter.drawRoundedRect(rect, rect.height() // 2, rect.height() // 2)
        else:
            painter.drawRect(rect)
        
        # Shimmer gradient
        gradient = QLinearGradient(self._shimmer_pos - 100, 0, self._shimmer_pos, 0)
        gradient.setColorAt(0, QColor(255, 255, 255, 0))
        gradient.setColorAt(0.5, QColor(255, 255, 255, 40))
        gradient.setColorAt(1, QColor(255, 255, 255, 0))
        
        painter.setBrush(gradient)
        if self._rounded:
            painter.drawRoundedRect(rect, rect.height() // 2, rect.height() // 2)
        else:
            painter.drawRect(rect)
    
    def stop(self) -> None:
        self._timer.stop()


class CountdownTimer(QWidget):
    """Visual countdown timer"""
    
    timeout = Signal()
    tick = Signal(int)  # Emits remaining seconds
    
    def __init__(
        self,
        duration_seconds: int = 60,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._total = duration_seconds
        self._remaining = duration_seconds
        
        self._setup_ui()
        
        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._on_tick)
    
    def _setup_ui(self) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self._label = QLabel(self._format_time(self._remaining))
        self._label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self._label.setStyleSheet(f"""
            font-size: 24px;
            font-weight: 700;
            color: {get_theme().palette.text_primary};
            font-family: 'Consolas', 'Monaco', monospace;
        """)
        layout.addWidget(self._label)
    
    def _format_time(self, seconds: int) -> str:
        mins, secs = divmod(max(0, seconds), 60)
        return f"{mins:02d}:{secs:02d}"
    
    def _on_tick(self) -> None:
        self._remaining -= 1
        self._label.setText(self._format_time(self._remaining))
        self.tick.emit(self._remaining)
        
        if self._remaining <= 10:
            self._label.setStyleSheet(f"""
                font-size: 24px;
                font-weight: 700;
                color: {get_theme().palette.danger};
                font-family: 'Consolas', 'Monaco', monospace;
            """)
        
        if self._remaining <= 0:
            self._timer.stop()
            self.timeout.emit()
    
    def start(self, duration_seconds: int | None = None) -> None:
        if duration_seconds is not None:
            self._total = duration_seconds
        self._remaining = self._total
        self._label.setText(self._format_time(self._remaining))
        self._label.setStyleSheet(f"""
            font-size: 24px;
            font-weight: 700;
            color: {get_theme().palette.text_primary};
            font-family: 'Consolas', 'Monaco', monospace;
        """)
        self._timer.start()
    
    def stop(self) -> None:
        self._timer.stop()
    
    def reset(self) -> None:
        self.stop()
        self._remaining = self._total
        self._label.setText(self._format_time(self._remaining))


class AvatarGroup(QWidget):
    """Stacked avatar/icon group"""
    
    def __init__(
        self,
        items: list[str] | None = None,
        max_display: int = 5,
        size: int = 32,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._items = items or []
        self._max_display = max_display
        self._size = size
        self._overlap = size // 3
        
        self._update_size()
    
    def set_items(self, items: list[str]) -> None:
        self._items = items
        self._update_size()
        self.update()
    
    def _update_size(self) -> None:
        display_count = min(len(self._items), self._max_display)
        if display_count == 0:
            self.setFixedSize(0, 0)
        else:
            width = self._size + (display_count - 1) * (self._size - self._overlap)
            if len(self._items) > self._max_display:
                width += self._size - self._overlap
            self.setFixedSize(width, self._size)
    
    def paintEvent(self, event) -> None:
        if not self._items:
            return
        
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        p = get_theme().palette
        colors = [p.chart_1, p.chart_2, p.chart_3, p.chart_4, p.chart_5, p.chart_6]
        
        display_items = self._items[:self._max_display]
        overflow = len(self._items) - self._max_display
        
        for i, item in enumerate(display_items):
            x = i * (self._size - self._overlap)
            rect = QRectF(x, 0, self._size, self._size)
            
            # Background circle
            painter.setPen(QPen(QColor(p.surface), 2))
            painter.setBrush(QColor(colors[i % len(colors)]))
            painter.drawEllipse(rect.adjusted(1, 1, -1, -1))
            
            # Icon/text
            painter.setPen(QColor(p.text_inverse))
            painter.setFont(QFont("Segoe UI Emoji", self._size // 3))
            painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, item)
        
        # Overflow indicator
        if overflow > 0:
            x = len(display_items) * (self._size - self._overlap)
            rect = QRectF(x, 0, self._size, self._size)
            
            painter.setPen(QPen(QColor(p.surface), 2))
            painter.setBrush(QColor(p.background_secondary))
            painter.drawEllipse(rect.adjusted(1, 1, -1, -1))
            
            painter.setPen(QColor(p.text_secondary))
            painter.setFont(QFont("Segoe UI", self._size // 4, QFont.Weight.DemiBold))
            painter.drawText(rect, Qt.AlignmentFlag.AlignCenter, f"+{overflow}")
