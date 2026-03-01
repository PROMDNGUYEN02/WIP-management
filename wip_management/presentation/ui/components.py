# wip_management/presentation/ui/components.py
"""
Reusable UI Components with Modern Design
"""
from __future__ import annotations

import contextlib
from datetime import datetime
from typing import Any, Callable

from PySide6.QtCore import (
    Property, QEasingCurve, QPropertyAnimation, QPoint, QRect, QSize, Qt, 
    QTimer, Signal
)
from PySide6.QtGui import QColor, QFont, QIcon, QPainter, QPainterPath, QPen
from PySide6.QtWidgets import (
    QFrame, QGraphicsDropShadowEffect, QGraphicsOpacityEffect,
    QHBoxLayout, QLabel, QProgressBar, QPushButton, QSizePolicy,
    QStackedWidget, QVBoxLayout, QWidget
)

from wip_management.presentation.ui.theme import theme


class AnimatedCard(QFrame):
    """Card with hover animation and shadow"""
    
    clicked = Signal()
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self._setup_shadow()
        self._hovered = False
    
    def _setup_shadow(self) -> None:
        shadow = QGraphicsDropShadowEffect(self)
        shadow.setBlurRadius(20)
        shadow.setOffset(0, 4)
        shadow.setColor(QColor(0, 0, 0, 25))
        self.setGraphicsEffect(shadow)
    
    def enterEvent(self, event) -> None:
        self._hovered = True
        shadow = self.graphicsEffect()
        if isinstance(shadow, QGraphicsDropShadowEffect):
            shadow.setBlurRadius(30)
            shadow.setOffset(0, 8)
        super().enterEvent(event)
    
    def leaveEvent(self, event) -> None:
        self._hovered = False
        shadow = self.graphicsEffect()
        if isinstance(shadow, QGraphicsDropShadowEffect):
            shadow.setBlurRadius(20)
            shadow.setOffset(0, 4)
        super().leaveEvent(event)
    
    def mousePressEvent(self, event) -> None:
        if event.button() == Qt.MouseButton.LeftButton:
            self.clicked.emit()
        super().mousePressEvent(event)


class MetricCard(AnimatedCard):
    """
    Modern metric card with:
    - Icon
    - Large value
    - Title
    - Optional trend indicator
    - Animated value changes
    """
    
    def __init__(
        self,
        title: str,
        icon: str = "📊",
        color: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.setObjectName("metricCard")
        self._current_value = 0
        self._target_value = 0
        self._color = color or theme.palette.primary
        
        self._setup_ui(title, icon)
        self._setup_animation()
    
    def _setup_ui(self, title: str, icon: str) -> None:
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(8)
        
        # Top row: icon + trend
        top_row = QHBoxLayout()
        top_row.setSpacing(0)
        
        self._icon = QLabel(icon)
        self._icon.setObjectName("metricIcon")
        self._icon.setFont(QFont("Segoe UI Emoji", 20))
        top_row.addWidget(self._icon)
        
        top_row.addStretch()
        
        self._trend = QLabel("")
        self._trend.setObjectName("metricTrend")
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
        
        # Progress bar (optional)
        self._progress = QProgressBar()
        self._progress.setTextVisible(False)
        self._progress.setMaximumHeight(4)
        self._progress.setVisible(False)
        layout.addWidget(self._progress)
        
        layout.addStretch()
    
    def _setup_animation(self) -> None:
        self._anim_timer = QTimer(self)
        self._anim_timer.setInterval(16)  # ~60fps
        self._anim_timer.timeout.connect(self._animate_value)
    
    def _animate_value(self) -> None:
        if self._current_value == self._target_value:
            self._anim_timer.stop()
            return
        
        diff = self._target_value - self._current_value
        step = max(1, abs(diff) // 10) * (1 if diff > 0 else -1)
        
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
        
        # Update trend
        if old_value > 0:
            diff = value - old_value
            if diff > 0:
                self._trend.setText(f"↑ +{diff}")
                self._trend.setObjectName("metricTrendUp")
            elif diff < 0:
                self._trend.setText(f"↓ {diff}")
                self._trend.setObjectName("metricTrendDown")
            else:
                self._trend.setText("")
            self._trend.setVisible(diff != 0)
            self._trend.style().unpolish(self._trend)
            self._trend.style().polish(self._trend)
    
    def set_progress(self, value: int, maximum: int) -> None:
        self._progress.setMaximum(maximum)
        self._progress.setValue(value)
        self._progress.setVisible(maximum > 0)
    
    def set_icon(self, icon: str) -> None:
        self._icon.setText(icon)


class StatusBadge(QLabel):
    """Colored status badge with icon"""
    
    def __init__(
        self,
        text: str = "",
        status: str = "default",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(text, parent)
        self.set_status(status)
    
    def set_status(self, status: str) -> None:
        status_map = {
            "success": ("agingReady", "✓"),
            "warning": ("agingWaiting", "⏳"),
            "danger": ("agingExceed", "⚠"),
            "info": ("", "ℹ"),
            "default": ("", ""),
        }
        object_name, icon = status_map.get(status, ("", ""))
        self.setObjectName(object_name)
        
        if icon and self.text():
            self.setText(f"{icon} {self.text()}")
        
        self.style().unpolish(self)
        self.style().polish(self)


class LoadingOverlay(QFrame):
    """Semi-transparent loading overlay with spinner"""
    
    def __init__(self, parent: QWidget | None = None) -> None:
        super().__init__(parent)
        self.setObjectName("loadingOverlay")
        self.setAttribute(Qt.WidgetAttribute.WA_TransparentForMouseEvents, False)
        self.hide()
        
        layout = QVBoxLayout(self)
        layout.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Spinner
        self._spinner = SpinnerWidget(size=48, parent=self)
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


class SpinnerWidget(QWidget):
    """Animated loading spinner"""
    
    def __init__(
        self,
        size: int = 32,
        color: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._size = size
        self._color = QColor(color) if color else QColor(theme.palette.primary)
        self._angle = 0
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._rotate)
        
        self.setFixedSize(size, size)
    
    def start(self) -> None:
        self._timer.start(16)
    
    def stop(self) -> None:
        self._timer.stop()
    
    def _rotate(self) -> None:
        self._angle = (self._angle + 10) % 360
        self.update()
    
    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        # Draw arc
        pen = QPen(self._color)
        pen.setWidth(3)
        pen.setCapStyle(Qt.PenCapStyle.RoundCap)
        painter.setPen(pen)
        
        margin = 4
        rect = QRect(margin, margin, self._size - 2*margin, self._size - 2*margin)
        painter.drawArc(rect, self._angle * 16, 270 * 16)


class IconButton(QPushButton):
    """Icon-only button with tooltip"""
    
    def __init__(
        self,
        icon: str,
        tooltip: str = "",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(icon, parent)
        self.setObjectName("iconButton")
        self.setToolTip(tooltip)
        self.setCursor(Qt.CursorShape.PointingHandCursor)
        self.setFont(QFont("Segoe UI Emoji", 14))


class SearchBar(QWidget):
    """Modern search bar with icon and clear button"""
    
    search_triggered = Signal(str)
    text_changed = Signal(str)
    
    def __init__(
        self,
        placeholder: str = "Search...",
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._setup_ui(placeholder)
    
    def _setup_ui(self, placeholder: str) -> None:
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)
        
        # Container
        container = QFrame()
        container.setObjectName("searchContainer")
        container.setStyleSheet(f"""
            QFrame#searchContainer {{
                background: {theme.palette.surface};
                border: 1px solid {theme.palette.border};
                border-radius: 10px;
            }}
            QFrame#searchContainer:focus-within {{
                border-color: {theme.palette.primary};
            }}
        """)
        
        inner_layout = QHBoxLayout(container)
        inner_layout.setContentsMargins(12, 0, 4, 0)
        inner_layout.setSpacing(8)
        
        # Search icon
        icon = QLabel("🔍")
        icon.setStyleSheet(f"color: {theme.palette.text_muted};")
        inner_layout.addWidget(icon)
        
        # Input
        from PySide6.QtWidgets import QLineEdit
        self._input = QLineEdit()
        self._input.setPlaceholderText(placeholder)
        self._input.setFrame(False)
        self._input.setStyleSheet("""
            QLineEdit {
                background: transparent;
                border: none;
                padding: 10px 0;
            }
        """)
        self._input.returnPressed.connect(self._on_search)
        self._input.textChanged.connect(self.text_changed.emit)
        inner_layout.addWidget(self._input, 1)
        
        # Clear button
        self._clear_btn = IconButton("✕", "Clear")
        self._clear_btn.setVisible(False)
        self._clear_btn.clicked.connect(self._clear)
        self._input.textChanged.connect(
            lambda t: self._clear_btn.setVisible(bool(t))
        )
        inner_layout.addWidget(self._clear_btn)
        
        layout.addWidget(container)
    
    def _on_search(self) -> None:
        self.search_triggered.emit(self._input.text().strip())
    
    def _clear(self) -> None:
        self._input.clear()
        self._input.setFocus()
    
    def text(self) -> str:
        return self._input.text()
    
    def set_text(self, text: str) -> None:
        self._input.setText(text)


class Toast(QFrame):
    """Notification toast that auto-dismisses"""
    
    closed = Signal()
    
    def __init__(
        self,
        message: str,
        toast_type: str = "info",
        duration_ms: int = 3000,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._duration = duration_ms
        self._setup_ui(message, toast_type)
        self._setup_animation()
    
    def _setup_ui(self, message: str, toast_type: str) -> None:
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.Tool
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        
        # Colors based on type
        colors = {
            "success": (theme.palette.success_bg, theme.palette.success, "✓"),
            "warning": (theme.palette.warning_bg, theme.palette.warning, "⚠"),
            "danger": (theme.palette.danger_bg, theme.palette.danger, "✕"),
            "info": (theme.palette.info_bg, theme.palette.info, "ℹ"),
        }
        bg, fg, icon = colors.get(toast_type, colors["info"])
        
        self.setStyleSheet(f"""
            QFrame {{
                background: {bg};
                border: 1px solid {fg};
                border-radius: 12px;
                padding: 12px 16px;
            }}
            QLabel {{
                color: {fg};
                font-weight: 500;
            }}
        """)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(12)
        
        icon_label = QLabel(icon)
        icon_label.setFont(QFont("Segoe UI Emoji", 16))
        layout.addWidget(icon_label)
        
        text_label = QLabel(message)
        text_label.setWordWrap(True)
        layout.addWidget(text_label, 1)
        
        close_btn = IconButton("✕")
        close_btn.clicked.connect(self.dismiss)
        layout.addWidget(close_btn)
    
    def _setup_animation(self) -> None:
        self._opacity = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self._opacity)
        
        self._show_anim = QPropertyAnimation(self._opacity, b"opacity")
        self._show_anim.setDuration(200)
        self._show_anim.setStartValue(0)
        self._show_anim.setEndValue(1)
        
        self._hide_anim = QPropertyAnimation(self._opacity, b"opacity")
        self._hide_anim.setDuration(200)
        self._hide_anim.setStartValue(1)
        self._hide_anim.setEndValue(0)
        self._hide_anim.finished.connect(self._on_hidden)
    
    def show_toast(self) -> None:
        self.show()
        self._show_anim.start()
        QTimer.singleShot(self._duration, self.dismiss)
    
    def dismiss(self) -> None:
        self._hide_anim.start()
    
    def _on_hidden(self) -> None:
        self.closed.emit()
        self.deleteLater()


class ToastManager:
    """Manages toast notifications positioning"""
    
    _instance: "ToastManager | None" = None
    
    def __new__(cls) -> "ToastManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._toasts = []
            cls._instance._parent = None
        return cls._instance
    
    def set_parent(self, parent: QWidget) -> None:
        self._parent = parent
    
    def show(
        self,
        message: str,
        toast_type: str = "info",
        duration_ms: int = 3000,
    ) -> Toast:
        toast = Toast(message, toast_type, duration_ms, self._parent)
        toast.closed.connect(lambda: self._remove_toast(toast))
        
        self._toasts.append(toast)
        self._position_toasts()
        toast.show_toast()
        
        return toast
    
    def _remove_toast(self, toast: Toast) -> None:
        with contextlib.suppress(ValueError):
            self._toasts.remove(toast)
        self._position_toasts()
    
    def _position_toasts(self) -> None:
        if not self._parent:
            return
        
        parent_rect = self._parent.rect()
        x = parent_rect.right() - 320
        y = parent_rect.top() + 80
        
        for toast in self._toasts:
            toast.move(x, y)
            toast.setFixedWidth(300)
            y += toast.sizeHint().height() + 12


# Global toast manager
toast_manager = ToastManager()