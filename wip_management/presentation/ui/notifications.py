# wip_management/presentation/ui/notifications.py
"""
Toast Notification System
"""
from __future__ import annotations

import contextlib
from typing import Callable
from datetime import datetime

from PySide6.QtCore import (
    QEasingCurve, QPoint, QPropertyAnimation, QRect, QSequentialAnimationGroup,
    Qt, QTimer, Signal, Property, QSize
)
from PySide6.QtGui import QColor, QFont, QGuiApplication
from PySide6.QtWidgets import (
    QFrame, QGraphicsOpacityEffect, QHBoxLayout, QLabel, 
    QPushButton, QVBoxLayout, QWidget, QApplication
)

from wip_management.presentation.ui.theme import get_theme


class ToastType:
    SUCCESS = "success"
    WARNING = "warning"
    DANGER = "danger"
    INFO = "info"


class Toast(QFrame):
    """
    Modern toast notification with:
    - Slide-in/out animations
    - Auto-dismiss
    - Action buttons
    - Progress indicator
    """
    
    closed = Signal()
    action_clicked = Signal(str)
    
    def __init__(
        self,
        message: str,
        toast_type: str = ToastType.INFO,
        title: str = "",
        duration_ms: int = 4000,
        action_text: str = "",
        show_close: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._duration = duration_ms
        self._toast_type = toast_type
        self._progress_value = 100
        
        self._setup_ui(message, title, action_text, show_close)
        self._setup_animations()
        self._apply_style()
    
    def _setup_ui(self, message: str, title: str, action_text: str, show_close: bool) -> None:
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint |
            Qt.WindowType.WindowStaysOnTopHint |
            Qt.WindowType.Tool |
            Qt.WindowType.BypassWindowManagerHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground)
        self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating)
        self.setFixedWidth(380)
        
        # Main layout
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(0)
        
        # Content container
        self._container = QFrame()
        container_layout = QHBoxLayout(self._container)
        container_layout.setContentsMargins(16, 14, 12, 14)
        container_layout.setSpacing(14)
        
        # Icon
        icons = {
            ToastType.SUCCESS: "✓",
            ToastType.WARNING: "⚠",
            ToastType.DANGER: "✕",
            ToastType.INFO: "ℹ",
        }
        icon_label = QLabel(icons.get(self._toast_type, "ℹ"))
        icon_label.setFont(QFont("Segoe UI Emoji", 16))
        icon_label.setFixedSize(28, 28)
        icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        container_layout.addWidget(icon_label)
        
        # Text container
        text_container = QVBoxLayout()
        text_container.setSpacing(4)
        
        if title:
            title_label = QLabel(title)
            title_label.setFont(QFont("Segoe UI", 12, QFont.Weight.DemiBold))
            text_container.addWidget(title_label)
        
        message_label = QLabel(message)
        message_label.setWordWrap(True)
        message_label.setFont(QFont("Segoe UI", 11))
        text_container.addWidget(message_label)
        
        container_layout.addLayout(text_container, 1)
        
        # Action button
        if action_text:
            action_btn = QPushButton(action_text)
            action_btn.setObjectName("ghostBtn")
            action_btn.setFont(QFont("Segoe UI", 11, QFont.Weight.DemiBold))
            action_btn.clicked.connect(lambda: self.action_clicked.emit(action_text))
            action_btn.clicked.connect(self.dismiss)
            container_layout.addWidget(action_btn)
        
        # Close button
        if show_close:
            close_btn = QPushButton("✕")
            close_btn.setObjectName("iconBtn")
            close_btn.setFixedSize(28, 28)
            close_btn.clicked.connect(self.dismiss)
            container_layout.addWidget(close_btn)
        
        main_layout.addWidget(self._container)
        
        # Progress bar
        self._progress_bar = QFrame()
        self._progress_bar.setFixedHeight(3)
        self._progress_bar.setStyleSheet(f"background: {get_theme().palette.primary}; border-radius: 1px;")
        main_layout.addWidget(self._progress_bar)
    
    def _apply_style(self) -> None:
        object_names = {
            ToastType.SUCCESS: "toastSuccess",
            ToastType.WARNING: "toastWarning",
            ToastType.DANGER: "toastDanger",
            ToastType.INFO: "toastInfo",
        }
        self._container.setObjectName(object_names.get(self._toast_type, "toastInfo"))
    
    def _setup_animations(self) -> None:
        # Opacity effect
        self._opacity = QGraphicsOpacityEffect(self)
        self._opacity.setOpacity(0)
        self.setGraphicsEffect(self._opacity)
        
        # Fade in
        self._fade_in = QPropertyAnimation(self._opacity, b"opacity")
        self._fade_in.setDuration(200)
        self._fade_in.setStartValue(0)
        self._fade_in.setEndValue(1)
        self._fade_in.setEasingCurve(QEasingCurve.Type.OutCubic)
        
        # Fade out
        self._fade_out = QPropertyAnimation(self._opacity, b"opacity")
        self._fade_out.setDuration(200)
        self._fade_out.setStartValue(1)
        self._fade_out.setEndValue(0)
        self._fade_out.setEasingCurve(QEasingCurve.Type.InCubic)
        self._fade_out.finished.connect(self._on_hidden)
        
        # Progress timer
        self._progress_timer = QTimer(self)
        self._progress_timer.setInterval(30)
        self._progress_timer.timeout.connect(self._update_progress)
    
    def _update_progress(self) -> None:
        step = 100 / (self._duration / 30)
        self._progress_value -= step
        
        if self._progress_value <= 0:
            self._progress_timer.stop()
            self.dismiss()
        else:
            self._progress_bar.setFixedWidth(int(self.width() * self._progress_value / 100))
    
    def show_toast(self) -> None:
        self.show()
        self._fade_in.start()
        QTimer.singleShot(300, self._progress_timer.start)
    
    def dismiss(self) -> None:
        self._progress_timer.stop()
        self._fade_out.start()
    
    def _on_hidden(self) -> None:
        self.closed.emit()
        self.deleteLater()
    
    def enterEvent(self, event) -> None:
        """Pause timer on hover"""
        self._progress_timer.stop()
        super().enterEvent(event)
    
    def leaveEvent(self, event) -> None:
        """Resume timer on leave"""
        if self._progress_value > 0:
            self._progress_timer.start()
        super().leaveEvent(event)


class ToastManager:
    """
    Manages toast notifications with:
    - Stacking
    - Position management
    - Queue for multiple toasts
    """
    
    _instance: "ToastManager | None" = None
    
    def __new__(cls) -> "ToastManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._toasts: list[Toast] = []
            cls._instance._parent = None
            cls._instance._position = "top-right"
            cls._instance._margin = 20
            cls._instance._spacing = 12
        return cls._instance
    
    def set_parent(self, parent: QWidget) -> None:
        self._parent = parent
    
    def set_position(self, position: str) -> None:
        """Set toast position: top-right, top-left, bottom-right, bottom-left, top-center, bottom-center"""
        self._position = position
    
    def success(self, message: str, title: str = "", duration_ms: int = 4000, **kwargs) -> Toast:
        return self.show(message, ToastType.SUCCESS, title, duration_ms, **kwargs)
    
    def warning(self, message: str, title: str = "", duration_ms: int = 5000, **kwargs) -> Toast:
        return self.show(message, ToastType.WARNING, title, duration_ms, **kwargs)
    
    def error(self, message: str, title: str = "", duration_ms: int = 6000, **kwargs) -> Toast:
        return self.show(message, ToastType.DANGER, title, duration_ms, **kwargs)
    
    def info(self, message: str, title: str = "", duration_ms: int = 4000, **kwargs) -> Toast:
        return self.show(message, ToastType.INFO, title, duration_ms, **kwargs)
    
    def show(
        self,
        message: str,
        toast_type: str = ToastType.INFO,
        title: str = "",
        duration_ms: int = 4000,
        action_text: str = "",
        show_close: bool = True,
    ) -> Toast:
        toast = Toast(
            message=message,
            toast_type=toast_type,
            title=title,
            duration_ms=duration_ms,
            action_text=action_text,
            show_close=show_close,
            parent=None,  # No parent for floating toast
        )
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
        screen = QGuiApplication.primaryScreen()
        if screen is None:
            return
        
        screen_geo = screen.availableGeometry()
        
        # Calculate base position
        if "right" in self._position:
            x = screen_geo.right() - 380 - self._margin
        elif "left" in self._position:
            x = screen_geo.left() + self._margin
        else:  # center
            x = screen_geo.center().x() - 190
        
        if "top" in self._position:
            y_start = screen_geo.top() + self._margin
            y_direction = 1
        else:  # bottom
            y_start = screen_geo.bottom() - self._margin
            y_direction = -1
        
        y = y_start
        
        for toast in self._toasts:
            toast_height = toast.sizeHint().height()
            
            if y_direction < 0:
                y -= toast_height
            
            toast.move(x, y)
            
            if y_direction > 0:
                y += toast_height + self._spacing
            else:
                y -= self._spacing
    
    def clear_all(self) -> None:
        """Dismiss all toasts"""
        for toast in list(self._toasts):
            toast.dismiss()


# Global instance
toast_manager = ToastManager()


def show_toast(message: str, toast_type: str = ToastType.INFO, **kwargs) -> Toast:
    """Convenience function to show a toast"""
    return toast_manager.show(message, toast_type, **kwargs)


def success_toast(message: str, **kwargs) -> Toast:
    return toast_manager.success(message, **kwargs)


def warning_toast(message: str, **kwargs) -> Toast:
    return toast_manager.warning(message, **kwargs)


def error_toast(message: str, **kwargs) -> Toast:
    return toast_manager.error(message, **kwargs)


def info_toast(message: str, **kwargs) -> Toast:
    return toast_manager.info(message, **kwargs)