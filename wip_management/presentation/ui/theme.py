# wip_management/presentation/ui/theme.py
"""
Advanced Theme System with Dark/Light Mode Support
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Callable
import json
from pathlib import Path

from PySide6.QtCore import QObject, Signal, QSettings
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication


class ThemeMode(Enum):
    LIGHT = "light"
    DARK = "dark"
    SYSTEM = "system"


@dataclass(frozen=True)
class ColorPalette:
    """Complete color palette for theming"""
    # Brand colors
    primary: str
    primary_hover: str
    primary_pressed: str
    primary_light: str
    primary_dark: str
    
    # Secondary
    secondary: str
    secondary_hover: str
    secondary_light: str
    
    # Status colors
    success: str
    success_bg: str
    success_border: str
    warning: str
    warning_bg: str
    warning_border: str
    danger: str
    danger_bg: str
    danger_border: str
    info: str
    info_bg: str
    info_border: str
    
    # Neutral colors
    background: str
    background_secondary: str
    surface: str
    surface_hover: str
    surface_pressed: str
    surface_elevated: str
    border: str
    border_light: str
    border_focus: str
    
    # Text colors
    text_primary: str
    text_secondary: str
    text_tertiary: str
    text_muted: str
    text_inverse: str
    text_link: str
    
    # Shadows
    shadow_sm: str
    shadow_md: str
    shadow_lg: str
    shadow_xl: str
    
    # Overlays
    overlay_light: str
    overlay_dark: str
    
    # Gradients
    gradient_primary: str
    gradient_success: str
    gradient_warning: str
    gradient_danger: str
    
    # Chart colors
    chart_1: str
    chart_2: str
    chart_3: str
    chart_4: str
    chart_5: str
    chart_6: str


LIGHT_PALETTE = ColorPalette(
    # Brand
    primary="#2563eb",
    primary_hover="#1d4ed8",
    primary_pressed="#1e40af",
    primary_light="#dbeafe",
    primary_dark="#1e3a8a",
    
    # Secondary
    secondary="#7c3aed",
    secondary_hover="#6d28d9",
    secondary_light="#ede9fe",
    
    # Status
    success="#16a34a",
    success_bg="#dcfce7",
    success_border="#86efac",
    warning="#d97706",
    warning_bg="#fef3c7",
    warning_border="#fcd34d",
    danger="#dc2626",
    danger_bg="#fee2e2",
    danger_border="#fca5a5",
    info="#0891b2",
    info_bg="#cffafe",
    info_border="#67e8f9",
    
    # Neutral
    background="#f8fafc",
    background_secondary="#f1f5f9",
    surface="#ffffff",
    surface_hover="#f8fafc",
    surface_pressed="#f1f5f9",
    surface_elevated="#ffffff",
    border="#e2e8f0",
    border_light="#f1f5f9",
    border_focus="#2563eb",
    
    # Text
    text_primary="#0f172a",
    text_secondary="#475569",
    text_tertiary="#64748b",
    text_muted="#94a3b8",
    text_inverse="#ffffff",
    text_link="#2563eb",
    
    # Shadows
    shadow_sm="rgba(0, 0, 0, 0.05)",
    shadow_md="rgba(0, 0, 0, 0.1)",
    shadow_lg="rgba(0, 0, 0, 0.15)",
    shadow_xl="rgba(0, 0, 0, 0.2)",
    
    # Overlays
    overlay_light="rgba(255, 255, 255, 0.8)",
    overlay_dark="rgba(15, 23, 42, 0.5)",
    
    # Gradients
    gradient_primary="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #2563eb, stop:1 #7c3aed)",
    gradient_success="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #16a34a, stop:1 #22d3ee)",
    gradient_warning="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #d97706, stop:1 #f59e0b)",
    gradient_danger="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #dc2626, stop:1 #f87171)",
    
    # Charts
    chart_1="#2563eb",
    chart_2="#7c3aed",
    chart_3="#16a34a",
    chart_4="#d97706",
    chart_5="#dc2626",
    chart_6="#0891b2",
)


DARK_PALETTE = ColorPalette(
    # Brand
    primary="#3b82f6",
    primary_hover="#60a5fa",
    primary_pressed="#2563eb",
    primary_light="#1e3a5f",
    primary_dark="#93c5fd",
    
    # Secondary
    secondary="#8b5cf6",
    secondary_hover="#a78bfa",
    secondary_light="#2e1065",
    
    # Status
    success="#22c55e",
    success_bg="#14532d",
    success_border="#16a34a",
    warning="#f59e0b",
    warning_bg="#451a03",
    warning_border="#d97706",
    danger="#ef4444",
    danger_bg="#450a0a",
    danger_border="#dc2626",
    info="#06b6d4",
    info_bg="#083344",
    info_border="#0891b2",
    
    # Neutral
    background="#0f172a",
    background_secondary="#1e293b",
    surface="#1e293b",
    surface_hover="#334155",
    surface_pressed="#475569",
    surface_elevated="#334155",
    border="#334155",
    border_light="#1e293b",
    border_focus="#3b82f6",
    
    # Text
    text_primary="#f8fafc",
    text_secondary="#cbd5e1",
    text_tertiary="#94a3b8",
    text_muted="#64748b",
    text_inverse="#0f172a",
    text_link="#60a5fa",
    
    # Shadows
    shadow_sm="rgba(0, 0, 0, 0.3)",
    shadow_md="rgba(0, 0, 0, 0.4)",
    shadow_lg="rgba(0, 0, 0, 0.5)",
    shadow_xl="rgba(0, 0, 0, 0.6)",
    
    # Overlays
    overlay_light="rgba(255, 255, 255, 0.1)",
    overlay_dark="rgba(0, 0, 0, 0.7)",
    
    # Gradients
    gradient_primary="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #3b82f6, stop:1 #8b5cf6)",
    gradient_success="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #22c55e, stop:1 #06b6d4)",
    gradient_warning="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #f59e0b, stop:1 #fbbf24)",
    gradient_danger="qlineargradient(x1:0, y1:0, x2:1, y2:1, stop:0 #ef4444, stop:1 #f87171)",
    
    # Charts
    chart_1="#3b82f6",
    chart_2="#8b5cf6",
    chart_3="#22c55e",
    chart_4="#f59e0b",
    chart_5="#ef4444",
    chart_6="#06b6d4",
)


class ThemeManager(QObject):
    """
    Singleton theme manager with:
    - Dark/Light mode switching
    - System theme detection
    - Theme persistence
    - Live theme updates
    """
    
    theme_changed = Signal(str)  # Emits theme mode name
    
    _instance: "ThemeManager | None" = None
    
    def __new__(cls) -> "ThemeManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        if self._initialized:
            return
        super().__init__()
        self._initialized = True
        self._mode = ThemeMode.LIGHT
        self._palette = LIGHT_PALETTE
        self._settings = QSettings("WIPManagement", "Theme")
        self._load_saved_theme()
    
    def _load_saved_theme(self) -> None:
        """Load theme from settings"""
        saved_mode = self._settings.value("mode", "light")
        if saved_mode == "dark":
            self._mode = ThemeMode.DARK
            self._palette = DARK_PALETTE
        elif saved_mode == "system":
            self._mode = ThemeMode.SYSTEM
            self._detect_system_theme()
        else:
            self._mode = ThemeMode.LIGHT
            self._palette = LIGHT_PALETTE
    
    def _detect_system_theme(self) -> None:
        """Detect system dark mode preference"""
        app = QApplication.instance()
        if app:
            palette = app.palette()
            is_dark = palette.color(QPalette.ColorRole.Window).lightness() < 128
            self._palette = DARK_PALETTE if is_dark else LIGHT_PALETTE
    
    @property
    def mode(self) -> ThemeMode:
        return self._mode
    
    @property
    def palette(self) -> ColorPalette:
        return self._palette
    
    @property
    def is_dark(self) -> bool:
        return self._palette == DARK_PALETTE
    
    def set_mode(self, mode: ThemeMode) -> None:
        """Set theme mode and apply"""
        if mode == self._mode:
            return
        
        self._mode = mode
        
        if mode == ThemeMode.DARK:
            self._palette = DARK_PALETTE
        elif mode == ThemeMode.LIGHT:
            self._palette = LIGHT_PALETTE
        else:
            self._detect_system_theme()
        
        self._settings.setValue("mode", mode.value)
        self.theme_changed.emit(mode.value)
        self._apply_to_application()
    
    def toggle(self) -> None:
        """Toggle between light and dark mode"""
        new_mode = ThemeMode.LIGHT if self.is_dark else ThemeMode.DARK
        self.set_mode(new_mode)
    
    def _apply_to_application(self) -> None:
        """Apply theme to application"""
        app = QApplication.instance()
        if app:
            app.setStyleSheet(self.get_stylesheet())
    
    def get_stylesheet(self) -> str:
        """Generate complete application stylesheet"""
        p = self._palette
        return f"""
        /* ═══════════════════════════════════════════════════════════════
           GLOBAL RESET & BASE STYLES
           ═══════════════════════════════════════════════════════════════ */
        
        * {{
            margin: 0;
            padding: 0;
            outline: none;
        }}
        
        QMainWindow {{
            background: {p.background};
        }}
        
        QWidget {{
            font-family: 'Segoe UI', 'SF Pro Display', -apple-system, sans-serif;
            font-size: 13px;
            color: {p.text_primary};
            background: transparent;
        }}
        
        QWidget:focus {{
            outline: none;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           SCROLLABLE AREAS
           ═══════════════════════════════════════════════════════════════ */
        
        QScrollArea {{
            background: transparent;
            border: none;
        }}
        
        QScrollArea > QWidget > QWidget {{
            background: transparent;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           METRIC CARDS
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#metricCard {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 16px;
        }}
        
        QFrame#metricCard:hover {{
            border-color: {p.primary};
            background: {p.surface_hover};
        }}
        
        QLabel#metricIcon {{
            font-size: 28px;
            color: {p.text_secondary};
        }}
        
        QLabel#metricValue {{
            color: {p.text_primary};
            font-size: 36px;
            font-weight: 700;
            letter-spacing: -1.5px;
        }}
        
        QLabel#metricTitle {{
            color: {p.text_secondary};
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.8px;
        }}
        
        QLabel#metricTrend {{
            font-size: 12px;
            font-weight: 600;
            padding: 4px 8px;
            border-radius: 8px;
        }}
        
        QLabel#metricTrendUp {{
            color: {p.success};
            background: {p.success_bg};
        }}
        
        QLabel#metricTrendDown {{
            color: {p.danger};
            background: {p.danger_bg};
        }}
        
        QLabel#metricSubtitle {{
            color: {p.text_muted};
            font-size: 11px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           COLUMN CARDS
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#columnCard {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 16px;
        }}
        
        QLabel#columnHeader {{
            color: {p.text_primary};
            font-size: 18px;
            font-weight: 700;
        }}
        
        QLabel#columnStats {{
            color: {p.text_muted};
            font-size: 12px;
            font-weight: 500;
        }}
        
        QFrame#columnHeaderFrame {{
            background: {p.surface};
            border: none;
            border-bottom: 1px solid {p.border};
            border-radius: 16px 16px 0 0;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           BUTTONS - PRIMARY
           ═══════════════════════════════════════════════════════════════ */
        
        QPushButton {{
            background: {p.primary};
            color: {p.text_inverse};
            border: none;
            border-radius: 10px;
            padding: 10px 20px;
            font-weight: 600;
            font-size: 13px;
            min-height: 20px;
        }}
        
        QPushButton:hover {{
            background: {p.primary_hover};
        }}
        
        QPushButton:pressed {{
            background: {p.primary_pressed};
        }}
        
        QPushButton:disabled {{
            background: {p.border};
            color: {p.text_muted};
        }}
        
        /* Secondary Button */
        QPushButton#secondaryBtn {{
            background: {p.surface};
            color: {p.text_primary};
            border: 1px solid {p.border};
        }}
        
        QPushButton#secondaryBtn:hover {{
            background: {p.surface_hover};
            border-color: {p.primary};
            color: {p.primary};
        }}
        
        QPushButton#secondaryBtn:pressed {{
            background: {p.surface_pressed};
        }}
        
        /* Ghost Button */
        QPushButton#ghostBtn {{
            background: transparent;
            color: {p.text_secondary};
            border: none;
        }}
        
        QPushButton#ghostBtn:hover {{
            background: {p.surface_hover};
            color: {p.text_primary};
        }}
        
        /* Icon Button */
        QPushButton#iconBtn {{
            background: transparent;
            color: {p.text_secondary};
            border: none;
            border-radius: 10px;
            padding: 10px;
            min-width: 40px;
            max-width: 40px;
            min-height: 40px;
            max-height: 40px;
            font-size: 18px;
        }}
        
        QPushButton#iconBtn:hover {{
            background: {p.surface_hover};
            color: {p.text_primary};
        }}
        
        QPushButton#iconBtn:pressed {{
            background: {p.surface_pressed};
        }}
        
        /* Success Button */
        QPushButton#successBtn {{
            background: {p.success};
        }}
        
        QPushButton#successBtn:hover {{
            background: #15803d;
        }}
        
        /* Danger Button */
        QPushButton#dangerBtn {{
            background: {p.danger};
        }}
        
        QPushButton#dangerBtn:hover {{
            background: #b91c1c;
        }}
        
        /* Warning Button */
        QPushButton#warningBtn {{
            background: {p.warning};
            color: {p.text_inverse};
        }}
        
        QPushButton#warningBtn:hover {{
            background: #b45309;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           INPUT FIELDS
           ═══════════════════════════════════════════════════════════════ */
        
        QLineEdit {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 10px;
            padding: 10px 14px;
            font-size: 13px;
            color: {p.text_primary};
            selection-background-color: {p.primary_light};
            selection-color: {p.text_primary};
        }}
        
        QLineEdit:hover {{
            border-color: {p.text_muted};
        }}
        
        QLineEdit:focus {{
            border-color: {p.primary};
            border-width: 2px;
            padding: 9px 13px;
        }}
        
        QLineEdit:disabled {{
            background: {p.background_secondary};
            color: {p.text_muted};
            border-color: {p.border_light};
        }}
        
        QLineEdit::placeholder {{
            color: {p.text_muted};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           CHECKBOXES
           ═══════════════════════════════════════════════════════════════ */
        
        QCheckBox {{
            spacing: 10px;
            color: {p.text_primary};
            font-weight: 500;
        }}
        
        QCheckBox::indicator {{
            width: 22px;
            height: 22px;
            border-radius: 6px;
            border: 2px solid {p.border};
            background: {p.surface};
        }}
        
        QCheckBox::indicator:hover {{
            border-color: {p.primary};
            background: {p.surface_hover};
        }}
        
        QCheckBox::indicator:checked {{
            background: {p.primary};
            border-color: {p.primary};
        }}
        
        QCheckBox::indicator:checked:hover {{
            background: {p.primary_hover};
            border-color: {p.primary_hover};
        }}
        
        QCheckBox:disabled {{
            color: {p.text_muted};
        }}
        
        QCheckBox::indicator:disabled {{
            background: {p.background_secondary};
            border-color: {p.border_light};
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
            background: {p.surface};
            border: 1px solid {p.border_light};
            border-radius: 12px;
            padding: 14px 16px;
            margin: 4px 8px;
            color: {p.text_primary};
        }}
        
        QListView::item:hover {{
            background: {p.surface_hover};
            border-color: {p.border};
        }}
        
        QListView::item:selected {{
            background: {p.primary_light};
            border-color: {p.primary};
            color: {p.text_primary};
        }}
        
        QListView::item:selected:hover {{
            background: {p.primary_light};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TABLE VIEWS
           ═══════════════════════════════════════════════════════════════ */
        
        QTableView {{
            background: transparent;
            border: none;
            gridline-color: {p.border_light};
            selection-background-color: {p.primary_light};
            selection-color: {p.text_primary};
            alternate-background-color: {p.background_secondary};
        }}
        
        QTableView::item {{
            padding: 10px 12px;
            border: none;
            border-bottom: 1px solid {p.border_light};
        }}
        
        QTableView::item:hover {{
            background: {p.surface_hover};
        }}
        
        QTableView::item:selected {{
            background: {p.primary_light};
        }}
        
        QTableView::item:focus {{
            outline: none;
            border: none;
        }}
        
        QHeaderView {{
            background: transparent;
            border: none;
        }}
        
        QHeaderView::section {{
            background: {p.surface};
            color: {p.text_secondary};
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            padding: 14px 12px;
            border: none;
            border-bottom: 2px solid {p.border};
        }}
        
        QHeaderView::section:hover {{
            background: {p.surface_hover};
            color: {p.text_primary};
        }}
        
        QHeaderView::section:pressed {{
            background: {p.surface_pressed};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           SCROLLBARS
           ═══════════════════════════════════════════════════════════════ */
        
        QScrollBar:vertical {{
            background: transparent;
            width: 12px;
            margin: 4px 2px;
        }}
        
        QScrollBar::handle:vertical {{
            background: {p.border};
            border-radius: 4px;
            min-height: 40px;
        }}
        
        QScrollBar::handle:vertical:hover {{
            background: {p.text_muted};
        }}
        
        QScrollBar::handle:vertical:pressed {{
            background: {p.text_secondary};
        }}
        
        QScrollBar::add-line:vertical,
        QScrollBar::sub-line:vertical {{
            height: 0;
        }}
        
        QScrollBar::add-page:vertical,
        QScrollBar::sub-page:vertical {{
            background: transparent;
        }}
        
        QScrollBar:horizontal {{
            background: transparent;
            height: 12px;
            margin: 2px 4px;
        }}
        
        QScrollBar::handle:horizontal {{
            background: {p.border};
            border-radius: 4px;
            min-width: 40px;
        }}
        
        QScrollBar::handle:horizontal:hover {{
            background: {p.text_muted};
        }}
        
        QScrollBar::add-line:horizontal,
        QScrollBar::sub-line:horizontal {{
            width: 0;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           STATUS BAR
           ═══════════════════════════════════════════════════════════════ */
        
        QStatusBar {{
            background: {p.surface};
            border-top: 1px solid {p.border};
            padding: 4px 0;
        }}
        
        QStatusBar QLabel {{
            color: {p.text_secondary};
            font-size: 12px;
            padding: 4px 12px;
        }}
        
        QStatusBar::item {{
            border: none;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TOOLTIPS
           ═══════════════════════════════════════════════════════════════ */
        
        QToolTip {{
            background: {p.text_primary};
            color: {p.text_inverse};
            border: none;
            border-radius: 8px;
            padding: 8px 14px;
            font-size: 12px;
            font-weight: 500;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           MENUS
           ═══════════════════════════════════════════════════════════════ */
        
        QMenu {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 12px;
            padding: 8px;
        }}
        
        QMenu::item {{
            padding: 10px 40px 10px 16px;
            border-radius: 8px;
            color: {p.text_primary};
            font-weight: 500;
        }}
        
        QMenu::item:selected {{
            background: {p.surface_hover};
        }}
        
        QMenu::item:disabled {{
            color: {p.text_muted};
        }}
        
        QMenu::separator {{
            height: 1px;
            background: {p.border};
            margin: 8px 12px;
        }}
        
        QMenu::icon {{
            padding-left: 12px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           DIALOGS
           ═══════════════════════════════════════════════════════════════ */
        
        QDialog {{
            background: {p.background};
        }}
        
        QDialogButtonBox {{
            button-layout: 3;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           SPIN BOXES
           ═══════════════════════════════════════════════════════════════ */
        
        QSpinBox, QDoubleSpinBox {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 10px;
            padding: 8px 12px;
            color: {p.text_primary};
            min-height: 20px;
        }}
        
        QSpinBox:hover, QDoubleSpinBox:hover {{
            border-color: {p.text_muted};
        }}
        
        QSpinBox:focus, QDoubleSpinBox:focus {{
            border-color: {p.primary};
            border-width: 2px;
            padding: 7px 11px;
        }}
        
        QSpinBox::up-button, QDoubleSpinBox::up-button,
        QSpinBox::down-button, QDoubleSpinBox::down-button {{
            border: none;
            background: transparent;
            width: 24px;
            padding: 4px;
        }}
        
        QSpinBox::up-button:hover, QDoubleSpinBox::up-button:hover,
        QSpinBox::down-button:hover, QDoubleSpinBox::down-button:hover {{
            background: {p.surface_hover};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           COMBO BOXES
           ═══════════════════════════════════════════════════════════════ */
        
        QComboBox {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 10px;
            padding: 10px 14px;
            color: {p.text_primary};
            min-height: 20px;
        }}
        
        QComboBox:hover {{
            border-color: {p.text_muted};
        }}
        
        QComboBox:focus {{
            border-color: {p.primary};
        }}
        
        QComboBox::drop-down {{
            border: none;
            width: 30px;
        }}
        
        QComboBox::down-arrow {{
            width: 12px;
            height: 12px;
        }}
        
        QComboBox QAbstractItemView {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 8px;
            padding: 4px;
            selection-background-color: {p.primary_light};
            selection-color: {p.text_primary};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           PROGRESS BARS
           ═══════════════════════════════════════════════════════════════ */
        
        QProgressBar {{
            background: {p.background_secondary};
            border: none;
            border-radius: 6px;
            height: 12px;
            text-align: center;
            font-size: 10px;
            font-weight: 600;
            color: {p.text_secondary};
        }}
        
        QProgressBar::chunk {{
            background: {p.gradient_primary};
            border-radius: 6px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TAB WIDGET
           ═══════════════════════════════════════════════════════════════ */
        
        QTabWidget::pane {{
            border: 1px solid {p.border};
            border-radius: 12px;
            background: {p.surface};
            margin-top: -1px;
        }}
        
        QTabBar {{
            background: transparent;
        }}
        
        QTabBar::tab {{
            background: transparent;
            color: {p.text_secondary};
            padding: 14px 24px;
            font-weight: 600;
            border: none;
            border-bottom: 3px solid transparent;
            margin-right: 4px;
        }}
        
        QTabBar::tab:hover {{
            color: {p.text_primary};
            background: {p.surface_hover};
        }}
        
        QTabBar::tab:selected {{
            color: {p.primary};
            border-bottom-color: {p.primary};
            background: {p.surface};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           SPLITTER
           ═══════════════════════════════════════════════════════════════ */
        
        QSplitter::handle {{
            background: {p.border};
            margin: 0 4px;
        }}
        
        QSplitter::handle:horizontal {{
            width: 3px;
        }}
        
        QSplitter::handle:vertical {{
            height: 3px;
        }}
        
        QSplitter::handle:hover {{
            background: {p.primary};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           GROUP BOX
           ═══════════════════════════════════════════════════════════════ */
        
        QGroupBox {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 12px;
            margin-top: 16px;
            padding-top: 24px;
            font-weight: 600;
        }}
        
        QGroupBox::title {{
            subcontrol-origin: margin;
            subcontrol-position: top left;
            padding: 4px 12px;
            color: {p.text_primary};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           SLIDER
           ═══════════════════════════════════════════════════════════════ */
        
        QSlider::groove:horizontal {{
            background: {p.background_secondary};
            height: 8px;
            border-radius: 4px;
        }}
        
        QSlider::handle:horizontal {{
            background: {p.primary};
            width: 20px;
            height: 20px;
            margin: -6px 0;
            border-radius: 10px;
        }}
        
        QSlider::handle:horizontal:hover {{
            background: {p.primary_hover};
        }}
        
        QSlider::sub-page:horizontal {{
            background: {p.primary};
            border-radius: 4px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TOOL BAR
           ═══════════════════════════════════════════════════════════════ */
        
        QToolBar {{
            background: {p.surface};
            border: none;
            border-bottom: 1px solid {p.border};
            padding: 8px;
            spacing: 8px;
        }}
        
        QToolBar::separator {{
            background: {p.border};
            width: 1px;
            margin: 4px 8px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TEXT EDIT / PLAIN TEXT EDIT
           ═══════════════════════════════════════════════════════════════ */
        
        QTextEdit, QPlainTextEdit {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 10px;
            padding: 12px;
            color: {p.text_primary};
            selection-background-color: {p.primary_light};
        }}
        
        QTextEdit:focus, QPlainTextEdit:focus {{
            border-color: {p.primary};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           LABEL VARIANTS
           ═══════════════════════════════════════════════════════════════ */
        
        QLabel#heading1 {{
            font-size: 28px;
            font-weight: 700;
            color: {p.text_primary};
        }}
        
        QLabel#heading2 {{
            font-size: 22px;
            font-weight: 600;
            color: {p.text_primary};
        }}
        
        QLabel#heading3 {{
            font-size: 18px;
            font-weight: 600;
            color: {p.text_primary};
        }}
        
        QLabel#subtitle {{
            font-size: 14px;
            color: {p.text_secondary};
        }}
        
        QLabel#caption {{
            font-size: 11px;
            color: {p.text_muted};
        }}
        
        QLabel#link {{
            color: {p.text_link};
            text-decoration: underline;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           STATUS BADGES
           ═══════════════════════════════════════════════════════════════ */
        
        QLabel#badgeSuccess {{
            background: {p.success_bg};
            color: {p.success};
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#badgeWarning {{
            background: {p.warning_bg};
            color: {p.warning};
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#badgeDanger {{
            background: {p.danger_bg};
            color: {p.danger};
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#badgeInfo {{
            background: {p.info_bg};
            color: {p.info};
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#badgePrimary {{
            background: {p.primary_light};
            color: {p.primary};
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#badgeNeutral {{
            background: {p.background_secondary};
            color: {p.text_secondary};
            padding: 6px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           CARDS
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#card {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 16px;
        }}
        
        QFrame#cardElevated {{
            background: {p.surface_elevated};
            border: none;
            border-radius: 16px;
        }}
        
        QFrame#cardHover:hover {{
            border-color: {p.primary};
            background: {p.surface_hover};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           LOADING OVERLAY
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#loadingOverlay {{
            background: {p.overlay_dark};
            border-radius: 16px;
        }}
        
        QLabel#loadingText {{
            color: {p.text_inverse};
            font-size: 14px;
            font-weight: 600;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TOAST NOTIFICATIONS
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#toastSuccess {{
            background: {p.success_bg};
            border: 1px solid {p.success_border};
            border-radius: 12px;
            border-left: 4px solid {p.success};
        }}
        
        QFrame#toastWarning {{
            background: {p.warning_bg};
            border: 1px solid {p.warning_border};
            border-radius: 12px;
            border-left: 4px solid {p.warning};
        }}
        
        QFrame#toastDanger {{
            background: {p.danger_bg};
            border: 1px solid {p.danger_border};
            border-radius: 12px;
            border-left: 4px solid {p.danger};
        }}
        
        QFrame#toastInfo {{
            background: {p.info_bg};
            border: 1px solid {p.info_border};
            border-radius: 12px;
            border-left: 4px solid {p.info};
        }}
        """
    
    def get_chart_colors(self) -> list[str]:
        """Get chart color palette"""
        p = self._palette
        return [p.chart_1, p.chart_2, p.chart_3, p.chart_4, p.chart_5, p.chart_6]


# Global instance
theme_manager = ThemeManager()


def get_theme() -> ThemeManager:
    """Get global theme manager instance"""
    return theme_manager