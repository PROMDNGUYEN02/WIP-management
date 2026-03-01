# wip_management/presentation/ui/theme.py
"""
Modern Theme System for WIP Management
Supports Dark/Light mode with smooth transitions
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any

from PySide6.QtCore import QObject, Signal
from PySide6.QtGui import QColor, QPalette
from PySide6.QtWidgets import QApplication


class ThemeMode(Enum):
    LIGHT = "light"
    DARK = "dark"
    AUTO = "auto"


@dataclass(frozen=True)
class ColorPalette:
    # Primary colors
    primary: str
    primary_hover: str
    primary_pressed: str
    primary_light: str
    
    # Status colors
    success: str
    success_bg: str
    warning: str
    warning_bg: str
    danger: str
    danger_bg: str
    info: str
    info_bg: str
    
    # Neutral colors
    background: str
    surface: str
    surface_hover: str
    border: str
    border_light: str
    
    # Text colors
    text_primary: str
    text_secondary: str
    text_muted: str
    text_inverse: str
    
    # Shadows
    shadow_sm: str
    shadow_md: str
    shadow_lg: str
    
    # Aging status colors
    aging_waiting: str
    aging_ready: str
    aging_exceed: str


LIGHT_PALETTE = ColorPalette(
    # Primary
    primary="#2563eb",
    primary_hover="#1d4ed8",
    primary_pressed="#1e40af",
    primary_light="#dbeafe",
    
    # Status
    success="#16a34a",
    success_bg="#dcfce7",
    warning="#d97706",
    warning_bg="#fef3c7",
    danger="#dc2626",
    danger_bg="#fee2e2",
    info="#0891b2",
    info_bg="#cffafe",
    
    # Neutral
    background="#f8fafc",
    surface="#ffffff",
    surface_hover="#f1f5f9",
    border="#e2e8f0",
    border_light="#f1f5f9",
    
    # Text
    text_primary="#0f172a",
    text_secondary="#475569",
    text_muted="#94a3b8",
    text_inverse="#ffffff",
    
    # Shadows
    shadow_sm="rgba(0, 0, 0, 0.05)",
    shadow_md="rgba(0, 0, 0, 0.1)",
    shadow_lg="rgba(0, 0, 0, 0.15)",
    
    # Aging
    aging_waiting="#fef3c7",
    aging_ready="#dcfce7",
    aging_exceed="#fee2e2",
)


DARK_PALETTE = ColorPalette(
    # Primary
    primary="#3b82f6",
    primary_hover="#60a5fa",
    primary_pressed="#2563eb",
    primary_light="#1e3a5f",
    
    # Status
    success="#22c55e",
    success_bg="#14532d",
    warning="#f59e0b",
    warning_bg="#451a03",
    danger="#ef4444",
    danger_bg="#450a0a",
    info="#06b6d4",
    info_bg="#083344",
    
    # Neutral
    background="#0f172a",
    surface="#1e293b",
    surface_hover="#334155",
    border="#334155",
    border_light="#1e293b",
    
    # Text
    text_primary="#f8fafc",
    text_secondary="#cbd5e1",
    text_muted="#64748b",
    text_inverse="#0f172a",
    
    # Shadows
    shadow_sm="rgba(0, 0, 0, 0.3)",
    shadow_md="rgba(0, 0, 0, 0.4)",
    shadow_lg="rgba(0, 0, 0, 0.5)",
    
    # Aging
    aging_waiting="#451a03",
    aging_ready="#14532d",
    aging_exceed="#450a0a",
)


class ThemeManager(QObject):
    """Singleton theme manager with live theme switching"""
    
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
    
    @property
    def mode(self) -> ThemeMode:
        return self._mode
    
    @property
    def palette(self) -> ColorPalette:
        return self._palette
    
    @property
    def is_dark(self) -> bool:
        return self._mode == ThemeMode.DARK
    
    def set_mode(self, mode: ThemeMode) -> None:
        if mode == self._mode:
            return
        
        self._mode = mode
        self._palette = DARK_PALETTE if mode == ThemeMode.DARK else LIGHT_PALETTE
        self.theme_changed.emit(mode.value)
    
    def toggle(self) -> None:
        new_mode = ThemeMode.LIGHT if self._mode == ThemeMode.DARK else ThemeMode.DARK
        self.set_mode(new_mode)
    
    def get_stylesheet(self) -> str:
        """Generate complete application stylesheet"""
        p = self._palette
        return f"""
        /* ═══════════════════════════════════════════════════════════════
           GLOBAL STYLES
           ═══════════════════════════════════════════════════════════════ */
        
        QMainWindow {{
            background: {p.background};
        }}
        
        QWidget {{
            font-family: 'Segoe UI', 'SF Pro Display', -apple-system, sans-serif;
            font-size: 13px;
            color: {p.text_primary};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           METRIC CARDS
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#metricCard {{
            background: {p.surface};
            border: 1px solid {p.border};
            border-radius: 16px;
            padding: 16px;
        }}
        
        QFrame#metricCard:hover {{
            border-color: {p.primary};
            background: {p.surface_hover};
        }}
        
        QLabel#metricIcon {{
            font-size: 24px;
        }}
        
        QLabel#metricValue {{
            color: {p.text_primary};
            font-size: 28px;
            font-weight: 700;
            letter-spacing: -0.5px;
        }}
        
        QLabel#metricTitle {{
            color: {p.text_secondary};
            font-size: 12px;
            font-weight: 500;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        
        QLabel#metricTrend {{
            font-size: 11px;
            font-weight: 600;
        }}
        
        QLabel#metricTrendUp {{
            color: {p.success};
        }}
        
        QLabel#metricTrendDown {{
            color: {p.danger};
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
            font-size: 16px;
            font-weight: 600;
            padding: 16px 16px 8px 16px;
        }}
        
        QLabel#columnStats {{
            color: {p.text_muted};
            font-size: 12px;
            padding: 0 16px 12px 16px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           BUTTONS
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
        
        QPushButton#secondaryButton {{
            background: {p.surface};
            color: {p.text_primary};
            border: 1px solid {p.border};
        }}
        
        QPushButton#secondaryButton:hover {{
            background: {p.surface_hover};
            border-color: {p.primary};
        }}
        
        QPushButton#dangerButton {{
            background: {p.danger};
        }}
        
        QPushButton#dangerButton:hover {{
            background: #b91c1c;
        }}
        
        QPushButton#successButton {{
            background: {p.success};
        }}
        
        QPushButton#iconButton {{
            background: transparent;
            border: none;
            padding: 8px;
            border-radius: 8px;
            min-width: 36px;
            max-width: 36px;
            min-height: 36px;
            max-height: 36px;
        }}
        
        QPushButton#iconButton:hover {{
            background: {p.surface_hover};
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
        }}
        
        QLineEdit:focus {{
            border-color: {p.primary};
            outline: none;
        }}
        
        QLineEdit:disabled {{
            background: {p.border_light};
            color: {p.text_muted};
        }}
        
        QLineEdit::placeholder {{
            color: {p.text_muted};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           CHECKBOXES
           ═══════════════════════════════════════════════════════════════ */
        
        QCheckBox {{
            spacing: 8px;
            color: {p.text_primary};
            font-weight: 500;
        }}
        
        QCheckBox::indicator {{
            width: 20px;
            height: 20px;
            border-radius: 6px;
            border: 2px solid {p.border};
            background: {p.surface};
        }}
        
        QCheckBox::indicator:hover {{
            border-color: {p.primary};
        }}
        
        QCheckBox::indicator:checked {{
            background: {p.primary};
            border-color: {p.primary};
            image: url(check-white.svg);
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
            border-radius: 10px;
            padding: 12px 14px;
            margin: 3px 8px;
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
        
        /* ═══════════════════════════════════════════════════════════════
           TABLE VIEWS
           ═══════════════════════════════════════════════════════════════ */
        
        QTableView {{
            background: transparent;
            border: none;
            gridline-color: {p.border_light};
            selection-background-color: {p.primary_light};
            selection-color: {p.text_primary};
        }}
        
        QTableView::item {{
            padding: 8px 12px;
            border: none;
        }}
        
        QTableView::item:hover {{
            background: {p.surface_hover};
        }}
        
        QTableView::item:selected {{
            background: {p.primary_light};
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
            padding: 12px 8px;
            border: none;
            border-bottom: 2px solid {p.border};
        }}
        
        QHeaderView::section:hover {{
            background: {p.surface_hover};
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           SCROLLBARS
           ═══════════════════════════════════════════════════════════════ */
        
        QScrollBar:vertical {{
            background: transparent;
            width: 10px;
            margin: 0;
        }}
        
        QScrollBar::handle:vertical {{
            background: {p.border};
            border-radius: 5px;
            min-height: 30px;
            margin: 2px;
        }}
        
        QScrollBar::handle:vertical:hover {{
            background: {p.text_muted};
        }}
        
        QScrollBar::add-line:vertical,
        QScrollBar::sub-line:vertical {{
            height: 0;
        }}
        
        QScrollBar:horizontal {{
            background: transparent;
            height: 10px;
            margin: 0;
        }}
        
        QScrollBar::handle:horizontal {{
            background: {p.border};
            border-radius: 5px;
            min-width: 30px;
            margin: 2px;
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
            padding: 8px 16px;
        }}
        
        QStatusBar QLabel {{
            color: {p.text_secondary};
            font-size: 12px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           TOOLTIPS
           ═══════════════════════════════════════════════════════════════ */
        
        QToolTip {{
            background: {p.text_primary};
            color: {p.text_inverse};
            border: none;
            border-radius: 8px;
            padding: 8px 12px;
            font-size: 12px;
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
            padding: 10px 32px 10px 16px;
            border-radius: 6px;
            color: {p.text_primary};
        }}
        
        QMenu::item:selected {{
            background: {p.surface_hover};
        }}
        
        QMenu::separator {{
            height: 1px;
            background: {p.border};
            margin: 6px 12px;
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
            border-radius: 8px;
            padding: 8px 12px;
            color: {p.text_primary};
        }}
        
        QSpinBox:focus, QDoubleSpinBox:focus {{
            border-color: {p.primary};
        }}
        
        QSpinBox::up-button, QDoubleSpinBox::up-button,
        QSpinBox::down-button, QDoubleSpinBox::down-button {{
            border: none;
            background: transparent;
            width: 20px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           AGING STATUS BADGES
           ═══════════════════════════════════════════════════════════════ */
        
        QLabel#agingWaiting {{
            background: {p.aging_waiting};
            color: {p.warning};
            padding: 4px 10px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#agingReady {{
            background: {p.aging_ready};
            color: {p.success};
            padding: 4px 10px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        QLabel#agingExceed {{
            background: {p.aging_exceed};
            color: {p.danger};
            padding: 4px 10px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 11px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           PROGRESS INDICATORS
           ═══════════════════════════════════════════════════════════════ */
        
        QProgressBar {{
            background: {p.border_light};
            border: none;
            border-radius: 4px;
            height: 8px;
            text-align: center;
        }}
        
        QProgressBar::chunk {{
            background: {p.primary};
            border-radius: 4px;
        }}
        
        /* ═══════════════════════════════════════════════════════════════
           LOADING OVERLAY
           ═══════════════════════════════════════════════════════════════ */
        
        QFrame#loadingOverlay {{
            background: rgba(15, 23, 42, 0.7);
            border-radius: 16px;
        }}
        
        QLabel#loadingText {{
            color: white;
            font-size: 14px;
            font-weight: 500;
        }}
        """


# Global instance
theme = ThemeManager()