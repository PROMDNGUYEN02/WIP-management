# wip_management/presentation/ui/charts.py
"""
Chart Components for Data Visualization
"""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from PySide6.QtCore import QPointF, QRectF, Qt, QTimer, Signal
from PySide6.QtGui import (
    QBrush, QColor, QFont, QFontMetrics, QLinearGradient,
    QPainter, QPainterPath, QPen
)
from PySide6.QtWidgets import QFrame, QSizePolicy, QToolTip, QWidget

from wip_management.presentation.ui.theme import get_theme


class DonutChart(QWidget):
    """Animated donut/pie chart"""
    
    segment_clicked = Signal(str, float)  # (label, value)
    
    def __init__(
        self,
        data: list[tuple[str, float, str]] | None = None,
        show_legend: bool = True,
        show_center_text: bool = True,
        thickness: float = 0.3,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._data = data or []  # [(label, value, color), ...]
        self._show_legend = show_legend
        self._show_center_text = show_center_text
        self._thickness = thickness
        self._hovered_index = -1
        self._animation_progress = 0.0
        
        self.setMinimumSize(200, 200)
        self.setMouseTracking(True)
        
        self._anim_timer = QTimer(self)
        self._anim_timer.timeout.connect(self._animate)
    
    def set_data(self, data: list[tuple[str, float, str]]) -> None:
        self._data = data
        self._animation_progress = 0.0
        self._anim_timer.start(16)
    
    def _animate(self) -> None:
        self._animation_progress += 0.05
        if self._animation_progress >= 1.0:
            self._animation_progress = 1.0
            self._anim_timer.stop()
        self.update()
    
    def paintEvent(self, event) -> None:
        if not self._data:
            return
        
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        rect = self.rect()
        size = min(rect.width(), rect.height())
        
        # Calculate chart rect
        chart_size = size - 40 if self._show_legend else size - 20
        chart_rect = QRectF(
            (rect.width() - chart_size) / 2,
            10,
            chart_size,
            chart_size
        )
        
        # Draw segments
        total = sum(v for _, v, _ in self._data)
        if total == 0:
            return
        
        start_angle = 90 * 16  # Start from top
        outer_rect = chart_rect
        thickness = chart_size * self._thickness
        inner_rect = chart_rect.adjusted(thickness, thickness, -thickness, -thickness)
        
        for i, (label, value, color) in enumerate(self._data):
            span_angle = int((value / total) * 360 * 16 * self._animation_progress)
            
            # Highlight hovered segment
            segment_rect = outer_rect
            if i == self._hovered_index:
                segment_rect = segment_rect.adjusted(-5, -5, 5, 5)
            
            # Draw arc
            path = QPainterPath()
            path.arcMoveTo(segment_rect, start_angle / 16)
            path.arcTo(segment_rect, start_angle / 16, span_angle / 16)
            path.arcTo(inner_rect, (start_angle + span_angle) / 16, -span_angle / 16)
            path.closeSubpath()
            
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(color))
            painter.drawPath(path)
            
            start_angle += span_angle
        
        # Center text
        if self._show_center_text:
            painter.setPen(QColor(get_theme().palette.text_primary))
            painter.setFont(QFont("Segoe UI", 18, QFont.Weight.Bold))
            painter.drawText(inner_rect, Qt.AlignmentFlag.AlignCenter, f"{int(total):,}")
        
        # Legend
        if self._show_legend:
            self._draw_legend(painter, rect, chart_rect)
    
    def _draw_legend(self, painter: QPainter, rect: QRectF, chart_rect: QRectF) -> None:
        p = get_theme().palette
        legend_y = chart_rect.bottom() + 20
        legend_x = 20
        
        painter.setFont(QFont("Segoe UI", 10))
        
        for i, (label, value, color) in enumerate(self._data):
            # Color box
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(QColor(color))
            painter.drawRect(QRectF(legend_x, legend_y, 12, 12))
            
            # Label
            painter.setPen(QColor(p.text_secondary))
            text = f"{label}: {int(value):,}"
            painter.drawText(
                QRectF(legend_x + 18, legend_y - 2, 200, 16),
                Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter,
                text
            )
            
            legend_x += 120
            if legend_x > rect.width() - 120:
                legend_x = 20
                legend_y += 20
    
    def mouseMoveEvent(self, event) -> None:
        # Hit test for hover effects
        pos = event.position()
        # Simple center distance check
        center = QPointF(self.width() / 2, (self.height() - 40) / 2 + 10)
        dist = ((pos.x() - center.x())**2 + (pos.y() - center.y())**2)**0.5
        
        size = min(self.width(), self.height()) - 40
        outer_r = size / 2
        inner_r = outer_r * (1 - self._thickness)
        
        if inner_r < dist < outer_r:
            # Calculate angle
            import math
            angle = math.degrees(math.atan2(center.y() - pos.y(), pos.x() - center.x()))
            angle = (90 - angle) % 360
            
            # Find segment
            total = sum(v for _, v, _ in self._data)
            cumulative = 0
            for i, (_, value, _) in enumerate(self._data):
                cumulative += (value / total) * 360
                if angle < cumulative:
                    if self._hovered_index != i:
                        self._hovered_index = i
                        self.update()
                    break
        else:
            if self._hovered_index != -1:
                self._hovered_index = -1
                self.update()
        
        super().mouseMoveEvent(event)
    
    def leaveEvent(self, event) -> None:
        self._hovered_index = -1
        self.update()
        super().leaveEvent(event)


class BarChart(QWidget):
    """Animated bar chart"""
    
    bar_clicked = Signal(str, float)
    
    def __init__(
        self,
        data: list[tuple[str, float]] | None = None,
        color: str | None = None,
        show_values: bool = True,
        horizontal: bool = False,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._data = data or []
        self._color = color or get_theme().palette.primary
        self._show_values = show_values
        self._horizontal = horizontal
        self._animation_progress = 0.0
        self._hovered_index = -1
        
        self.setMinimumSize(200, 150)
        self.setMouseTracking(True)
        
        self._anim_timer = QTimer(self)
        self._anim_timer.timeout.connect(self._animate)
    
    def set_data(self, data: list[tuple[str, float]]) -> None:
        self._data = data
        self._animation_progress = 0.0
        self._anim_timer.start(16)
    
    def _animate(self) -> None:
        self._animation_progress += 0.08
        if self._animation_progress >= 1.0:
            self._animation_progress = 1.0
            self._anim_timer.stop()
        self.update()
    
    def paintEvent(self, event) -> None:
        if not self._data:
            return
        
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        p = get_theme().palette
        rect = self.rect()
        
        # Margins
        margin = 50
        chart_rect = rect.adjusted(margin, 20, -20, -40)
        
        max_value = max(v for _, v in self._data)
        if max_value == 0:
            return
        
        bar_count = len(self._data)
        bar_spacing = 8
        bar_width = (chart_rect.width() - (bar_count - 1) * bar_spacing) / bar_count
        
        # Draw bars
        for i, (label, value) in enumerate(self._data):
            x = chart_rect.left() + i * (bar_width + bar_spacing)
            bar_height = (value / max_value) * chart_rect.height() * self._animation_progress
            y = chart_rect.bottom() - bar_height
            
            bar_rect = QRectF(x, y, bar_width, bar_height)
            
            # Gradient
            gradient = QLinearGradient(x, y, x, chart_rect.bottom())
            color = QColor(self._color)
            if i == self._hovered_index:
                gradient.setColorAt(0, color.lighter(110))
                gradient.setColorAt(1, color)
            else:
                gradient.setColorAt(0, color)
                gradient.setColorAt(1, color.darker(110))
            
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(gradient)
            painter.drawRect(bar_rect)
            
            # Value label
            if self._show_values and self._animation_progress >= 1.0:
                painter.setPen(QColor(p.text_secondary))
                painter.setFont(QFont("Segoe UI", 9, QFont.Weight.DemiBold))
                painter.drawText(
                    QRectF(x, y - 20, bar_width, 18),
                    Qt.AlignmentFlag.AlignCenter,
                    f"{int(value):,}"
                )
            
            # X-axis label
            painter.setPen(QColor(p.text_muted))
            painter.setFont(QFont("Segoe UI", 9))
            painter.drawText(
                QRectF(x, chart_rect.bottom() + 8, bar_width, 20),
                Qt.AlignmentFlag.AlignCenter,
                label
            )
        
        # Y-axis
        painter.setPen(QColor(p.border))
        painter.drawLine(
            int(chart_rect.left() - 10), int(chart_rect.top()),
            int(chart_rect.left() - 10), int(chart_rect.bottom())
        )
        
        # Y-axis labels
        painter.setPen(QColor(p.text_muted))
        painter.setFont(QFont("Segoe UI", 9))
        for i in range(5):
            y = chart_rect.bottom() - (i / 4) * chart_rect.height()
            value = (i / 4) * max_value
            painter.drawText(
                QRectF(0, y - 8, margin - 15, 16),
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                f"{int(value):,}"
            )
    
    def mouseMoveEvent(self, event) -> None:
        if not self._data:
            return
        
        margin = 50
        chart_rect = self.rect().adjusted(margin, 20, -20, -40)
        
        bar_count = len(self._data)
        bar_spacing = 8
        bar_width = (chart_rect.width() - (bar_count - 1) * bar_spacing) / bar_count
        
        pos_x = event.position().x()
        
        for i in range(bar_count):
            x = chart_rect.left() + i * (bar_width + bar_spacing)
            if x <= pos_x <= x + bar_width:
                if self._hovered_index != i:
                    self._hovered_index = i
                    self.update()
                break
        else:
            if self._hovered_index != -1:
                self._hovered_index = -1
                self.update()
        
        super().mouseMoveEvent(event)
    
    def leaveEvent(self, event) -> None:
        self._hovered_index = -1
        self.update()
        super().leaveEvent(event)


class LineChart(QWidget):
    """Animated line chart with area fill"""
    
    def __init__(
        self,
        data: list[tuple[str, float]] | None = None,
        color: str | None = None,
        show_area: bool = True,
        show_points: bool = True,
        show_grid: bool = True,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._data = data or []
        self._color = color or get_theme().palette.primary
        self._show_area = show_area
        self._show_points = show_points
        self._show_grid = show_grid
        self._animation_progress = 0.0
        self._hovered_index = -1
        
        self.setMinimumSize(300, 150)
        self.setMouseTracking(True)
        
        self._anim_timer = QTimer(self)
        self._anim_timer.timeout.connect(self._animate)
    
    def set_data(self, data: list[tuple[str, float]]) -> None:
        self._data = data
        self._animation_progress = 0.0
        self._anim_timer.start(16)
    
    def _animate(self) -> None:
        self._animation_progress += 0.05
        if self._animation_progress >= 1.0:
            self._animation_progress = 1.0
            self._anim_timer.stop()
        self.update()
    
    def paintEvent(self, event) -> None:
        if len(self._data) < 2:
            return
        
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        p = get_theme().palette
        rect = self.rect()
        
        margin_left = 50
        margin_right = 20
        margin_top = 20
        margin_bottom = 40
        
        chart_rect = QRectF(
            margin_left, margin_top,
            rect.width() - margin_left - margin_right,
            rect.height() - margin_top - margin_bottom
        )
        
        max_value = max(v for _, v in self._data)
        min_value = min(v for _, v in self._data)
        value_range = max_value - min_value or 1
        
        # Calculate points
        points = []
        step_x = chart_rect.width() / (len(self._data) - 1)
        
        for i, (_, value) in enumerate(self._data):
            x = chart_rect.left() + i * step_x
            normalized = (value - min_value) / value_range
            animated_normalized = normalized * self._animation_progress
            y = chart_rect.bottom() - animated_normalized * chart_rect.height()
            points.append(QPointF(x, y))
        
        # Draw grid
        if self._show_grid:
            painter.setPen(QPen(QColor(p.border_light), 1, Qt.PenStyle.DashLine))
            for i in range(5):
                y = chart_rect.top() + (i / 4) * chart_rect.height()
                painter.drawLine(
                    QPointF(chart_rect.left(), y),
                    QPointF(chart_rect.right(), y)
                )
        
        # Draw area fill
        if self._show_area and points:
            area_path = QPainterPath()
            area_path.moveTo(chart_rect.left(), chart_rect.bottom())
            for point in points:
                area_path.lineTo(point)
            area_path.lineTo(chart_rect.right(), chart_rect.bottom())
            area_path.closeSubpath()
            
            gradient = QLinearGradient(0, chart_rect.top(), 0, chart_rect.bottom())
            color = QColor(self._color)
            gradient.setColorAt(0, QColor(color.red(), color.green(), color.blue(), 80))
            gradient.setColorAt(1, QColor(color.red(), color.green(), color.blue(), 10))
            
            painter.setPen(Qt.PenStyle.NoPen)
            painter.setBrush(gradient)
            painter.drawPath(area_path)
        
        # Draw line
        if points:
            line_path = QPainterPath()
            line_path.moveTo(points[0])
            for point in points[1:]:
                line_path.lineTo(point)
            
            pen = QPen(QColor(self._color))
            pen.setWidth(3)
            pen.setCapStyle(Qt.PenCapStyle.RoundCap)
            pen.setJoinStyle(Qt.PenJoinStyle.RoundJoin)
            painter.setPen(pen)
            painter.setBrush(Qt.BrushStyle.NoBrush)
            painter.drawPath(line_path)
        
        # Draw points
        if self._show_points:
            for i, point in enumerate(points):
                size = 10 if i == self._hovered_index else 8
                
                painter.setPen(QPen(QColor(p.surface), 2))
                painter.setBrush(QColor(self._color))
                painter.drawEllipse(point, size / 2, size / 2)
        
        # Y-axis labels
        painter.setPen(QColor(p.text_muted))
        painter.setFont(QFont("Segoe UI", 9))
        for i in range(5):
            y = chart_rect.bottom() - (i / 4) * chart_rect.height()
            value = min_value + (i / 4) * value_range
            painter.drawText(
                QRectF(0, y - 8, margin_left - 10, 16),
                Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter,
                f"{int(value):,}"
            )
        
        # X-axis labels
        label_step = max(1, len(self._data) // 6)
        for i in range(0, len(self._data), label_step):
            label, _ = self._data[i]
            x = chart_rect.left() + i * step_x
            painter.drawText(
                QRectF(x - 30, chart_rect.bottom() + 8, 60, 20),
                Qt.AlignmentFlag.AlignCenter,
                label
            )
    
    def mouseMoveEvent(self, event) -> None:
        if len(self._data) < 2:
            return
        
        margin_left = 50
        chart_width = self.width() - margin_left - 20
        step_x = chart_width / (len(self._data) - 1)
        
        pos_x = event.position().x() - margin_left
        
        closest = -1
        min_dist = float('inf')
        
        for i in range(len(self._data)):
            x = i * step_x
            dist = abs(pos_x - x)
            if dist < min_dist and dist < 20:
                min_dist = dist
                closest = i
        
        if self._hovered_index != closest:
            self._hovered_index = closest
            self.update()
            
            if closest >= 0:
                label, value = self._data[closest]
                QToolTip.showText(
                    event.globalPosition().toPoint(),
                    f"{label}: {value:,.0f}"
                )
        
        super().mouseMoveEvent(event)
    
    def leaveEvent(self, event) -> None:
        self._hovered_index = -1
        self.update()
        super().leaveEvent(event)


class GaugeChart(QWidget):
    """Gauge/speedometer chart"""
    
    def __init__(
        self,
        value: float = 0,
        max_value: float = 100,
        label: str = "",
        color: str | None = None,
        parent: QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self._value = value
        self._max_value = max_value
        self._label = label
        self._color = color or get_theme().palette.primary
        self._displayed_value = 0.0
        
        self.setMinimumSize(150, 100)
        
        self._anim_timer = QTimer(self)
        self._anim_timer.timeout.connect(self._animate)
    
    def set_value(self, value: float) -> None:
        self._value = min(value, self._max_value)
        self._anim_timer.start(16)
    
    def _animate(self) -> None:
        diff = self._value - self._displayed_value
        step = diff * 0.15
        
        if abs(diff) < 0.5:
            self._displayed_value = self._value
            self._anim_timer.stop()
        else:
            self._displayed_value += step
        
        self.update()
    
    def paintEvent(self, event) -> None:
        painter = QPainter(self)
        painter.setRenderHint(QPainter.RenderHint.Antialiasing)
        
        p = get_theme().palette
        rect = self.rect()
        
        # Center and size
        size = min(rect.width(), rect.height() * 1.5) - 20
        center = QPointF(rect.width() / 2, rect.height() - 20)
        
        # Arc parameters
        start_angle = 180
        span_angle = 180
        
        arc_rect = QRectF(
            center.x() - size / 2,
            center.y() - size / 2,
            size,
            size
        )
        
        # Background arc
        painter.setPen(QPen(QColor(p.background_secondary), 12, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
        painter.drawArc(arc_rect, start_angle * 16, span_angle * 16)
        
        # Value arc
        progress = self._displayed_value / self._max_value
        value_span = span_angle * progress
        
        gradient = QLinearGradient(arc_rect.left(), 0, arc_rect.right(), 0)
        color = QColor(self._color)
        gradient.setColorAt(0, color.lighter(120))
        gradient.setColorAt(1, color)
        
        painter.setPen(QPen(QBrush(gradient), 12, Qt.PenStyle.SolidLine, Qt.PenCapStyle.RoundCap))
        painter.drawArc(arc_rect, start_angle * 16, int(value_span * 16))
        
        # Value text
        painter.setPen(QColor(p.text_primary))
        painter.setFont(QFont("Segoe UI", 24, QFont.Weight.Bold))
        painter.drawText(
            QRectF(0, center.y() - 50, rect.width(), 40),
            Qt.AlignmentFlag.AlignCenter,
            f"{int(self._displayed_value):,}"
        )
        
        # Label
        if self._label:
            painter.setPen(QColor(p.text_muted))
            painter.setFont(QFont("Segoe UI", 10))
            painter.drawText(
                QRectF(0, center.y() - 15, rect.width(), 20),
                Qt.AlignmentFlag.AlignCenter,
                self._label
            )
