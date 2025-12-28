"""
대시보드 위젯 컴포넌트
- 요약 카드
- 차트 위젯
- 테이블 위젯
"""
from PyQt5.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLabel, QFrame,
    QTableWidget, QTableWidgetItem, QHeaderView, QSizePolicy
)
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QFont, QColor

import matplotlib
matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import pandas as pd
from typing import Dict, List, Optional


class SummaryCard(QFrame):
    """요약 정보 카드 위젯"""

    def __init__(self, title: str, value: str, subtitle: str = "",
                 color: str = "#3498db", parent=None):
        super().__init__(parent)
        self.color = color
        self.initUI(title, value, subtitle)

    def initUI(self, title: str, value: str, subtitle: str):
        self.setFrameShape(QFrame.StyledPanel)
        self.setStyleSheet(f"""
            QFrame {{
                background-color: white;
                border: 2px solid {self.color};
                border-radius: 10px;
                padding: 10px;
            }}
        """)
        self.setMinimumSize(180, 120)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)

        layout = QVBoxLayout(self)
        layout.setSpacing(5)

        # 타이틀
        self.title_label = QLabel(title)
        self.title_label.setStyleSheet("color: #7f8c8d; font-size: 12px; border: none;")
        self.title_label.setAlignment(Qt.AlignCenter)

        # 값
        self.value_label = QLabel(value)
        self.value_label.setStyleSheet(f"color: {self.color}; font-size: 28px; font-weight: bold; border: none;")
        self.value_label.setAlignment(Qt.AlignCenter)

        # 부제목 (전년대비 등)
        self.subtitle_label = QLabel(subtitle)
        self.subtitle_label.setStyleSheet("color: #95a5a6; font-size: 11px; border: none;")
        self.subtitle_label.setAlignment(Qt.AlignCenter)

        layout.addWidget(self.title_label)
        layout.addWidget(self.value_label)
        layout.addWidget(self.subtitle_label)

    def update_value(self, value: str, subtitle: str = ""):
        """값 업데이트"""
        self.value_label.setText(value)
        self.subtitle_label.setText(subtitle)

    def set_status_color(self, status: str):
        """상태에 따른 색상 변경"""
        colors = {
            'good': '#2ecc71',
            'warning': '#f39c12',
            'danger': '#e74c3c',
            'normal': '#3498db',
        }
        self.color = colors.get(status, '#3498db')
        self.value_label.setStyleSheet(
            f"color: {self.color}; font-size: 28px; font-weight: bold; border: none;"
        )


class ChartWidget(QWidget):
    """Matplotlib 차트 위젯"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.figure = Figure(figsize=(8, 5), dpi=100)
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.canvas)

        # 한글 폰트 설정
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False

    def clear(self):
        """차트 초기화"""
        self.figure.clear()

    def plot_bar(self, data: Dict[str, float], title: str = "",
                 xlabel: str = "", ylabel: str = "", color: str = '#3498db'):
        """막대 그래프"""
        self.clear()
        ax = self.figure.add_subplot(111)

        if not data:
            ax.text(0.5, 0.5, '데이터 없음', ha='center', va='center')
            self.canvas.draw()
            return

        x = list(data.keys())
        y = list(data.values())

        bars = ax.bar(x, y, color=color, alpha=0.8)

        # 값 표시
        for bar, val in zip(bars, y):
            height = bar.get_height()
            ax.annotate(f'{val:,.0f}',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3), textcoords="offset points",
                       ha='center', va='bottom', fontsize=9)

        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.tick_params(axis='x', rotation=45)
        self.figure.tight_layout()
        self.canvas.draw()

    def plot_line(self, data: Dict[str, float], title: str = "",
                  xlabel: str = "", ylabel: str = "", color: str = '#3498db'):
        """선 그래프"""
        self.clear()
        ax = self.figure.add_subplot(111)

        if not data:
            ax.text(0.5, 0.5, '데이터 없음', ha='center', va='center')
            self.canvas.draw()
            return

        x = list(data.keys())
        y = list(data.values())

        ax.plot(x, y, marker='o', linewidth=2, markersize=6, color=color)
        ax.fill_between(x, y, alpha=0.3, color=color)

        # 값 표시
        for i, (xi, yi) in enumerate(zip(x, y)):
            ax.annotate(f'{yi:,.0f}',
                       xy=(xi, yi), xytext=(0, 10),
                       textcoords="offset points",
                       ha='center', fontsize=9)

        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)
        ax.tick_params(axis='x', rotation=45)
        ax.grid(True, alpha=0.3)
        self.figure.tight_layout()
        self.canvas.draw()

    def plot_pie(self, data: Dict[str, float], title: str = ""):
        """파이 차트"""
        self.clear()
        ax = self.figure.add_subplot(111)

        if not data:
            ax.text(0.5, 0.5, '데이터 없음', ha='center', va='center')
            self.canvas.draw()
            return

        labels = list(data.keys())
        sizes = list(data.values())
        colors = plt.cm.Pastel1.colors[:len(labels)]

        wedges, texts, autotexts = ax.pie(
            sizes, labels=labels, autopct='%1.1f%%',
            colors=colors, startangle=90,
            pctdistance=0.75
        )

        ax.set_title(title, fontsize=12, fontweight='bold')
        self.figure.tight_layout()
        self.canvas.draw()

    def plot_horizontal_bar(self, data: Dict[str, float], title: str = "",
                           color: str = '#3498db'):
        """수평 막대 그래프 (담당자별 등)"""
        self.clear()
        ax = self.figure.add_subplot(111)

        if not data:
            ax.text(0.5, 0.5, '데이터 없음', ha='center', va='center')
            self.canvas.draw()
            return

        y = list(data.keys())
        x = list(data.values())

        bars = ax.barh(y, x, color=color, alpha=0.8)

        # 값 표시
        for bar, val in zip(bars, x):
            width = bar.get_width()
            ax.annotate(f'{val:,.0f}',
                       xy=(width, bar.get_y() + bar.get_height() / 2),
                       xytext=(3, 0), textcoords="offset points",
                       ha='left', va='center', fontsize=9)

        ax.set_title(title, fontsize=12, fontweight='bold')
        ax.invert_yaxis()
        self.figure.tight_layout()
        self.canvas.draw()

    def save_chart(self, filepath: str, dpi: int = 150):
        """차트를 이미지로 저장"""
        self.figure.savefig(filepath, dpi=dpi, bbox_inches='tight')


class DataTableWidget(QTableWidget):
    """데이터 테이블 위젯"""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setAlternatingRowColors(True)
        self.setStyleSheet("""
            QTableWidget {
                background-color: white;
                gridline-color: #ecf0f1;
            }
            QTableWidget::item {
                padding: 8px;
            }
            QTableWidget::item:selected {
                background-color: #3498db;
                color: white;
            }
            QHeaderView::section {
                background-color: #34495e;
                color: white;
                padding: 8px;
                border: none;
            }
        """)
        self.horizontalHeader().setStretchLastSection(True)
        self.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.setSelectionBehavior(QTableWidget.SelectRows)
        self.setEditTriggers(QTableWidget.NoEditTriggers)

    def load_dataframe(self, df: pd.DataFrame, highlight_negative: bool = False):
        """DataFrame을 테이블에 로드"""
        if df is None or df.empty:
            self.setRowCount(0)
            self.setColumnCount(0)
            return

        self.setRowCount(len(df))
        self.setColumnCount(len(df.columns))
        self.setHorizontalHeaderLabels([str(col) for col in df.columns])

        for row_idx, row in df.iterrows():
            for col_idx, value in enumerate(row):
                item = QTableWidgetItem(self._format_value(value))
                item.setTextAlignment(Qt.AlignCenter)

                # 음수 값 하이라이트
                if highlight_negative and isinstance(value, (int, float)) and value < 0:
                    item.setForeground(QColor('#e74c3c'))
                    item.setFont(QFont('Malgun Gothic', weight=QFont.Bold))

                self.setItem(row_idx if isinstance(row_idx, int) else 0, col_idx, item)

        self.resizeColumnsToContents()

    def _format_value(self, value) -> str:
        """값을 문자열로 포맷"""
        if pd.isna(value):
            return "-"
        elif isinstance(value, float):
            if abs(value) >= 10000:
                return f"{value:,.0f}"
            else:
                return f"{value:.2f}"
        else:
            return str(value)


class KPIGaugeWidget(QWidget):
    """KPI 게이지 위젯"""

    def __init__(self, title: str = "달성률", parent=None):
        super().__init__(parent)
        self.title = title
        self.value = 0
        self.target = 100
        self.initUI()

    def initUI(self):
        layout = QVBoxLayout(self)

        self.title_label = QLabel(self.title)
        self.title_label.setAlignment(Qt.AlignCenter)
        self.title_label.setStyleSheet("font-weight: bold; font-size: 14px;")

        self.value_label = QLabel("0%")
        self.value_label.setAlignment(Qt.AlignCenter)
        self.value_label.setStyleSheet("font-size: 36px; font-weight: bold; color: #3498db;")

        self.status_label = QLabel("")
        self.status_label.setAlignment(Qt.AlignCenter)
        self.status_label.setStyleSheet("font-size: 12px;")

        layout.addWidget(self.title_label)
        layout.addWidget(self.value_label)
        layout.addWidget(self.status_label)

    def set_value(self, value: float, target: float = 100):
        """값 설정"""
        self.value = value
        self.target = target
        percentage = (value / target * 100) if target > 0 else 0

        self.value_label.setText(f"{percentage:.1f}%")

        # 상태에 따른 색상
        if percentage >= 120:
            color = '#27ae60'
            status = '우수'
        elif percentage >= 100:
            color = '#2ecc71'
            status = '달성'
        elif percentage >= 80:
            color = '#f39c12'
            status = '주의'
        else:
            color = '#e74c3c'
            status = '미달'

        self.value_label.setStyleSheet(f"font-size: 36px; font-weight: bold; color: {color};")
        self.status_label.setText(f"목표: {target:,.0f} / 실적: {value:,.0f} ({status})")
