"""
경영지표 분석기 메인 윈도우
"""
import sys
from pathlib import Path
from PyQt5.QtWidgets import (
    QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QTabWidget,
    QPushButton, QLabel, QFileDialog, QMessageBox, QFrame,
    QScrollArea, QSplitter, QProgressBar, QStatusBar, QGroupBox,
    QComboBox, QDateEdit, QLineEdit, QSpinBox, QFormLayout
)
from PyQt5.QtCore import Qt, QDate, QThread, pyqtSignal
from PyQt5.QtGui import QFont, QIcon

import pandas as pd
from typing import Optional, Dict

# 모듈 임포트
sys.path.insert(0, str(Path(__file__).parent.parent))
from modules.data_loader import DataLoader
from modules.data_processor import DataProcessor
from modules.metrics_calculator import MetricsCalculator
from modules.report_generator import ReportGenerator
from config.settings import COLUMN_MAPPING, REPORTS_DIR

from .dashboard_widgets import SummaryCard, ChartWidget, DataTableWidget, KPIGaugeWidget


class DataLoadThread(QThread):
    """데이터 로딩 스레드"""
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)

    def __init__(self, file_path: str):
        super().__init__()
        self.file_path = file_path

    def run(self):
        try:
            self.progress.emit(20)
            loader = DataLoader(column_mapping=COLUMN_MAPPING)
            df = loader.load_excel(self.file_path)

            self.progress.emit(50)
            processor = DataProcessor()
            df = processor.process(df)

            self.progress.emit(100)
            self.finished.emit(df)
        except Exception as e:
            self.error.emit(str(e))


class MainWindow(QMainWindow):
    """메인 윈도우"""

    def __init__(self):
        super().__init__()
        self.df: Optional[pd.DataFrame] = None
        self.metrics: Optional[Dict] = None
        self.targets: Dict = {}

        self.initUI()

    def initUI(self):
        self.setWindowTitle("경영지표 분석기 (Business Metrics Analyzer)")
        self.setGeometry(100, 100, 1400, 900)
        self.setMinimumSize(1200, 800)

        # 중앙 위젯
        central_widget = QWidget()
        self.setCentralWidget(central_widget)

        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)

        # 상단 툴바
        self.create_toolbar(main_layout)

        # 탭 위젯
        self.tab_widget = QTabWidget()
        self.tab_widget.setStyleSheet("""
            QTabBar::tab {
                height: 35px;
                width: 150px;
                font-size: 13px;
            }
        """)

        # 대시보드 탭
        self.dashboard_tab = self.create_dashboard_tab()
        self.tab_widget.addTab(self.dashboard_tab, "대시보드")

        # 상세 분석 탭
        self.detail_tab = self.create_detail_tab()
        self.tab_widget.addTab(self.detail_tab, "상세 분석")

        # 설정 탭
        self.settings_tab = self.create_settings_tab()
        self.tab_widget.addTab(self.settings_tab, "설정")

        main_layout.addWidget(self.tab_widget)

        # 상태바
        self.statusBar = QStatusBar()
        self.setStatusBar(self.statusBar)
        self.statusBar.showMessage("엑셀 파일을 불러와 주세요.")

        # 프로그레스 바
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximumWidth(200)
        self.progress_bar.setVisible(False)
        self.statusBar.addPermanentWidget(self.progress_bar)

    def create_toolbar(self, layout):
        """상단 툴바 생성"""
        toolbar_frame = QFrame()
        toolbar_frame.setFrameShape(QFrame.StyledPanel)
        toolbar_frame.setStyleSheet("background-color: #34495e; border-radius: 5px;")
        toolbar_frame.setMaximumHeight(60)

        toolbar_layout = QHBoxLayout(toolbar_frame)
        toolbar_layout.setContentsMargins(15, 5, 15, 5)

        # 로고/타이틀
        title_label = QLabel("경영지표 분석기")
        title_label.setStyleSheet("color: white; font-size: 18px; font-weight: bold;")

        # 파일 불러오기 버튼
        self.load_btn = QPushButton("엑셀 불러오기")
        self.load_btn.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                padding: 8px 20px;
                border-radius: 5px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
        """)
        self.load_btn.clicked.connect(self.load_excel_file)

        # 새로고침 버튼
        self.refresh_btn = QPushButton("새로고침")
        self.refresh_btn.setStyleSheet("""
            QPushButton {
                background-color: #2ecc71;
                color: white;
                padding: 8px 20px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #27ae60;
            }
        """)
        self.refresh_btn.clicked.connect(self.refresh_data)
        self.refresh_btn.setEnabled(False)

        # Excel 보고서 버튼
        self.excel_report_btn = QPushButton("Excel 보고서")
        self.excel_report_btn.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                padding: 8px 20px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #1e8449;
            }
        """)
        self.excel_report_btn.clicked.connect(self.export_excel_report)
        self.excel_report_btn.setEnabled(False)

        # PDF 보고서 버튼
        self.pdf_report_btn = QPushButton("PDF 보고서")
        self.pdf_report_btn.setStyleSheet("""
            QPushButton {
                background-color: #e74c3c;
                color: white;
                padding: 8px 20px;
                border-radius: 5px;
            }
            QPushButton:hover {
                background-color: #c0392b;
            }
        """)
        self.pdf_report_btn.clicked.connect(self.export_pdf_report)
        self.pdf_report_btn.setEnabled(False)

        # 파일 정보 레이블
        self.file_info_label = QLabel("")
        self.file_info_label.setStyleSheet("color: #bdc3c7; font-size: 12px;")

        toolbar_layout.addWidget(title_label)
        toolbar_layout.addSpacing(30)
        toolbar_layout.addWidget(self.load_btn)
        toolbar_layout.addWidget(self.refresh_btn)
        toolbar_layout.addSpacing(20)
        toolbar_layout.addWidget(self.excel_report_btn)
        toolbar_layout.addWidget(self.pdf_report_btn)
        toolbar_layout.addStretch()
        toolbar_layout.addWidget(self.file_info_label)

        layout.addWidget(toolbar_frame)

    def create_dashboard_tab(self) -> QWidget:
        """대시보드 탭 생성"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 스크롤 영역
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setFrameShape(QFrame.NoFrame)

        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setSpacing(15)

        # 1. 상단 요약 카드
        cards_layout = QHBoxLayout()
        cards_layout.setSpacing(15)

        self.total_sales_card = SummaryCard("총 매출액", "-", "", "#3498db")
        self.total_profit_card = SummaryCard("총 이익", "-", "", "#2ecc71")
        self.profit_margin_card = SummaryCard("이익률", "-", "", "#9b59b6")
        self.transaction_card = SummaryCard("거래 건수", "-", "", "#e67e22")

        cards_layout.addWidget(self.total_sales_card)
        cards_layout.addWidget(self.total_profit_card)
        cards_layout.addWidget(self.profit_margin_card)
        cards_layout.addWidget(self.transaction_card)

        scroll_layout.addLayout(cards_layout)

        # 2. 차트 영역
        charts_layout = QHBoxLayout()
        charts_layout.setSpacing(15)

        # 월별 매출 추이 차트
        monthly_group = QGroupBox("월별 매출 추이")
        monthly_layout = QVBoxLayout(monthly_group)
        self.monthly_chart = ChartWidget()
        monthly_layout.addWidget(self.monthly_chart)

        # 센터별 비중 차트
        center_group = QGroupBox("센터별 비중")
        center_layout = QVBoxLayout(center_group)
        self.center_chart = ChartWidget()
        center_layout.addWidget(self.center_chart)

        charts_layout.addWidget(monthly_group, 2)
        charts_layout.addWidget(center_group, 1)

        scroll_layout.addLayout(charts_layout)

        # 3. 하단 테이블
        tables_layout = QHBoxLayout()
        tables_layout.setSpacing(15)

        # 담당자별 실적
        manager_group = QGroupBox("담당자별 실적")
        manager_layout = QVBoxLayout(manager_group)
        self.manager_table = DataTableWidget()
        manager_layout.addWidget(self.manager_table)

        # 전년 대비
        yoy_group = QGroupBox("전년 대비")
        yoy_layout = QVBoxLayout(yoy_group)
        self.yoy_table = DataTableWidget()
        yoy_layout.addWidget(self.yoy_table)

        tables_layout.addWidget(manager_group)
        tables_layout.addWidget(yoy_group)

        scroll_layout.addLayout(tables_layout)

        scroll.setWidget(scroll_content)
        layout.addWidget(scroll)

        return tab

    def create_detail_tab(self) -> QWidget:
        """상세 분석 탭"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 필터 영역
        filter_frame = QFrame()
        filter_frame.setFrameShape(QFrame.StyledPanel)
        filter_layout = QHBoxLayout(filter_frame)

        filter_layout.addWidget(QLabel("분석 유형:"))
        self.analysis_type_combo = QComboBox()
        self.analysis_type_combo.addItems([
            "담당자별 분석", "센터별 분석", "검사목적별 분석",
            "월별 추이", "분기별 추이", "전년 대비"
        ])
        self.analysis_type_combo.currentTextChanged.connect(self.update_detail_analysis)
        filter_layout.addWidget(self.analysis_type_combo)

        filter_layout.addStretch()
        layout.addWidget(filter_frame)

        # 차트와 테이블
        splitter = QSplitter(Qt.Horizontal)

        # 차트
        chart_group = QGroupBox("차트")
        chart_layout = QVBoxLayout(chart_group)
        self.detail_chart = ChartWidget()
        chart_layout.addWidget(self.detail_chart)
        splitter.addWidget(chart_group)

        # 테이블
        table_group = QGroupBox("상세 데이터")
        table_layout = QVBoxLayout(table_group)
        self.detail_table = DataTableWidget()
        table_layout.addWidget(self.detail_table)
        splitter.addWidget(table_group)

        splitter.setSizes([600, 400])
        layout.addWidget(splitter)

        return tab

    def create_settings_tab(self) -> QWidget:
        """설정 탭"""
        tab = QWidget()
        layout = QVBoxLayout(tab)

        # 목표 설정
        target_group = QGroupBox("목표 설정")
        target_layout = QFormLayout(target_group)

        self.sales_target_input = QSpinBox()
        self.sales_target_input.setRange(0, 999999999999)
        self.sales_target_input.setSingleStep(10000000)
        self.sales_target_input.setSuffix(" 원")
        self.sales_target_input.valueChanged.connect(self.update_targets)
        target_layout.addRow("매출 목표:", self.sales_target_input)

        self.profit_target_input = QSpinBox()
        self.profit_target_input.setRange(0, 999999999999)
        self.profit_target_input.setSingleStep(1000000)
        self.profit_target_input.setSuffix(" 원")
        self.profit_target_input.valueChanged.connect(self.update_targets)
        target_layout.addRow("이익 목표:", self.profit_target_input)

        layout.addWidget(target_group)

        # KPI 표시
        kpi_group = QGroupBox("KPI 현황")
        kpi_layout = QHBoxLayout(kpi_group)

        self.sales_kpi = KPIGaugeWidget("매출 달성률")
        self.profit_kpi = KPIGaugeWidget("이익 달성률")

        kpi_layout.addWidget(self.sales_kpi)
        kpi_layout.addWidget(self.profit_kpi)

        layout.addWidget(kpi_group)
        layout.addStretch()

        return tab

    def load_excel_file(self):
        """엑셀 파일 불러오기"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "엑셀 파일 선택", "",
            "Excel Files (*.xlsx *.xls *.csv);;All Files (*)"
        )

        if not file_path:
            return

        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.load_btn.setEnabled(False)
        self.statusBar.showMessage("데이터 로딩 중...")

        # 스레드로 로딩
        self.load_thread = DataLoadThread(file_path)
        self.load_thread.finished.connect(self.on_data_loaded)
        self.load_thread.error.connect(self.on_load_error)
        self.load_thread.progress.connect(self.progress_bar.setValue)
        self.load_thread.start()

        self.file_info_label.setText(f"파일: {Path(file_path).name}")

    def on_data_loaded(self, df: pd.DataFrame):
        """데이터 로딩 완료"""
        self.df = df
        self.progress_bar.setVisible(False)
        self.load_btn.setEnabled(True)
        self.refresh_btn.setEnabled(True)
        self.excel_report_btn.setEnabled(True)
        self.pdf_report_btn.setEnabled(True)

        self.statusBar.showMessage(f"데이터 로드 완료: {len(df)} 행")

        # 지표 계산 및 대시보드 업데이트
        self.calculate_and_update()

    def on_load_error(self, error_msg: str):
        """로딩 에러"""
        self.progress_bar.setVisible(False)
        self.load_btn.setEnabled(True)
        self.statusBar.showMessage("로드 실패")
        QMessageBox.critical(self, "오류", f"파일 로드 중 오류 발생:\n{error_msg}")

    def calculate_and_update(self):
        """지표 계산 및 UI 업데이트"""
        if self.df is None:
            return

        try:
            # 지표 계산
            calculator = MetricsCalculator(self.df)
            self.metrics = calculator.calculate_all_metrics(self.targets)

            # 대시보드 업데이트
            self.update_dashboard()

            # 상세 분석 업데이트
            self.update_detail_analysis()

            # KPI 업데이트
            self.update_kpi_display()

        except Exception as e:
            QMessageBox.warning(self, "경고", f"지표 계산 중 오류:\n{str(e)}")

    def update_dashboard(self):
        """대시보드 업데이트"""
        if not self.metrics:
            return

        summary = self.metrics.get('summary', {})

        # 요약 카드 업데이트
        total_sales = summary.get('total_sales', 0)
        total_profit = summary.get('total_profit', 0)
        margin = summary.get('profit_margin', 0)
        count = summary.get('transaction_count', 0)

        self.total_sales_card.update_value(self._format_currency(total_sales))
        self.total_profit_card.update_value(self._format_currency(total_profit))
        self.profit_margin_card.update_value(f"{margin}%")
        self.transaction_card.update_value(f"{count:,}")

        # 월별 추이 차트
        financial = self.metrics.get('financial', {})
        monthly_sales = financial.get('monthly_sales', {})
        if monthly_sales:
            self.monthly_chart.plot_line(monthly_sales, "월별 매출 추이", "월", "매출액")

        # 센터별 비중 차트
        center_df = self.metrics.get('sales_by_center')
        if isinstance(center_df, pd.DataFrame) and not center_df.empty:
            if '매출액' in center_df.columns:
                center_col = center_df.columns[0]
                center_data = dict(zip(center_df[center_col], center_df['매출액']))
                self.center_chart.plot_pie(center_data, "센터별 비중")

        # 담당자별 테이블
        manager_df = self.metrics.get('sales_by_manager')
        if isinstance(manager_df, pd.DataFrame) and not manager_df.empty:
            self.manager_table.load_dataframe(manager_df)

        # 전년 대비 테이블
        yoy_df = self.metrics.get('yoy_comparison')
        if isinstance(yoy_df, pd.DataFrame) and not yoy_df.empty:
            self.yoy_table.load_dataframe(yoy_df, highlight_negative=True)

    def update_detail_analysis(self):
        """상세 분석 업데이트"""
        if not self.metrics:
            return

        analysis_type = self.analysis_type_combo.currentText()

        df = None
        chart_data = {}
        chart_title = ""

        if analysis_type == "담당자별 분석":
            df = self.metrics.get('sales_by_manager')
            if isinstance(df, pd.DataFrame) and not df.empty and '매출액' in df.columns:
                col = df.columns[0]
                chart_data = dict(zip(df[col].head(10), df['매출액'].head(10)))
                chart_title = "담당자별 매출 TOP 10"

        elif analysis_type == "센터별 분석":
            df = self.metrics.get('sales_by_center')
            if isinstance(df, pd.DataFrame) and not df.empty and '매출액' in df.columns:
                col = df.columns[0]
                chart_data = dict(zip(df[col], df['매출액']))
                chart_title = "센터별 매출"

        elif analysis_type == "검사목적별 분석":
            df = self.metrics.get('sales_by_purpose')
            if isinstance(df, pd.DataFrame) and not df.empty and '매출액' in df.columns:
                col = df.columns[0]
                chart_data = dict(zip(df[col], df['매출액']))
                chart_title = "검사목적별 매출"

        elif analysis_type == "월별 추이":
            df = self.metrics.get('monthly_trend')
            if isinstance(df, pd.DataFrame) and not df.empty and '매출액' in df.columns:
                chart_data = dict(zip(df['년월'], df['매출액']))
                chart_title = "월별 매출 추이"

        elif analysis_type == "분기별 추이":
            df = self.metrics.get('quarterly_summary')
            if isinstance(df, pd.DataFrame) and not df.empty and '매출액' in df.columns:
                chart_data = dict(zip(df['분기'], df['매출액']))
                chart_title = "분기별 매출"

        elif analysis_type == "전년 대비":
            df = self.metrics.get('yoy_comparison')

        # 차트 그리기
        if chart_data:
            if analysis_type in ["담당자별 분석"]:
                self.detail_chart.plot_horizontal_bar(chart_data, chart_title)
            elif analysis_type in ["센터별 분석", "검사목적별 분석"]:
                self.detail_chart.plot_pie(chart_data, chart_title)
            else:
                self.detail_chart.plot_bar(chart_data, chart_title)

        # 테이블 로드
        if isinstance(df, pd.DataFrame) and not df.empty:
            self.detail_table.load_dataframe(df, highlight_negative=True)

    def update_targets(self):
        """목표 값 업데이트"""
        self.targets = {
            'sales_target': self.sales_target_input.value(),
            'profit_target': self.profit_target_input.value(),
        }

        if self.df is not None:
            self.calculate_and_update()

    def update_kpi_display(self):
        """KPI 표시 업데이트"""
        if not self.metrics or 'summary' not in self.metrics:
            return

        summary = self.metrics['summary']

        sales_target = self.targets.get('sales_target', 0)
        profit_target = self.targets.get('profit_target', 0)

        if sales_target > 0:
            self.sales_kpi.set_value(summary.get('total_sales', 0), sales_target)

        if profit_target > 0:
            self.profit_kpi.set_value(summary.get('total_profit', 0), profit_target)

    def refresh_data(self):
        """데이터 새로고침"""
        if self.df is not None:
            self.calculate_and_update()
            self.statusBar.showMessage("데이터 새로고침 완료")

    def export_excel_report(self):
        """Excel 보고서 내보내기"""
        if not self.metrics:
            QMessageBox.warning(self, "경고", "먼저 데이터를 불러와 주세요.")
            return

        try:
            generator = ReportGenerator(REPORTS_DIR)
            filepath = generator.generate_excel_report(self.metrics)
            QMessageBox.information(self, "완료", f"Excel 보고서가 저장되었습니다:\n{filepath}")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"보고서 생성 실패:\n{str(e)}")

    def export_pdf_report(self):
        """PDF 보고서 내보내기"""
        if not self.metrics:
            QMessageBox.warning(self, "경고", "먼저 데이터를 불러와 주세요.")
            return

        try:
            generator = ReportGenerator(REPORTS_DIR)

            # 차트 이미지 임시 저장
            charts = {}
            temp_chart_path = REPORTS_DIR / "temp_monthly_chart.png"
            self.monthly_chart.save_chart(str(temp_chart_path))
            charts['monthly'] = str(temp_chart_path)

            filepath = generator.generate_pdf_report(self.metrics, charts)
            QMessageBox.information(self, "완료", f"PDF 보고서가 저장되었습니다:\n{filepath}")
        except Exception as e:
            QMessageBox.critical(self, "오류", f"보고서 생성 실패:\n{str(e)}")

    def _format_currency(self, value: float) -> str:
        """통화 형식으로 포맷"""
        if value >= 100000000:
            return f"{value/100000000:.1f}억"
        elif value >= 10000:
            return f"{value/10000:.0f}만"
        else:
            return f"{value:,.0f}"


def main():
    """메인 함수"""
    from PyQt5.QtWidgets import QApplication
    import sys

    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # 한글 폰트 설정
    font = QFont("Malgun Gothic", 10)
    app.setFont(font)

    window = MainWindow()
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()
