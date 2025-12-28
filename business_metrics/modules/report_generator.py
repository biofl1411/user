"""
보고서 생성 모듈
- PDF 보고서 출력
- Excel 보고서 저장
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ReportGenerator:
    """보고서 생성기"""

    def __init__(self, output_dir: Optional[Path] = None):
        """
        Args:
            output_dir: 보고서 저장 디렉토리
        """
        self.output_dir = output_dir or Path("reports")
        self.output_dir.mkdir(exist_ok=True)

    def generate_excel_report(self, metrics: Dict,
                              filename: Optional[str] = None) -> Path:
        """
        Excel 보고서 생성

        Args:
            metrics: 지표 딕셔너리
            filename: 파일명 (없으면 자동 생성)

        Returns:
            저장된 파일 경로
        """
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"경영지표보고서_{timestamp}.xlsx"

        filepath = self.output_dir / filename

        with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
            # 1. 요약 시트
            summary_data = []
            if 'summary' in metrics:
                s = metrics['summary']
                summary_data = [
                    ['총 매출액', self._format_currency(s.get('total_sales', 0))],
                    ['총 비용', self._format_currency(s.get('total_cost', 0))],
                    ['총 이익', self._format_currency(s.get('total_profit', 0))],
                    ['이익률(%)', f"{s.get('profit_margin', 0)}%"],
                    ['거래 건수', s.get('transaction_count', 0)],
                    ['평균 거래금액', self._format_currency(s.get('avg_sales_per_transaction', 0))],
                    ['기간', s.get('period', '')],
                ]
            summary_df = pd.DataFrame(summary_data, columns=['항목', '값'])
            summary_df.to_excel(writer, sheet_name='요약', index=False)

            # 2. 월별 추이
            if 'monthly_trend' in metrics and isinstance(metrics['monthly_trend'], pd.DataFrame):
                if not metrics['monthly_trend'].empty:
                    metrics['monthly_trend'].to_excel(writer, sheet_name='월별추이', index=False)

            # 3. 분기별 요약
            if 'quarterly_summary' in metrics and isinstance(metrics['quarterly_summary'], pd.DataFrame):
                if not metrics['quarterly_summary'].empty:
                    metrics['quarterly_summary'].to_excel(writer, sheet_name='분기별', index=False)

            # 4. 담당자별
            if 'sales_by_manager' in metrics and isinstance(metrics['sales_by_manager'], pd.DataFrame):
                if not metrics['sales_by_manager'].empty:
                    metrics['sales_by_manager'].to_excel(writer, sheet_name='담당자별', index=False)

            # 5. 센터별
            if 'sales_by_center' in metrics and isinstance(metrics['sales_by_center'], pd.DataFrame):
                if not metrics['sales_by_center'].empty:
                    metrics['sales_by_center'].to_excel(writer, sheet_name='센터별', index=False)

            # 6. 검사목적별
            if 'sales_by_purpose' in metrics and isinstance(metrics['sales_by_purpose'], pd.DataFrame):
                if not metrics['sales_by_purpose'].empty:
                    metrics['sales_by_purpose'].to_excel(writer, sheet_name='검사목적별', index=False)

            # 7. 전년대비
            if 'yoy_comparison' in metrics and isinstance(metrics['yoy_comparison'], pd.DataFrame):
                if not metrics['yoy_comparison'].empty:
                    metrics['yoy_comparison'].to_excel(writer, sheet_name='전년대비', index=False)

            # 8. KPI
            if 'kpi' in metrics and metrics['kpi']:
                kpi = metrics['kpi']
                kpi_data = [['전체 상태', kpi.get('overall_status', '')]]

                if kpi.get('sales_achievement'):
                    sa = kpi['sales_achievement']
                    kpi_data.extend([
                        ['매출 목표', self._format_currency(sa.get('target', 0))],
                        ['매출 실적', self._format_currency(sa.get('actual', 0))],
                        ['달성률(%)', f"{sa.get('rate', 0)}%"],
                        ['차이', self._format_currency(sa.get('gap', 0))],
                    ])

                kpi_df = pd.DataFrame(kpi_data, columns=['항목', '값'])
                kpi_df.to_excel(writer, sheet_name='KPI', index=False)

        logger.info(f"Excel 보고서 저장: {filepath}")
        return filepath

    def generate_pdf_report(self, metrics: Dict, charts: Dict = None,
                           filename: Optional[str] = None,
                           company_name: str = "우리 회사") -> Path:
        """
        PDF 보고서 생성

        Args:
            metrics: 지표 딕셔너리
            charts: 차트 이미지 딕셔너리 {'monthly': 'path/to/chart.png', ...}
            filename: 파일명
            company_name: 회사명

        Returns:
            저장된 파일 경로
        """
        try:
            from reportlab.lib import colors
            from reportlab.lib.pagesizes import A4
            from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
            from reportlab.lib.units import mm
            from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, Image
            from reportlab.pdfbase import pdfmetrics
            from reportlab.pdfbase.ttfonts import TTFont
        except ImportError:
            logger.error("reportlab이 설치되지 않았습니다.")
            raise

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"경영지표보고서_{timestamp}.pdf"

        filepath = self.output_dir / filename

        # PDF 생성
        doc = SimpleDocTemplate(
            str(filepath),
            pagesize=A4,
            rightMargin=20*mm,
            leftMargin=20*mm,
            topMargin=20*mm,
            bottomMargin=20*mm
        )

        styles = getSampleStyleSheet()
        story = []

        # 제목
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            alignment=1  # Center
        )
        story.append(Paragraph(f"{company_name} - 경영지표 보고서", title_style))

        # 생성일
        date_style = ParagraphStyle(
            'DateStyle',
            parent=styles['Normal'],
            fontSize=12,
            alignment=1
        )
        story.append(Paragraph(f"생성일: {datetime.now().strftime('%Y년 %m월 %d일')}", date_style))
        story.append(Spacer(1, 20))

        # 요약 섹션
        if 'summary' in metrics:
            story.append(Paragraph("1. 요약", styles['Heading2']))
            story.append(Spacer(1, 10))

            s = metrics['summary']
            summary_data = [
                ['항목', '값'],
                ['총 매출액', self._format_currency(s.get('total_sales', 0))],
                ['총 이익', self._format_currency(s.get('total_profit', 0))],
                ['이익률', f"{s.get('profit_margin', 0)}%"],
                ['거래 건수', str(s.get('transaction_count', 0))],
                ['기간', s.get('period', '-')],
            ]

            table = Table(summary_data, colWidths=[80*mm, 80*mm])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498db')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('FONTSIZE', (0, 0), (-1, -1), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#ecf0f1')),
                ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7')),
            ]))
            story.append(table)
            story.append(Spacer(1, 20))

        # KPI 섹션
        if 'kpi' in metrics and metrics['kpi']:
            story.append(Paragraph("2. KPI 달성 현황", styles['Heading2']))
            story.append(Spacer(1, 10))

            kpi = metrics['kpi']
            status = kpi.get('overall_status', '')
            status_color = {
                '우수': colors.HexColor('#27ae60'),
                '달성': colors.HexColor('#2ecc71'),
                '주의': colors.HexColor('#f39c12'),
                '미달': colors.HexColor('#e74c3c'),
            }.get(status, colors.grey)

            story.append(Paragraph(f"전체 상태: <font color='{status_color}'><b>{status}</b></font>", styles['Normal']))

            if kpi.get('sales_achievement'):
                sa = kpi['sales_achievement']
                kpi_data = [
                    ['구분', '목표', '실적', '달성률', '차이'],
                    ['매출',
                     self._format_currency(sa.get('target', 0)),
                     self._format_currency(sa.get('actual', 0)),
                     f"{sa.get('rate', 0)}%",
                     self._format_currency(sa.get('gap', 0))],
                ]
                kpi_table = Table(kpi_data)
                kpi_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#9b59b6')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#bdc3c7')),
                ]))
                story.append(Spacer(1, 10))
                story.append(kpi_table)

            story.append(Spacer(1, 20))

        # 차트 이미지 삽입
        if charts:
            story.append(Paragraph("3. 차트", styles['Heading2']))
            story.append(Spacer(1, 10))

            for chart_name, chart_path in charts.items():
                if Path(chart_path).exists():
                    img = Image(chart_path, width=160*mm, height=100*mm)
                    story.append(img)
                    story.append(Spacer(1, 10))

        # PDF 빌드
        doc.build(story)
        logger.info(f"PDF 보고서 저장: {filepath}")
        return filepath

    def _format_currency(self, value: float) -> str:
        """통화 형식으로 포맷"""
        if value >= 100000000:  # 1억 이상
            return f"{value/100000000:.1f}억원"
        elif value >= 10000:  # 1만 이상
            return f"{value/10000:.0f}만원"
        else:
            return f"{value:,.0f}원"


# 테스트 코드
if __name__ == "__main__":
    # 테스트 지표
    test_metrics = {
        'summary': {
            'total_sales': 150000000,
            'total_cost': 100000000,
            'total_profit': 50000000,
            'profit_margin': 33.33,
            'transaction_count': 150,
            'avg_sales_per_transaction': 1000000,
            'period': '2024-01-01 ~ 2024-03-31',
        },
        'kpi': {
            'overall_status': '달성',
            'sales_achievement': {
                'target': 140000000,
                'actual': 150000000,
                'rate': 107.14,
                'gap': 10000000,
            },
        },
    }

    generator = ReportGenerator()

    # Excel 보고서 생성
    excel_path = generator.generate_excel_report(test_metrics)
    print(f"Excel: {excel_path}")

    # PDF 보고서 생성
    try:
        pdf_path = generator.generate_pdf_report(test_metrics)
        print(f"PDF: {pdf_path}")
    except ImportError:
        print("PDF 생성에 필요한 reportlab이 없습니다.")
