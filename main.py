#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
경영지표 분석기 (Business Metrics Analyzer)
- 엑셀 데이터 불러오기
- 경영 지표 자동 계산
- 대시보드 시각화
- 보고서 출력 (Excel/PDF)

실행 방법:
    python main.py

또는 GUI 없이 CLI로:
    python main.py --cli input.xlsx
"""
import sys
import argparse
from pathlib import Path


def run_gui():
    """GUI 모드 실행"""
    from PyQt5.QtWidgets import QApplication
    from PyQt5.QtGui import QFont
    from ui.main_window import MainWindow

    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # 한글 폰트 설정
    font = QFont("Malgun Gothic", 10)
    app.setFont(font)

    window = MainWindow()
    window.show()

    sys.exit(app.exec_())


def run_cli(input_file: str, output_dir: str = "reports"):
    """CLI 모드 실행"""
    from modules.data_loader import DataLoader
    from modules.data_processor import DataProcessor
    from modules.metrics_calculator import MetricsCalculator
    from modules.report_generator import ReportGenerator
    from config.settings import COLUMN_MAPPING

    print("=" * 50)
    print("경영지표 분석기 (CLI Mode)")
    print("=" * 50)

    # 1. 데이터 로드
    print(f"\n[1/4] 데이터 로드 중... ({input_file})")
    loader = DataLoader(column_mapping=COLUMN_MAPPING)
    df = loader.load_excel(input_file)
    print(f"    - {len(df)} 행 로드 완료")

    # 2. 데이터 전처리
    print("\n[2/4] 데이터 전처리 중...")
    processor = DataProcessor()
    df = processor.process(df)
    summary = processor.get_summary(df)
    print(f"    - 기간: {summary.get('date_range', {})}")

    # 3. 지표 계산
    print("\n[3/4] 지표 계산 중...")
    calculator = MetricsCalculator(df)
    metrics = calculator.calculate_all_metrics()

    # 요약 출력
    s = metrics.get('summary', {})
    print(f"\n{'='*40}")
    print("[ 요약 ]")
    print(f"{'='*40}")
    print(f"  총 매출액: {s.get('total_sales', 0):,.0f} 원")
    print(f"  총 이익:   {s.get('total_profit', 0):,.0f} 원")
    print(f"  이익률:    {s.get('profit_margin', 0)}%")
    print(f"  거래 건수: {s.get('transaction_count', 0)}")

    # 4. 보고서 생성
    print(f"\n[4/4] 보고서 생성 중...")
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    generator = ReportGenerator(output_path)
    excel_path = generator.generate_excel_report(metrics)
    print(f"    - Excel: {excel_path}")

    try:
        pdf_path = generator.generate_pdf_report(metrics)
        print(f"    - PDF:   {pdf_path}")
    except ImportError:
        print("    - PDF: reportlab이 설치되지 않아 건너뜀")

    print(f"\n{'='*50}")
    print("완료!")
    print("=" * 50)


def create_sample_data():
    """샘플 데이터 생성"""
    import pandas as pd
    import numpy as np
    from datetime import datetime, timedelta

    print("샘플 데이터 생성 중...")

    # 날짜 범위: 최근 1년
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]

    # 샘플 데이터 생성
    np.random.seed(42)
    n = 500

    data = {
        '날짜': np.random.choice(dates, n),
        '매출액': np.random.randint(100000, 5000000, n),
        '비용': np.random.randint(50000, 2000000, n),
        '담당자': np.random.choice(['김영업', '이대리', '박과장', '최부장', '정차장'], n),
        '센터': np.random.choice(['서울센터', '부산센터', '대전센터', '광주센터'], n),
        '검사목적': np.random.choice(['정기검사', '품질관리', '신제품검사', '클레임대응', '연구개발'], n),
        '검사건수': np.random.randint(1, 20, n),
        '거래처': np.random.choice(['A식품', 'B제과', 'C음료', 'D유업', 'E농산'], n),
    }

    df = pd.DataFrame(data)
    df['날짜'] = pd.to_datetime(df['날짜'])
    df = df.sort_values('날짜').reset_index(drop=True)

    # 이익 계산
    df['이익'] = df['매출액'] - df['비용']

    # 파일 저장
    output_path = Path("data/sample_sales_data.xlsx")
    output_path.parent.mkdir(exist_ok=True)
    df.to_excel(output_path, index=False)

    print(f"샘플 데이터 저장: {output_path}")
    print(f"  - 행 수: {len(df)}")
    print(f"  - 기간: {df['날짜'].min()} ~ {df['날짜'].max()}")

    return output_path


def main():
    parser = argparse.ArgumentParser(description="경영지표 분석기")
    parser.add_argument('--cli', type=str, help='CLI 모드로 실행 (엑셀 파일 경로)')
    parser.add_argument('--output', type=str, default='reports', help='출력 디렉토리')
    parser.add_argument('--sample', action='store_true', help='샘플 데이터 생성')

    args = parser.parse_args()

    if args.sample:
        create_sample_data()
    elif args.cli:
        run_cli(args.cli, args.output)
    else:
        run_gui()


if __name__ == "__main__":
    main()
