"""
경영지표 분석기 설정
"""
import os
from pathlib import Path

# 프로젝트 루트 경로
BASE_DIR = Path(__file__).resolve().parent.parent

# 데이터 저장 경로
DATA_DIR = BASE_DIR / "data"
REPORTS_DIR = BASE_DIR / "reports"

# 디렉토리 생성
DATA_DIR.mkdir(exist_ok=True)
REPORTS_DIR.mkdir(exist_ok=True)

# 엑셀 컬럼 매핑 (BioFL 접수내역 엑셀 기준)
COLUMN_MAPPING = {
    "date": ["접수일자", "날짜", "일자"],
    "sales_amount": ["수수료", "공급가액", "매출액", "금액"],
    "tax_amount": ["세액"],
    "manager": ["영업담당", "담당자"],
    "center": ["영업팀", "지부명", "센터", "부서"],
    "client": ["의뢰업체명", "거래처", "업체명"],
    "test_purpose": ["검사목적", "목적"],
    "test_count": ["항목개수", "검사건수", "건수"],
    "status": ["상태"],
    "test_field": ["시험분야"],
    "product_name": ["제품/시료명", "제품명", "시료명"],
    "receipt_no": ["접수번호"],
    "completion_date": ["완료예정일"],
    "payment_status": ["입금여부", "입금구분"],
    "payment_amount": ["입금액"],
    "outstanding": ["업체총미수금", "잔액"],
}

# 지표 계산 설정
METRICS_CONFIG = {
    "fiscal_year_start_month": 1,  # 회계연도 시작월 (1=1월, 4=4월)
    "target_achievement_threshold": {
        "excellent": 120,  # 120% 이상
        "good": 100,       # 100% 이상
        "warning": 80,     # 80% 이상
        "danger": 0,       # 80% 미만
    },
    "yoy_growth_threshold": {
        "high_growth": 20,   # 20% 이상 성장
        "growth": 5,         # 5% 이상 성장
        "stable": -5,        # -5% ~ 5%
        "decline": -20,      # -5% ~ -20%
        "critical": -100,    # -20% 미만
    },
}

# 차트 색상
CHART_COLORS = {
    "primary": "#3498db",
    "success": "#2ecc71",
    "warning": "#f39c12",
    "danger": "#e74c3c",
    "info": "#9b59b6",
    "secondary": "#95a5a6",
}

# 보고서 설정
REPORT_CONFIG = {
    "company_name": "우리 회사",  # 회사명 설정
    "logo_path": None,            # 로고 이미지 경로
    "font_family": "Malgun Gothic",
}
