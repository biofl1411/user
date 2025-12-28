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

# 엑셀 컬럼 매핑 (실제 엑셀 컬럼명에 맞게 수정 필요)
COLUMN_MAPPING = {
    "date": ["날짜", "일자", "접수일", "Date"],
    "sales_amount": ["매출액", "매출금액", "금액", "Sales"],
    "cost": ["비용", "원가", "Cost"],
    "profit": ["이익", "영업이익", "순이익", "Profit"],
    "manager": ["담당자", "영업담당", "Manager"],
    "center": ["센터", "지점", "부서", "Center", "Branch"],
    "client": ["거래처", "고객", "업체명", "Client"],
    "test_purpose": ["검사목적", "목적", "Purpose"],
    "test_count": ["검사건수", "건수", "Count"],
    "processing_days": ["처리일수", "TAT", "소요일"],
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
