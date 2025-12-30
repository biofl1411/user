"""
목표 데이터 관리 모듈
- 연간/월간 목표 로드
- 목표 대비 실적 비교
- 달성률 계산
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


class TargetManager:
    """목표 데이터 관리"""

    def __init__(self, targets_dir: Optional[Path] = None):
        """
        Args:
            targets_dir: 목표 파일 디렉토리
        """
        self.targets_dir = targets_dir or Path("data/targets")
        self.targets_data: Dict = {}

    def load_targets(self, year: int = 2025) -> pd.DataFrame:
        """
        연간 목표 데이터 로드

        Args:
            year: 연도

        Returns:
            목표 DataFrame
        """
        # CSV 또는 Excel 파일 찾기
        csv_file = self.targets_dir / f"{year}_목표.csv"
        xlsx_file = self.targets_dir / f"{year}_목표.xlsx"

        if csv_file.exists():
            df = pd.read_csv(csv_file, encoding='utf-8-sig')
        elif xlsx_file.exists():
            df = pd.read_excel(xlsx_file)
        else:
            logger.warning(f"{year}년 목표 파일이 없습니다.")
            return pd.DataFrame()

        self.targets_data[year] = df
        logger.info(f"{year}년 목표 데이터 로드 완료")
        return df

    def get_monthly_target(self, year: int, month: int, category: str = "총계") -> float:
        """
        월별 목표 조회

        Args:
            year: 연도
            month: 월 (1-12)
            category: 구분 (총계, 식품/축산, 잔류물질, 신규사업)

        Returns:
            목표 금액 (천원)
        """
        if year not in self.targets_data:
            self.load_targets(year)

        df = self.targets_data.get(year)
        if df is None or df.empty:
            return 0

        month_col = f"{month}월"

        # 구분과 분류가 둘 다 있는 경우
        if '분류' in df.columns:
            row = df[(df['구분'] == category) & (df['분류'].isna() | (df['분류'] == ''))]
        else:
            row = df[df['구분'] == category]

        if row.empty:
            return 0

        if month_col in row.columns:
            return float(row[month_col].values[0])
        return 0

    def get_yearly_target(self, year: int, category: str = "총계") -> float:
        """
        연간 목표 조회

        Args:
            year: 연도
            category: 구분

        Returns:
            연간 목표 금액 (천원)
        """
        if year not in self.targets_data:
            self.load_targets(year)

        df = self.targets_data.get(year)
        if df is None or df.empty:
            return 0

        if '분류' in df.columns:
            row = df[(df['구분'] == category) & (df['분류'].isna() | (df['분류'] == ''))]
        else:
            row = df[df['구분'] == category]

        if row.empty:
            return 0

        if '합계' in row.columns:
            return float(row['합계'].values[0])
        return 0

    def calculate_achievement(self, actual: float, target: float) -> Dict:
        """
        달성률 계산

        Args:
            actual: 실적
            target: 목표

        Returns:
            달성률 정보
        """
        if target <= 0:
            return {
                'rate': 0,
                'gap': actual,
                'status': '목표없음'
            }

        rate = (actual / target) * 100

        if rate >= 120:
            status = '우수'
        elif rate >= 100:
            status = '달성'
        elif rate >= 80:
            status = '주의'
        else:
            status = '미달'

        return {
            'rate': round(rate, 1),
            'gap': actual - target,
            'status': status
        }

    def compare_with_actual(self, actual_df: pd.DataFrame, year: int, month: int = None) -> pd.DataFrame:
        """
        목표 대비 실적 비교

        Args:
            actual_df: 실적 DataFrame (월별 집계된)
            year: 연도
            month: 월 (None이면 연간)

        Returns:
            비교 결과 DataFrame
        """
        if year not in self.targets_data:
            self.load_targets(year)

        targets_df = self.targets_data.get(year)
        if targets_df is None or targets_df.empty:
            return actual_df

        # 결과 데이터 구성
        results = []
        categories = ['총계', '식품/축산', '잔류물질', '신규사업']

        for cat in categories:
            if month:
                target = self.get_monthly_target(year, month, cat)
            else:
                target = self.get_yearly_target(year, cat)

            # 실적은 actual_df에서 가져와야 함 (구현 필요)
            actual = 0  # TODO: actual_df에서 카테고리별 실적 계산

            achievement = self.calculate_achievement(actual, target)

            results.append({
                '구분': cat,
                '목표': target,
                '실적': actual,
                '달성률': f"{achievement['rate']}%",
                '차이': achievement['gap'],
                '상태': achievement['status']
            })

        return pd.DataFrame(results)

    def get_all_targets_summary(self, year: int) -> pd.DataFrame:
        """
        연간 목표 요약

        Args:
            year: 연도

        Returns:
            요약 DataFrame
        """
        if year not in self.targets_data:
            self.load_targets(year)

        df = self.targets_data.get(year)
        if df is None or df.empty:
            return pd.DataFrame()

        # 주요 구분만 필터링
        main_categories = df[df['분류'].isna() | (df['분류'] == '')]

        return main_categories[['구분', '합계']].copy()


# 테스트 코드
if __name__ == "__main__":
    manager = TargetManager(Path("data/targets"))

    # 목표 로드
    targets = manager.load_targets(2025)
    print("=== 2025년 목표 ===")
    print(targets)

    # 월별 목표 조회
    jan_target = manager.get_monthly_target(2025, 1, "총계")
    print(f"\n1월 총계 목표: {jan_target:,.0f}천원")

    # 연간 목표 조회
    yearly = manager.get_yearly_target(2025, "총계")
    print(f"연간 총계 목표: {yearly:,.0f}천원 ({yearly/1000:,.0f}백만원)")
