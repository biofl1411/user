"""
경영지표 계산 엔진
- 재무 지표: 매출액, 성장률, 이익
- 영업 지표: 담당자별, 센터별, 거래처별
- 운영 지표: 검사건수, TAT
- KPI: 목표 달성률, 전년 대비
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class MetricsCalculator:
    """경영지표 계산기"""

    def __init__(self, df: pd.DataFrame):
        """
        Args:
            df: 전처리된 DataFrame
        """
        self.df = df
        self.metrics_cache: Dict = {}

    def calculate_all_metrics(self, targets: Optional[Dict] = None) -> Dict:
        """
        모든 지표 계산

        Args:
            targets: 목표 값 딕셔너리 {'매출목표': 100000000, ...}

        Returns:
            전체 지표 딕셔너리
        """
        metrics = {
            "summary": self.get_summary_metrics(),
            "financial": self.calculate_financial_metrics(),
            "sales_by_manager": self.calculate_by_dimension("manager"),
            "sales_by_center": self.calculate_by_dimension("center"),
            "sales_by_purpose": self.calculate_by_dimension("test_purpose"),
            "monthly_trend": self.calculate_monthly_trend(),
            "quarterly_summary": self.calculate_quarterly_summary(),
            "yoy_comparison": self.calculate_yoy_comparison(),
            "operational": self.calculate_operational_metrics(),
        }

        if targets:
            metrics["kpi"] = self.calculate_kpi(targets)

        self.metrics_cache = metrics
        return metrics

    def get_summary_metrics(self) -> Dict:
        """상단 요약 카드용 지표"""
        df = self.df

        # 매출 관련 컬럼 찾기
        sales_col = self._find_column(['sales_amount', '매출액', '매출금액', '금액'])
        cost_col = self._find_column(['cost', '비용', '원가'])
        profit_col = self._find_column(['profit', '이익', '순이익', 'calculated_profit'])

        summary = {
            "total_sales": 0,
            "total_cost": 0,
            "total_profit": 0,
            "profit_margin": 0,
            "avg_sales_per_transaction": 0,
            "transaction_count": len(df),
            "period": self._get_period_string(),
        }

        if sales_col:
            summary["total_sales"] = float(df[sales_col].sum())
            summary["avg_sales_per_transaction"] = float(df[sales_col].mean())

        if cost_col:
            summary["total_cost"] = float(df[cost_col].sum())

        if profit_col:
            summary["total_profit"] = float(df[profit_col].sum())
        elif sales_col and cost_col:
            summary["total_profit"] = summary["total_sales"] - summary["total_cost"]

        if summary["total_sales"] > 0:
            summary["profit_margin"] = round(
                summary["total_profit"] / summary["total_sales"] * 100, 2
            )

        return summary

    def calculate_financial_metrics(self) -> Dict:
        """재무 지표 계산"""
        df = self.df
        sales_col = self._find_column(['sales_amount', '매출액', '매출금액'])

        financial = {
            "monthly_sales": {},
            "quarterly_sales": {},
            "yearly_sales": {},
            "growth_rates": {},
        }

        if not sales_col or 'year_month' not in df.columns:
            return financial

        # 월별 매출
        monthly = df.groupby('year_month')[sales_col].sum()
        financial["monthly_sales"] = monthly.to_dict()

        # 분기별 매출
        if 'year_quarter' in df.columns:
            quarterly = df.groupby('year_quarter')[sales_col].sum()
            financial["quarterly_sales"] = quarterly.to_dict()

        # 연도별 매출
        if 'year' in df.columns:
            yearly = df.groupby('year')[sales_col].sum()
            financial["yearly_sales"] = yearly.to_dict()

            # 연간 성장률
            growth_rates = yearly.pct_change() * 100
            financial["growth_rates"] = growth_rates.dropna().round(2).to_dict()

        return financial

    def calculate_by_dimension(self, dimension: str) -> pd.DataFrame:
        """
        차원별 집계

        Args:
            dimension: 집계 기준 ('manager', 'center', 'test_purpose', 'client')

        Returns:
            집계 DataFrame
        """
        df = self.df

        # 컬럼명 매핑
        dimension_cols = {
            'manager': ['manager', '담당자', '영업담당'],
            'center': ['center', '센터', '지점', '부서'],
            'test_purpose': ['test_purpose', '검사목적', '목적'],
            'client': ['client', '거래처', '고객', '업체명'],
        }

        dim_col = None
        for col_name in dimension_cols.get(dimension, []):
            if col_name in df.columns:
                dim_col = col_name
                break

        if not dim_col:
            return pd.DataFrame()

        sales_col = self._find_column(['sales_amount', '매출액', '매출금액', '금액'])
        count_col = self._find_column(['test_count', '검사건수', '건수'])

        agg_dict = {}
        if sales_col:
            agg_dict['매출액'] = (sales_col, 'sum')
            agg_dict['평균단가'] = (sales_col, 'mean')
        if count_col:
            agg_dict['건수'] = (count_col, 'sum')

        agg_dict['거래건수'] = (df.columns[0], 'count')

        result = df.groupby(dim_col).agg(**agg_dict).reset_index()
        result.columns = [dim_col] + list(agg_dict.keys())

        # 비중 계산
        if '매출액' in result.columns:
            total = result['매출액'].sum()
            result['비중(%)'] = (result['매출액'] / total * 100).round(2)
            result = result.sort_values('매출액', ascending=False)

        return result

    def calculate_monthly_trend(self) -> pd.DataFrame:
        """월별 추이 계산"""
        df = self.df

        if 'year_month' not in df.columns:
            return pd.DataFrame()

        sales_col = self._find_column(['sales_amount', '매출액', '매출금액'])
        count_col = self._find_column(['test_count', '검사건수', '건수'])

        agg_dict = {'거래건수': (df.columns[0], 'count')}
        if sales_col:
            agg_dict['매출액'] = (sales_col, 'sum')
        if count_col:
            agg_dict['검사건수'] = (count_col, 'sum')

        monthly = df.groupby('year_month').agg(**agg_dict).reset_index()
        monthly.columns = ['년월'] + list(agg_dict.keys())

        # 전월 대비 증감
        if '매출액' in monthly.columns:
            monthly['전월대비'] = monthly['매출액'].diff()
            monthly['전월대비율(%)'] = (monthly['매출액'].pct_change() * 100).round(2)

        return monthly

    def calculate_quarterly_summary(self) -> pd.DataFrame:
        """분기별 요약"""
        df = self.df

        if 'year_quarter' not in df.columns:
            return pd.DataFrame()

        sales_col = self._find_column(['sales_amount', '매출액', '매출금액'])

        if not sales_col:
            return pd.DataFrame()

        quarterly = df.groupby('year_quarter').agg(
            매출액=(sales_col, 'sum'),
            거래건수=(df.columns[0], 'count'),
            평균단가=(sales_col, 'mean'),
        ).reset_index()

        quarterly.columns = ['분기', '매출액', '거래건수', '평균단가']
        quarterly['전분기대비율(%)'] = (quarterly['매출액'].pct_change() * 100).round(2)

        return quarterly

    def calculate_yoy_comparison(self) -> pd.DataFrame:
        """전년 대비 비교"""
        df = self.df

        if 'year' not in df.columns or 'month' not in df.columns:
            return pd.DataFrame()

        sales_col = self._find_column(['sales_amount', '매출액', '매출금액'])
        if not sales_col:
            return pd.DataFrame()

        # 월별 집계
        monthly = df.groupby(['year', 'month'])[sales_col].sum().reset_index()
        monthly.columns = ['년도', '월', '매출액']

        # 피벗하여 연도별 비교
        pivot = monthly.pivot(index='월', columns='년도', values='매출액')

        # 전년 대비 계산
        years = sorted(pivot.columns)
        if len(years) >= 2:
            current_year = years[-1]
            prev_year = years[-2]
            pivot['전년대비'] = pivot[current_year] - pivot[prev_year]
            pivot['전년대비율(%)'] = ((pivot[current_year] / pivot[prev_year] - 1) * 100).round(2)

        return pivot.reset_index()

    def calculate_operational_metrics(self) -> Dict:
        """운영 지표 계산"""
        df = self.df

        operational = {
            "total_tests": 0,
            "avg_processing_days": 0,
            "test_by_purpose": {},
        }

        # 검사 건수
        count_col = self._find_column(['test_count', '검사건수', '건수'])
        if count_col:
            operational["total_tests"] = int(df[count_col].sum())
        else:
            operational["total_tests"] = len(df)

        # 평균 처리 기간 (TAT)
        tat_col = self._find_column(['processing_days', 'TAT', '처리일수', '소요일'])
        if tat_col:
            operational["avg_processing_days"] = float(df[tat_col].mean().round(2))

        # 검사목적별 비중
        purpose_col = self._find_column(['test_purpose', '검사목적', '목적'])
        if purpose_col:
            purpose_counts = df[purpose_col].value_counts()
            total = purpose_counts.sum()
            operational["test_by_purpose"] = {
                k: {"count": int(v), "ratio": round(v/total*100, 2)}
                for k, v in purpose_counts.items()
            }

        return operational

    def calculate_kpi(self, targets: Dict) -> Dict:
        """
        KPI 계산

        Args:
            targets: 목표 딕셔너리
                {
                    'sales_target': 100000000,  # 매출 목표
                    'profit_target': 20000000,  # 이익 목표
                    'test_count_target': 1000,  # 검사건수 목표
                }

        Returns:
            KPI 딕셔너리
        """
        summary = self.get_summary_metrics()

        kpi = {
            "sales_achievement": None,
            "profit_achievement": None,
            "test_achievement": None,
            "overall_status": "정상",
        }

        # 매출 목표 달성률
        if 'sales_target' in targets and targets['sales_target'] > 0:
            kpi["sales_achievement"] = {
                "target": targets['sales_target'],
                "actual": summary['total_sales'],
                "rate": round(summary['total_sales'] / targets['sales_target'] * 100, 2),
                "gap": summary['total_sales'] - targets['sales_target'],
            }

        # 이익 목표 달성률
        if 'profit_target' in targets and targets['profit_target'] > 0:
            kpi["profit_achievement"] = {
                "target": targets['profit_target'],
                "actual": summary['total_profit'],
                "rate": round(summary['total_profit'] / targets['profit_target'] * 100, 2),
                "gap": summary['total_profit'] - targets['profit_target'],
            }

        # 전체 상태 판단
        if kpi["sales_achievement"]:
            rate = kpi["sales_achievement"]["rate"]
            if rate >= 120:
                kpi["overall_status"] = "우수"
            elif rate >= 100:
                kpi["overall_status"] = "달성"
            elif rate >= 80:
                kpi["overall_status"] = "주의"
            else:
                kpi["overall_status"] = "미달"

        return kpi

    def get_top_clients(self, n: int = 10) -> pd.DataFrame:
        """상위 거래처"""
        return self.calculate_by_dimension('client').head(n)

    def get_top_managers(self, n: int = 10) -> pd.DataFrame:
        """상위 담당자"""
        return self.calculate_by_dimension('manager').head(n)

    def get_declining_items(self, threshold: float = -10) -> pd.DataFrame:
        """감소 항목 자동 하이라이트"""
        yoy = self.calculate_yoy_comparison()
        if '전년대비율(%)' in yoy.columns:
            declining = yoy[yoy['전년대비율(%)'] < threshold]
            return declining
        return pd.DataFrame()

    def _find_column(self, possible_names: List[str]) -> Optional[str]:
        """가능한 컬럼명 중 실제 존재하는 컬럼 찾기"""
        for name in possible_names:
            if name in self.df.columns:
                return name
        return None

    def _get_period_string(self) -> str:
        """데이터 기간 문자열"""
        date_cols = self.df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) > 0:
            date_col = date_cols[0]
            start = self.df[date_col].min()
            end = self.df[date_col].max()
            return f"{start.strftime('%Y-%m-%d')} ~ {end.strftime('%Y-%m-%d')}"
        return ""


# 테스트 코드
if __name__ == "__main__":
    # 테스트 데이터 생성
    test_data = {
        '날짜': pd.date_range('2024-01-01', periods=100, freq='D'),
        '매출액': np.random.randint(100000, 1000000, 100),
        '비용': np.random.randint(50000, 500000, 100),
        '담당자': np.random.choice(['김철수', '이영희', '박민수'], 100),
        '센터': np.random.choice(['서울', '부산', '대전'], 100),
        '검사목적': np.random.choice(['정기검사', '품질관리', '신제품'], 100),
    }
    df = pd.DataFrame(test_data)

    # 시간 컬럼 추가
    df['year'] = df['날짜'].dt.year
    df['month'] = df['날짜'].dt.month
    df['quarter'] = df['날짜'].dt.quarter
    df['year_month'] = df['날짜'].dt.to_period('M').astype(str)
    df['year_quarter'] = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)

    # 계산
    calc = MetricsCalculator(df)
    metrics = calc.calculate_all_metrics(targets={'sales_target': 50000000})

    print("=== 요약 ===")
    print(metrics['summary'])
    print("\n=== 담당자별 ===")
    print(metrics['sales_by_manager'])
    print("\n=== KPI ===")
    print(metrics['kpi'])
