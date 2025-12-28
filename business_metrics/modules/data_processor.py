"""
데이터 정제 및 전처리 모듈
- 공백/오류 제거
- 날짜 파싱
- 월/분기/연도 컬럼 생성
- 담당자/센터/검사목적 매핑
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class DataProcessor:
    """데이터 정제 및 전처리"""

    def __init__(self):
        self.original_df: Optional[pd.DataFrame] = None
        self.processed_df: Optional[pd.DataFrame] = None

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        전체 전처리 파이프라인 실행

        Args:
            df: 원본 DataFrame

        Returns:
            정제된 DataFrame
        """
        self.original_df = df.copy()

        # 1. 기본 정제
        df = self.clean_data(df)

        # 2. 날짜 컬럼 처리
        df = self.process_dates(df)

        # 3. 시간 기반 컬럼 추가
        df = self.add_time_columns(df)

        # 4. 숫자 컬럼 정제
        df = self.clean_numeric_columns(df)

        # 5. 카테고리 컬럼 정제
        df = self.clean_category_columns(df)

        self.processed_df = df
        logger.info(f"데이터 전처리 완료: {len(df)} 행")
        return df

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """기본 데이터 정제"""
        # 빈 행 제거
        df = df.dropna(how='all')

        # 문자열 공백 제거
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace(['nan', 'None', '', 'NaN'], np.nan)

        # 중복 행 제거 (옵션)
        initial_count = len(df)
        df = df.drop_duplicates()
        if len(df) < initial_count:
            logger.info(f"중복 행 {initial_count - len(df)}개 제거됨")

        return df

    def process_dates(self, df: pd.DataFrame,
                     date_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """날짜 컬럼 처리"""
        if date_columns is None:
            # 날짜로 추정되는 컬럼 자동 탐지
            date_columns = [col for col in df.columns
                           if any(kw in str(col).lower()
                                 for kw in ['date', '날짜', '일자', '일시'])]

        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                    logger.info(f"날짜 컬럼 변환: {col}")
                except Exception as e:
                    logger.warning(f"날짜 변환 실패 ({col}): {e}")

        return df

    def add_time_columns(self, df: pd.DataFrame,
                        date_column: str = 'date') -> pd.DataFrame:
        """시간 기반 분석 컬럼 추가"""
        if date_column not in df.columns:
            # 날짜 컬럼 찾기
            date_cols = [col for col in df.columns
                        if df[col].dtype == 'datetime64[ns]']
            if date_cols:
                date_column = date_cols[0]
            else:
                logger.warning("날짜 컬럼을 찾을 수 없습니다.")
                return df

        if df[date_column].dtype != 'datetime64[ns]':
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')

        # 시간 기반 컬럼 추가
        df['year'] = df[date_column].dt.year
        df['month'] = df[date_column].dt.month
        df['quarter'] = df[date_column].dt.quarter
        df['year_month'] = df[date_column].dt.to_period('M').astype(str)
        df['year_quarter'] = df['year'].astype(str) + '-Q' + df['quarter'].astype(str)
        df['weekday'] = df[date_column].dt.day_name()
        df['day_of_week'] = df[date_column].dt.dayofweek  # 0=월요일

        logger.info("시간 기반 컬럼 추가 완료")
        return df

    def clean_numeric_columns(self, df: pd.DataFrame,
                             numeric_columns: Optional[List[str]] = None) -> pd.DataFrame:
        """숫자 컬럼 정제"""
        if numeric_columns is None:
            # 금액/수량 관련 컬럼 추정
            numeric_keywords = ['금액', '매출', '비용', '이익', '건수', '수량',
                               'amount', 'price', 'cost', 'count', 'qty']
            numeric_columns = [col for col in df.columns
                              if any(kw in str(col).lower() for kw in numeric_keywords)]

        for col in numeric_columns:
            if col in df.columns:
                try:
                    # 문자열인 경우 숫자로 변환
                    if df[col].dtype == 'object':
                        # 쉼표, 원화 기호 등 제거
                        df[col] = df[col].astype(str).str.replace(',', '')
                        df[col] = df[col].str.replace('원', '')
                        df[col] = df[col].str.replace('₩', '')
                        df[col] = df[col].str.replace(' ', '')

                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    # 결측치는 0으로 채움
                    df[col] = df[col].fillna(0)

                except Exception as e:
                    logger.warning(f"숫자 변환 실패 ({col}): {e}")

        return df

    def clean_category_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """카테고리 컬럼 정제"""
        category_keywords = ['담당', '센터', '지점', '부서', '목적', '유형', '분류']
        category_columns = [col for col in df.columns
                           if any(kw in str(col) for kw in category_keywords)]

        for col in category_columns:
            if col in df.columns:
                # 공백 및 특수문자 정리
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace(['nan', 'None', ''], '미지정')

        return df

    def add_calculated_columns(self, df: pd.DataFrame,
                               sales_col: str = 'sales_amount',
                               cost_col: str = 'cost') -> pd.DataFrame:
        """계산 컬럼 추가"""
        # 이익 = 매출 - 비용
        if sales_col in df.columns and cost_col in df.columns:
            df['calculated_profit'] = df[sales_col] - df[cost_col]
            df['profit_margin'] = (df['calculated_profit'] / df[sales_col] * 100).round(2)
            df['profit_margin'] = df['profit_margin'].replace([np.inf, -np.inf], 0)

        return df

    def filter_date_range(self, df: pd.DataFrame,
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         date_column: str = 'date') -> pd.DataFrame:
        """날짜 범위 필터링"""
        if date_column not in df.columns:
            return df

        if start_date:
            df = df[df[date_column] >= pd.to_datetime(start_date)]

        if end_date:
            df = df[df[date_column] <= pd.to_datetime(end_date)]

        return df

    def get_summary(self, df: pd.DataFrame) -> Dict:
        """데이터 요약 정보"""
        summary = {
            "total_rows": len(df),
            "columns": list(df.columns),
            "date_range": None,
            "numeric_columns": list(df.select_dtypes(include=[np.number]).columns),
            "category_columns": list(df.select_dtypes(include=['object']).columns),
        }

        # 날짜 범위
        date_cols = df.select_dtypes(include=['datetime64']).columns
        if len(date_cols) > 0:
            date_col = date_cols[0]
            summary["date_range"] = {
                "start": str(df[date_col].min()),
                "end": str(df[date_col].max()),
            }

        return summary


# 테스트 코드
if __name__ == "__main__":
    processor = DataProcessor()

    # 예시 데이터
    test_data = {
        '날짜': ['2024-01-15', '2024-02-20', '2024-03-10'],
        '매출액': ['1,000,000', '2,500,000', '1,800,000'],
        '담당자': ['김철수', '이영희', '박민수'],
        '센터': ['서울', '부산', '대전'],
    }
    df = pd.DataFrame(test_data)

    processed = processor.process(df)
    print(processed)
    print(processor.get_summary(processed))
