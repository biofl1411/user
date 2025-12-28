"""
엑셀 데이터 로더 모듈
- 엑셀/CSV 파일 불러오기
- 다중 시트 지원
- 컬럼 자동 매핑
"""
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, List, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataLoader:
    """엑셀/CSV 데이터 로더"""

    def __init__(self, column_mapping: Optional[Dict[str, List[str]]] = None):
        """
        Args:
            column_mapping: 표준 컬럼명 -> 가능한 엑셀 컬럼명 리스트 매핑
        """
        self.column_mapping = column_mapping or {}
        self.loaded_data: Dict[str, pd.DataFrame] = {}

    def load_excel(self, file_path: Union[str, Path],
                   sheet_name: Optional[Union[str, int, List]] = None) -> pd.DataFrame:
        """
        엑셀 파일 로드

        Args:
            file_path: 엑셀 파일 경로
            sheet_name: 시트명 또는 인덱스 (None이면 첫 번째 시트)

        Returns:
            DataFrame
        """
        file_path = Path(file_path)

        if not file_path.exists():
            raise FileNotFoundError(f"파일을 찾을 수 없습니다: {file_path}")

        try:
            # 엑셀 파일 확장자 확인
            if file_path.suffix.lower() in ['.xlsx', '.xlsm', '.xlsb']:
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine='openpyxl')
            elif file_path.suffix.lower() == '.xls':
                df = pd.read_excel(file_path, sheet_name=sheet_name, engine='xlrd')
            elif file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, encoding='utf-8-sig')
            else:
                raise ValueError(f"지원하지 않는 파일 형식: {file_path.suffix}")

            # 다중 시트인 경우 딕셔너리로 반환됨
            if isinstance(df, dict):
                logger.info(f"로드된 시트: {list(df.keys())}")
                self.loaded_data.update(df)
                # 첫 번째 시트 반환
                df = list(df.values())[0]

            logger.info(f"데이터 로드 완료: {len(df)} 행, {len(df.columns)} 열")
            return df

        except Exception as e:
            logger.error(f"파일 로드 오류: {e}")
            raise

    def load_multiple_files(self, file_paths: List[Union[str, Path]]) -> pd.DataFrame:
        """
        여러 엑셀 파일을 로드하여 하나로 합침

        Args:
            file_paths: 파일 경로 리스트

        Returns:
            합쳐진 DataFrame
        """
        dfs = []
        for file_path in file_paths:
            try:
                df = self.load_excel(file_path)
                # 파일명을 소스 컬럼으로 추가
                df['_source_file'] = Path(file_path).name
                dfs.append(df)
            except Exception as e:
                logger.warning(f"파일 로드 실패 ({file_path}): {e}")
                continue

        if not dfs:
            raise ValueError("로드된 데이터가 없습니다.")

        combined_df = pd.concat(dfs, ignore_index=True)
        logger.info(f"총 {len(combined_df)} 행 로드 완료")
        return combined_df

    def auto_detect_columns(self, df: pd.DataFrame) -> Dict[str, str]:
        """
        DataFrame 컬럼을 자동으로 매핑

        Args:
            df: 입력 DataFrame

        Returns:
            표준컬럼명 -> 실제컬럼명 매핑 딕셔너리
        """
        detected_mapping = {}
        df_columns = [str(col).strip() for col in df.columns]

        for standard_name, possible_names in self.column_mapping.items():
            for col in df_columns:
                # 정확히 일치하거나 포함되는 경우
                if col in possible_names or any(pn in col for pn in possible_names):
                    detected_mapping[standard_name] = col
                    break

        logger.info(f"자동 감지된 컬럼: {detected_mapping}")
        return detected_mapping

    def standardize_columns(self, df: pd.DataFrame,
                           column_map: Optional[Dict[str, str]] = None) -> pd.DataFrame:
        """
        컬럼명을 표준화

        Args:
            df: 입력 DataFrame
            column_map: 표준컬럼명 -> 실제컬럼명 매핑

        Returns:
            표준화된 DataFrame
        """
        if column_map is None:
            column_map = self.auto_detect_columns(df)

        # 역매핑 생성 (실제컬럼명 -> 표준컬럼명)
        reverse_map = {v: k for k, v in column_map.items()}

        # 컬럼명 변경
        df_standardized = df.rename(columns=reverse_map)

        return df_standardized

    def get_file_info(self, file_path: Union[str, Path]) -> Dict:
        """
        엑셀 파일 정보 조회

        Args:
            file_path: 파일 경로

        Returns:
            파일 정보 딕셔너리
        """
        file_path = Path(file_path)

        info = {
            "name": file_path.name,
            "size_kb": file_path.stat().st_size / 1024,
            "sheets": [],
        }

        try:
            if file_path.suffix.lower() in ['.xlsx', '.xlsm', '.xlsb']:
                xl = pd.ExcelFile(file_path, engine='openpyxl')
                info["sheets"] = xl.sheet_names
        except Exception as e:
            logger.warning(f"시트 정보 조회 실패: {e}")

        return info


# 테스트 코드
if __name__ == "__main__":
    from config.settings import COLUMN_MAPPING

    loader = DataLoader(column_mapping=COLUMN_MAPPING)

    # 예시: 엑셀 파일 로드
    # df = loader.load_excel("data/sales_202401.xlsx")
    # df = loader.standardize_columns(df)
    # print(df.head())
