"""
경영지표 대시보드 (Flask 버전)
- 오래된 CPU에서도 작동
- Chart.js 사용
- 연도 비교, 검사목적 필터, 업체별 분석, 부적합항목 분석
- AI 분석 (Google Gemini API)
"""
from flask import Flask, render_template_string, jsonify, request
import os
from pathlib import Path
from datetime import datetime
import json

app = Flask(__name__)

# Gemini API 설정 (여러 키로 429 에러 대응)
GEMINI_API_KEYS = [
    os.environ.get('GEMINI_API_KEY', ''),
    os.environ.get('GEMINI_API_KEY_2', 'AIzaSyA7saUcePkpMh3olwkKKG7z-u1XXcDc7u4'),  # 경영지표1
    os.environ.get('GEMINI_API_KEY_3', 'AIzaSyCo8k3H7Pi128OuBgcupa7jlcm-hH1q68g'),  # 경영지표2
]
GEMINI_API_KEYS = [k for k in GEMINI_API_KEYS if k]  # 빈 키 제거
current_api_key_index = 0  # 현재 사용 중인 키 인덱스

# 경로 설정 - 절대 경로 사용
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path("/home/biofl/business_metrics/data")
CACHE_FILE = BASE_DIR / "data_cache.pkl"  # 파일 캐시 경로
SQLITE_DB = DATA_DIR / "business_data.db"  # SQLite 데이터베이스 경로

# 데이터 캐시 (메모리에 저장)
DATA_CACHE = {}
CACHE_TIME = {}
FILE_MTIME = {}  # 파일 수정 시간 추적
AI_SUMMARY_CACHE = {}  # AI용 데이터 요약 캐시
USE_SQLITE = True  # SQLite 사용 여부


def init_sqlite_db():
    """SQLite 데이터베이스 초기화"""
    import sqlite3

    conn = sqlite3.connect(str(SQLITE_DB))
    cursor = conn.cursor()

    # 기본 데이터 테이블 (연도별 Excel 데이터)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS excel_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year TEXT,
            접수번호 TEXT,
            접수일자 TEXT,
            발행일 TEXT,
            검체유형 TEXT,
            업체명 TEXT,
            의뢰인명 TEXT,
            업체주소 TEXT,
            영업담당 TEXT,
            검사목적 TEXT,
            총금액 REAL,
            raw_data TEXT
        )
    ''')

    # food_item 데이터 테이블
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS food_item_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            year TEXT,
            접수일자 TEXT,
            발행일 TEXT,
            검체유형 TEXT,
            업체명 TEXT,
            의뢰인명 TEXT,
            업체주소 TEXT,
            항목명 TEXT,
            규격 TEXT,
            항목담당 TEXT,
            결과입력자 TEXT,
            입력일 TEXT,
            분석일 TEXT,
            항목단위 TEXT,
            시험결과 TEXT,
            시험치 TEXT,
            성적서결과 TEXT,
            판정 TEXT,
            검사목적 TEXT,
            긴급여부 TEXT,
            항목수수료 REAL,
            영업담당 TEXT
        )
    ''')

    # 메타데이터 테이블 (파일 수정 시간 추적)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_metadata (
            file_path TEXT PRIMARY KEY,
            mtime REAL,
            row_count INTEGER
        )
    ''')

    # 인덱스 생성 (빠른 검색용)
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_excel_year ON excel_data(year)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_excel_manager ON excel_data(영업담당)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_excel_purpose ON excel_data(검사목적)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_food_year ON food_item_data(year)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_food_manager ON food_item_data(영업담당)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_food_purpose ON food_item_data(검사목적)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_food_item ON food_item_data(항목명)')

    conn.commit()
    conn.close()
    print("[SQLITE] 데이터베이스 초기화 완료")


def check_sqlite_needs_update():
    """SQLite DB 업데이트 필요 여부 확인"""
    import sqlite3

    if not SQLITE_DB.exists():
        return True

    try:
        conn = sqlite3.connect(str(SQLITE_DB))
        cursor = conn.cursor()

        # 모든 Excel 파일의 현재 mtime 확인
        for year in ['2024', '2025']:
            # 기본 데이터
            data_path = DATA_DIR / str(year)
            if data_path.exists():
                for f in sorted(data_path.glob("*.xlsx")):
                    file_path = str(f)
                    current_mtime = f.stat().st_mtime

                    cursor.execute('SELECT mtime FROM file_metadata WHERE file_path = ?', (file_path,))
                    row = cursor.fetchone()

                    if not row or row[0] < current_mtime:
                        conn.close()
                        print(f"[SQLITE] 업데이트 필요: {f.name}")
                        return True

            # food_item 데이터
            food_path = DATA_DIR / "food_item" / str(year)
            if food_path.exists():
                for f in sorted(food_path.glob("*.xlsx")):
                    file_path = str(f)
                    current_mtime = f.stat().st_mtime

                    cursor.execute('SELECT mtime FROM file_metadata WHERE file_path = ?', (file_path,))
                    row = cursor.fetchone()

                    if not row or row[0] < current_mtime:
                        conn.close()
                        print(f"[SQLITE] 업데이트 필요: {f.name}")
                        return True

        conn.close()
        return False

    except Exception as e:
        print(f"[SQLITE] 체크 오류: {e}")
        return True


def convert_excel_to_sqlite():
    """Excel 파일을 SQLite로 변환"""
    import sqlite3
    import time
    from openpyxl import load_workbook

    print("[SQLITE] Excel → SQLite 변환 시작...")
    start_time = time.time()

    init_sqlite_db()
    conn = sqlite3.connect(str(SQLITE_DB))
    cursor = conn.cursor()

    total_records = 0

    for year in ['2024', '2025']:
        # 기본 데이터 변환
        data_path = DATA_DIR / str(year)
        if data_path.exists():
            for f in sorted(data_path.glob("*.xlsx")):
                file_path = str(f)
                current_mtime = f.stat().st_mtime

                # 이미 변환된 파일인지 확인
                cursor.execute('SELECT mtime FROM file_metadata WHERE file_path = ?', (file_path,))
                row = cursor.fetchone()
                if row and row[0] >= current_mtime:
                    print(f"[SQLITE] {f.name} 스킵 (이미 최신)")
                    continue

                # 기존 데이터 삭제 후 재삽입
                cursor.execute('DELETE FROM excel_data WHERE year = ? AND raw_data LIKE ?',
                              (year, f'%{f.name}%'))

                try:
                    wb = load_workbook(f, read_only=True, data_only=True)
                    ws = wb.active
                    headers = [cell.value for cell in ws[1]]

                    batch = []
                    for row_data in ws.iter_rows(min_row=2, values_only=True):
                        row_dict = dict(zip(headers, row_data))
                        batch.append((
                            year,
                            str(row_dict.get('접수번호', '')),
                            str(row_dict.get('접수일자', '')),
                            str(row_dict.get('발행일', '')),
                            str(row_dict.get('검체유형', '')),
                            str(row_dict.get('업체명', '')),
                            str(row_dict.get('의뢰인명', '')),
                            str(row_dict.get('업체주소', '')),
                            str(row_dict.get('영업담당', '')),
                            str(row_dict.get('검사목적', '')),
                            float(row_dict.get('총금액', 0) or 0),
                            json.dumps(row_dict, ensure_ascii=False, default=str)
                        ))

                    cursor.executemany('''
                        INSERT INTO excel_data
                        (year, 접수번호, 접수일자, 발행일, 검체유형, 업체명, 의뢰인명, 업체주소, 영업담당, 검사목적, 총금액, raw_data)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', batch)

                    # 메타데이터 업데이트
                    cursor.execute('''
                        INSERT OR REPLACE INTO file_metadata (file_path, mtime, row_count)
                        VALUES (?, ?, ?)
                    ''', (file_path, current_mtime, len(batch)))

                    wb.close()
                    total_records += len(batch)
                    print(f"[SQLITE] {f.name}: {len(batch)}건 변환")

                except Exception as e:
                    print(f"[SQLITE ERROR] {f.name}: {e}")

        # food_item 데이터 변환
        food_path = DATA_DIR / "food_item" / str(year)
        if food_path.exists():
            for f in sorted(food_path.glob("*.xlsx")):
                file_path = str(f)
                current_mtime = f.stat().st_mtime

                cursor.execute('SELECT mtime FROM file_metadata WHERE file_path = ?', (file_path,))
                row = cursor.fetchone()
                if row and row[0] >= current_mtime:
                    print(f"[SQLITE] food_item {f.name} 스킵 (이미 최신)")
                    continue

                # 파일명 기반으로 삭제 (월별 데이터)
                month = f.stem.split('_')[-1] if '_' in f.stem else f.stem
                cursor.execute('DELETE FROM food_item_data WHERE year = ?', (year,))

                try:
                    wb = load_workbook(f, read_only=True, data_only=True)
                    ws = wb.active
                    headers = [cell.value for cell in ws[1]]

                    required_columns = ['접수일자', '발행일', '검체유형', '업체명', '의뢰인명', '업체주소',
                                       '항목명', '규격', '항목담당', '결과입력자', '입력일', '분석일',
                                       '항목단위', '시험결과', '시험치', '성적서결과', '판정', '검사목적',
                                       '긴급여부', '항목수수료', '영업담당']

                    col_indices = {}
                    for i, h in enumerate(headers):
                        if h in required_columns:
                            col_indices[h] = i

                    batch = []
                    for row_data in ws.iter_rows(min_row=2, values_only=True):
                        row_dict = {}
                        for col_name, idx in col_indices.items():
                            row_dict[col_name] = row_data[idx] if idx < len(row_data) else None

                        batch.append((
                            year,
                            str(row_dict.get('접수일자', '') or ''),
                            str(row_dict.get('발행일', '') or ''),
                            str(row_dict.get('검체유형', '') or ''),
                            str(row_dict.get('업체명', '') or ''),
                            str(row_dict.get('의뢰인명', '') or ''),
                            str(row_dict.get('업체주소', '') or ''),
                            str(row_dict.get('항목명', '') or ''),
                            str(row_dict.get('규격', '') or ''),
                            str(row_dict.get('항목담당', '') or ''),
                            str(row_dict.get('결과입력자', '') or ''),
                            str(row_dict.get('입력일', '') or ''),
                            str(row_dict.get('분석일', '') or ''),
                            str(row_dict.get('항목단위', '') or ''),
                            str(row_dict.get('시험결과', '') or ''),
                            str(row_dict.get('시험치', '') or ''),
                            str(row_dict.get('성적서결과', '') or ''),
                            str(row_dict.get('판정', '') or ''),
                            str(row_dict.get('검사목적', '') or ''),
                            str(row_dict.get('긴급여부', '') or ''),
                            float(row_dict.get('항목수수료', 0) or 0),
                            str(row_dict.get('영업담당', '') or '')
                        ))

                    cursor.executemany('''
                        INSERT INTO food_item_data
                        (year, 접수일자, 발행일, 검체유형, 업체명, 의뢰인명, 업체주소, 항목명, 규격,
                         항목담당, 결과입력자, 입력일, 분석일, 항목단위, 시험결과, 시험치, 성적서결과,
                         판정, 검사목적, 긴급여부, 항목수수료, 영업담당)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', batch)

                    cursor.execute('''
                        INSERT OR REPLACE INTO file_metadata (file_path, mtime, row_count)
                        VALUES (?, ?, ?)
                    ''', (file_path, current_mtime, len(batch)))

                    wb.close()
                    total_records += len(batch)
                    print(f"[SQLITE] food_item {f.name}: {len(batch)}건 변환")

                except Exception as e:
                    print(f"[SQLITE ERROR] food_item {f.name}: {e}")

    conn.commit()
    conn.close()

    elapsed = time.time() - start_time
    print(f"[SQLITE] 변환 완료! 총 {total_records:,}건, {elapsed:.1f}초 소요")


def load_excel_data_sqlite(year):
    """SQLite에서 데이터 로드 (빠름)"""
    import sqlite3
    import time

    start_time = time.time()

    conn = sqlite3.connect(str(SQLITE_DB))
    cursor = conn.cursor()

    cursor.execute('SELECT raw_data FROM excel_data WHERE year = ?', (str(year),))
    rows = cursor.fetchall()

    data = []
    for row in rows:
        try:
            data.append(json.loads(row[0]))
        except:
            pass

    conn.close()

    elapsed = time.time() - start_time
    print(f"[SQLITE] {year}년 데이터 로드: {len(data):,}건, {elapsed:.2f}초")

    return data


def load_food_item_data_sqlite(year):
    """SQLite에서 food_item 데이터 로드 (빠름)"""
    import sqlite3
    import time

    start_time = time.time()

    conn = sqlite3.connect(str(SQLITE_DB))
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute('''
        SELECT 접수일자, 발행일, 검체유형, 업체명, 의뢰인명, 업체주소, 항목명, 규격,
               항목담당, 결과입력자, 입력일, 분석일, 항목단위, 시험결과, 시험치,
               성적서결과, 판정, 검사목적, 긴급여부, 항목수수료, 영업담당
        FROM food_item_data WHERE year = ?
    ''', (str(year),))

    rows = cursor.fetchall()
    data = [dict(row) for row in rows]

    conn.close()

    elapsed = time.time() - start_time
    print(f"[SQLITE] food_item {year}년 데이터 로드: {len(data):,}건, {elapsed:.2f}초")

    return data


def get_data_files_mtime():
    """모든 데이터 파일의 최신 수정 시간 반환"""
    latest_mtime = 0
    for year in ['2024', '2025']:
        data_path = DATA_DIR / str(year)
        if data_path.exists():
            for f in data_path.glob("*.xlsx"):
                mtime = f.stat().st_mtime
                if mtime > latest_mtime:
                    latest_mtime = mtime
        food_path = DATA_DIR / "food_item" / str(year)
        if food_path.exists():
            for f in food_path.glob("*.xlsx"):
                mtime = f.stat().st_mtime
                if mtime > latest_mtime:
                    latest_mtime = mtime
    return latest_mtime


def load_cache_from_file():
    """파일에서 캐시 로드 (서버 시작 시)"""
    global DATA_CACHE, CACHE_TIME, FILE_MTIME, AI_SUMMARY_CACHE
    import pickle

    if not CACHE_FILE.exists():
        print("[CACHE] 캐시 파일 없음 - 새로 생성 필요")
        return False

    try:
        # 데이터 파일 수정 시간 확인
        current_mtime = get_data_files_mtime()
        cache_mtime = CACHE_FILE.stat().st_mtime

        # 캐시가 데이터보다 오래된 경우 무효화
        if current_mtime > cache_mtime:
            print(f"[CACHE] 데이터 파일이 캐시보다 최신 - 다시 로드 필요")
            return False

        with open(CACHE_FILE, 'rb') as f:
            cached = pickle.load(f)

        DATA_CACHE = cached.get('DATA_CACHE', {})
        CACHE_TIME = cached.get('CACHE_TIME', {})
        FILE_MTIME = cached.get('FILE_MTIME', {})
        AI_SUMMARY_CACHE = cached.get('AI_SUMMARY_CACHE', {})

        # 캐시 시간 업데이트 (현재 시간 기준으로)
        import time
        current_time = time.time()
        for key in CACHE_TIME:
            CACHE_TIME[key] = current_time

        total_records = sum(len(v) for v in DATA_CACHE.values() if isinstance(v, list))
        print(f"[CACHE] 파일에서 캐시 로드 완료 ({total_records:,}건)")
        return True

    except Exception as e:
        print(f"[CACHE] 파일 캐시 로드 실패: {e}")
        return False


def save_cache_to_file():
    """캐시를 파일로 저장"""
    import pickle

    try:
        cached = {
            'DATA_CACHE': DATA_CACHE,
            'CACHE_TIME': CACHE_TIME,
            'FILE_MTIME': FILE_MTIME,
            'AI_SUMMARY_CACHE': AI_SUMMARY_CACHE
        }
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump(cached, f)
        print(f"[CACHE] 파일로 캐시 저장 완료")
    except Exception as e:
        print(f"[CACHE] 파일 캐시 저장 실패: {e}")

# 설정
MANAGER_TO_BRANCH = {
    "장동욱": "충청지사", "지병훈": "충청지사", "박은태": "충청지사",
    "도준구": "경북지사",
    "이강현": "전북지사",
    "엄은정": "경기지사", "정유경": "경기지사",
    "이성복": "서울지사",
    "조봉현": "서울센터", "오세중": "서울센터", "장동주": "서울센터", "오석현": "서울센터",
    "엄상흠": "경북센터",
    "마케팅": "마케팅",
    "본사접수": "본사접수",
}

# 개인별 분석에서 제외할 영업담당 (외부 기관 등)
EXCLUDED_MANAGERS = {"ISA", "IBK", "미지정"}

def load_excel_data(year, use_cache=True):
    """데이터 로드 (SQLite 우선, 없으면 Excel)"""
    import time

    # 캐시 확인 (1시간 유효)
    cache_key = str(year)
    if use_cache and cache_key in DATA_CACHE:
        cache_age = time.time() - CACHE_TIME.get(cache_key, 0)
        if cache_age < 3600:  # 1시간
            print(f"[CACHE] {year}년 데이터 캐시 사용 ({len(DATA_CACHE[cache_key])}건)")
            return DATA_CACHE[cache_key]

    # SQLite 사용 (DB가 존재하면)
    if USE_SQLITE and SQLITE_DB.exists():
        all_data = load_excel_data_sqlite(year)
        DATA_CACHE[cache_key] = all_data
        CACHE_TIME[cache_key] = time.time()
        return all_data

    # 기존 Excel 로드 방식 (폴백)
    from openpyxl import load_workbook

    data_path = DATA_DIR / str(year)
    if not data_path.exists():
        return []

    print(f"[LOAD] {year}년 데이터 로딩 시작 (Excel)...")
    start_time = time.time()

    all_data = []
    files = sorted(data_path.glob("*.xlsx"))

    for f in files:
        try:
            wb = load_workbook(f, read_only=True, data_only=True)
            ws = wb.active
            headers = [cell.value for cell in ws[1]]

            for row in ws.iter_rows(min_row=2, values_only=True):
                row_dict = dict(zip(headers, row))
                all_data.append(row_dict)
            wb.close()
            print(f"[LOAD] {f.name} 완료")
        except Exception as e:
            print(f"[ERROR] Loading {f}: {e}")

    elapsed = time.time() - start_time
    print(f"[LOAD] {year}년 완료: {len(all_data)}건, {elapsed:.1f}초 소요")

    # 캐시 저장
    DATA_CACHE[cache_key] = all_data
    CACHE_TIME[cache_key] = time.time()

    return all_data

def load_food_item_data(year, use_cache=True):
    """food_item 데이터 로드 (SQLite 우선, 없으면 Excel)"""
    import time

    cache_key = f"food_item_{year}"
    if use_cache and cache_key in DATA_CACHE:
        cache_age = time.time() - CACHE_TIME.get(cache_key, 0)
        if cache_age < 3600:
            print(f"[CACHE] food_item {year}년 데이터 캐시 사용 ({len(DATA_CACHE[cache_key])}건)")
            return DATA_CACHE[cache_key]

    # SQLite 사용 (DB가 존재하면)
    if USE_SQLITE and SQLITE_DB.exists():
        all_data = load_food_item_data_sqlite(year)
        DATA_CACHE[cache_key] = all_data
        CACHE_TIME[cache_key] = time.time()
        return all_data

    # 기존 Excel 로드 방식 (폴백)
    from openpyxl import load_workbook

    data_path = DATA_DIR / "food_item" / str(year)
    if not data_path.exists():
        print(f"[WARN] food_item {year}년 폴더 없음: {data_path}")
        return []

    print(f"[LOAD] food_item {year}년 데이터 로딩 시작 (Excel)...")
    start_time = time.time()

    # 필요한 컬럼만 로드
    required_columns = ['접수일자', '발행일', '검체유형', '업체명', '의뢰인명', '업체주소',
                       '항목명', '규격', '항목담당', '결과입력자', '입력일', '분석일',
                       '항목단위', '시험결과', '시험치', '성적서결과', '판정', '검사목적',
                       '긴급여부', '항목수수료', '영업담당']

    all_data = []
    files = sorted(data_path.glob("*.xlsx"))

    for f in files:
        try:
            wb = load_workbook(f, read_only=True, data_only=True)
            ws = wb.active
            headers = [cell.value for cell in ws[1]]

            # 컬럼 인덱스 매핑
            col_indices = {}
            for i, h in enumerate(headers):
                if h in required_columns:
                    col_indices[h] = i

            for row in ws.iter_rows(min_row=2, values_only=True):
                row_dict = {}
                for col_name, idx in col_indices.items():
                    row_dict[col_name] = row[idx] if idx < len(row) else None
                all_data.append(row_dict)
            wb.close()
            print(f"[LOAD] food_item {f.name} 완료")
        except Exception as e:
            print(f"[ERROR] Loading food_item {f}: {e}")

    elapsed = time.time() - start_time
    print(f"[LOAD] food_item {year}년 완료: {len(all_data)}건, {elapsed:.1f}초 소요")

    DATA_CACHE[cache_key] = all_data
    CACHE_TIME[cache_key] = time.time()

    return all_data


def check_data_changed(year):
    """데이터 파일 변경 감지"""
    data_path = DATA_DIR / str(year)
    if not data_path.exists():
        return False

    files = sorted(data_path.glob("*.xlsx"))
    current_mtimes = {}

    for f in files:
        current_mtimes[str(f)] = f.stat().st_mtime

    cache_key = f"mtime_{year}"
    old_mtimes = FILE_MTIME.get(cache_key, {})

    if current_mtimes != old_mtimes:
        FILE_MTIME[cache_key] = current_mtimes
        return True

    return False


def get_ai_data_summary(force_refresh=False):
    """AI 분석용 데이터 요약 생성 (캐시됨)"""
    import time

    cache_key = 'ai_summary'

    # 데이터 변경 확인
    data_changed = check_data_changed('2024') or check_data_changed('2025')

    # 캐시 유효성 확인 (1시간 또는 데이터 변경 시)
    if not force_refresh and cache_key in AI_SUMMARY_CACHE:
        cache_age = time.time() - AI_SUMMARY_CACHE.get('_time', 0)
        if cache_age < 3600 and not data_changed:
            print(f"[AI-CACHE] 요약 캐시 사용 (나이: {cache_age:.0f}초)")
            return AI_SUMMARY_CACHE[cache_key]

    print(f"[AI-CACHE] 데이터 요약 생성 중...")
    start_time = time.time()

    # 데이터 로드
    food_2024 = load_food_item_data('2024')
    food_2025 = load_food_item_data('2025')

    # 요약 통계 계산
    summary = {
        '2024': {'total_count': 0, 'total_fee': 0, 'by_purpose': {}, 'by_sample_type': {},
                 'by_manager': {}, 'by_item': {}, 'monthly': {}},
        '2025': {'total_count': 0, 'total_fee': 0, 'by_purpose': {}, 'by_sample_type': {},
                 'by_manager': {}, 'by_item': {}, 'monthly': {}},
        'filter_values': {'purposes': set(), 'sample_types': set(), 'items': set(), 'managers': set()}
    }

    for year, data in [('2024', food_2024), ('2025', food_2025)]:
        for row in data:
            purpose = str(row.get('검사목적', '') or '').strip()
            sample_type = str(row.get('검체유형', '') or '').strip()
            item_name = str(row.get('항목명', '') or '').strip()
            manager = str(row.get('영업담당', '') or '').strip() or '미지정'
            fee = row.get('항목수수료', 0) or 0
            date = row.get('접수일자')

            if isinstance(fee, str):
                fee = float(fee.replace(',', '').replace('원', '')) if fee else 0

            summary[year]['total_count'] += 1
            summary[year]['total_fee'] += fee

            # 목적별
            if purpose:
                if purpose not in summary[year]['by_purpose']:
                    summary[year]['by_purpose'][purpose] = {'count': 0, 'fee': 0}
                summary[year]['by_purpose'][purpose]['count'] += 1
                summary[year]['by_purpose'][purpose]['fee'] += fee
                summary['filter_values']['purposes'].add(purpose)

            # 검체유형별
            if sample_type:
                if sample_type not in summary[year]['by_sample_type']:
                    summary[year]['by_sample_type'][sample_type] = {'count': 0, 'fee': 0}
                summary[year]['by_sample_type'][sample_type]['count'] += 1
                summary[year]['by_sample_type'][sample_type]['fee'] += fee
                summary['filter_values']['sample_types'].add(sample_type)

            # 영업담당별
            if manager not in summary[year]['by_manager']:
                summary[year]['by_manager'][manager] = {'count': 0, 'fee': 0}
            summary[year]['by_manager'][manager]['count'] += 1
            summary[year]['by_manager'][manager]['fee'] += fee
            summary['filter_values']['managers'].add(manager)

            # 항목별 (TOP 50만)
            if item_name:
                if item_name not in summary[year]['by_item']:
                    summary[year]['by_item'][item_name] = {'count': 0, 'fee': 0}
                summary[year]['by_item'][item_name]['count'] += 1
                summary[year]['by_item'][item_name]['fee'] += fee
                summary['filter_values']['items'].add(item_name)

            # 월별
            if date and hasattr(date, 'month'):
                m = date.month
                if m not in summary[year]['monthly']:
                    summary[year]['monthly'][m] = {'count': 0, 'fee': 0}
                summary[year]['monthly'][m]['count'] += 1
                summary[year]['monthly'][m]['fee'] += fee

    # set을 sorted list로 변환
    summary['filter_values']['purposes'] = sorted(summary['filter_values']['purposes'])
    summary['filter_values']['sample_types'] = sorted(summary['filter_values']['sample_types'])
    summary['filter_values']['items'] = sorted(summary['filter_values']['items'])[:100]  # 상위 100개만
    # ISA, IBK 등 제외 대상은 필터 목록에서 제외
    summary['filter_values']['managers'] = sorted([m for m in summary['filter_values']['managers'] if m not in EXCLUDED_MANAGERS])

    # 항목별 데이터 정렬 (상위 50개만 유지)
    for year in ['2024', '2025']:
        sorted_items = sorted(summary[year]['by_item'].items(),
                             key=lambda x: x[1]['fee'], reverse=True)[:50]
        summary[year]['by_item'] = dict(sorted_items)

    elapsed = time.time() - start_time
    print(f"[AI-CACHE] 요약 생성 완료: {elapsed:.1f}초 소요")

    AI_SUMMARY_CACHE[cache_key] = summary
    AI_SUMMARY_CACHE['_time'] = time.time()

    return summary


def process_food_item_data(data, purpose_filter=None, sample_type_filter=None,
                           item_filter=None, manager_filter=None):
    """검사항목 데이터 처리"""
    by_item = {}  # 항목별 데이터
    by_item_month = {}  # 항목별-월별 데이터
    by_item_analyzer = {}  # 항목별-분석자 데이터
    by_sample_type_item = {}  # 검체유형별-항목 데이터
    by_manager_item = {}  # 영업담당별-항목 데이터
    by_manager_fee = {}  # 영업담당별-수수료 데이터
    by_month_fee = {}  # 월별-수수료 데이터
    by_purpose_sample_type = {}  # 검사목적별-검체유형 매핑
    by_purpose_sample_type_item = {}  # 검사목적+검체유형별-항목 매핑

    purposes = set()
    sample_types = set()
    items = set()
    managers = set()
    analyzers = set()

    total_fee = 0
    total_count = 0

    for row in data:
        purpose = str(row.get('검사목적', '') or '').strip()
        sample_type = str(row.get('검체유형', '') or '').strip()
        item_name = str(row.get('항목명', '') or '').strip()
        manager = str(row.get('영업담당', '') or '').strip() or '미지정'
        analyzer = str(row.get('결과입력자', '') or '').strip() or '미지정'
        fee = row.get('항목수수료', 0) or 0
        date = row.get('접수일자')

        if isinstance(fee, str):
            fee = float(fee.replace(',', '').replace('원', '')) if fee else 0

        # 목록 수집
        if purpose: purposes.add(purpose)
        if sample_type: sample_types.add(sample_type)
        if item_name: items.add(item_name)
        if manager and manager != '미지정': managers.add(manager)
        if analyzer and analyzer != '미지정': analyzers.add(analyzer)

        # 검사목적별-검체유형 매핑 수집
        if purpose and sample_type:
            if purpose not in by_purpose_sample_type:
                by_purpose_sample_type[purpose] = set()
            by_purpose_sample_type[purpose].add(sample_type)

        # 검사목적+검체유형별-항목 매핑 수집 (잔류농약, 항생물질 제외)
        if purpose and sample_type and item_name:
            if not (sample_type.startswith('잔류농약') or sample_type.startswith('항생물질')):
                key = f"{purpose}|{sample_type}"
                if key not in by_purpose_sample_type_item:
                    by_purpose_sample_type_item[key] = set()
                by_purpose_sample_type_item[key].add(item_name)

        # 필터 적용
        if purpose_filter and purpose_filter != '전체' and purpose != purpose_filter:
            continue
        # 검체유형 필터 (와일드카드 지원)
        if sample_type_filter and sample_type_filter != '전체':
            if sample_type_filter.endswith('*'):
                # 와일드카드 패턴: "잔류농약*" -> 잔류농약으로 시작하는 모든 유형 매칭
                prefix = sample_type_filter[:-1]  # '*' 제거
                if not sample_type.startswith(prefix):
                    continue
            elif sample_type != sample_type_filter:
                continue
        if item_filter and item_filter != '전체' and item_name != item_filter:
            continue
        if manager_filter and manager_filter != '전체' and manager != manager_filter:
            continue

        # 월 추출
        month = 0
        if date:
            if hasattr(date, 'month'):
                month = date.month
            else:
                try:
                    month = int(str(date).split('-')[1])
                except:
                    month = 0

        total_fee += fee
        total_count += 1

        # 항목별 집계
        if item_name:
            if item_name not in by_item:
                by_item[item_name] = {'count': 0, 'fee': 0}
            by_item[item_name]['count'] += 1
            by_item[item_name]['fee'] += fee

            # 항목별-월별
            if month > 0:
                if item_name not in by_item_month:
                    by_item_month[item_name] = {}
                if month not in by_item_month[item_name]:
                    by_item_month[item_name][month] = 0
                by_item_month[item_name][month] += 1

            # 항목별-분석자
            if item_name not in by_item_analyzer:
                by_item_analyzer[item_name] = {}
            if analyzer not in by_item_analyzer[item_name]:
                by_item_analyzer[item_name][analyzer] = {'count': 0, 'fee': 0}
            by_item_analyzer[item_name][analyzer]['count'] += 1
            by_item_analyzer[item_name][analyzer]['fee'] += fee

        # 검체유형별-항목
        if sample_type:
            if sample_type not in by_sample_type_item:
                by_sample_type_item[sample_type] = {}
            if item_name:
                if item_name not in by_sample_type_item[sample_type]:
                    by_sample_type_item[sample_type][item_name] = {'count': 0, 'fee': 0}
                by_sample_type_item[sample_type][item_name]['count'] += 1
                by_sample_type_item[sample_type][item_name]['fee'] += fee

        # 영업담당별 집계
        if manager not in by_manager_item:
            by_manager_item[manager] = {'count': 0, 'fee': 0, 'items': {}}
        by_manager_item[manager]['count'] += 1
        by_manager_item[manager]['fee'] += fee
        if item_name:
            if item_name not in by_manager_item[manager]['items']:
                by_manager_item[manager]['items'][item_name] = {'count': 0, 'fee': 0}
            by_manager_item[manager]['items'][item_name]['count'] += 1
            by_manager_item[manager]['items'][item_name]['fee'] += fee

        # 월별 수수료
        if month > 0:
            if month not in by_month_fee:
                by_month_fee[month] = {'count': 0, 'fee': 0}
            by_month_fee[month]['count'] += 1
            by_month_fee[month]['fee'] += fee

    # 결과 정리
    by_item_sorted = sorted(by_item.items(), key=lambda x: x[1]['count'], reverse=True)
    by_manager_sorted = sorted(by_manager_item.items(), key=lambda x: x[1]['fee'], reverse=True)

    return {
        'by_item': by_item_sorted,
        'by_item_month': {k: list(v.items()) for k, v in by_item_month.items()},
        'by_item_analyzer': {k: sorted(v.items(), key=lambda x: x[1]['count'], reverse=True)
                            for k, v in by_item_analyzer.items()},
        'by_sample_type_item': {k: sorted(v.items(), key=lambda x: x[1]['count'], reverse=True)
                               for k, v in by_sample_type_item.items()},
        'by_manager_item': by_manager_sorted,
        'by_month_fee': list(by_month_fee.items()),
        'purposes': sorted(purposes),
        'sample_types': sorted(sample_types),
        'items': sorted(items),
        'managers': sorted(managers),
        'analyzers': sorted(analyzers),
        'total_fee': total_fee,
        'total_count': total_count,
        'by_purpose_sample_type': {k: sorted(v) for k, v in by_purpose_sample_type.items()},
        'by_purpose_sample_type_item': {k: sorted(v) for k, v in by_purpose_sample_type_item.items()}
    }

def extract_region(address):
    """주소에서 시/도, 시/군/구 추출"""
    if not address:
        return None, None

    addr = str(address).strip()
    if not addr:
        return None, None

    # 시/도 추출
    sido = None
    sigungu = None

    # 광역시/특별시/도 패턴
    sido_patterns = [
        '서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종',
        '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주'
    ]

    for pattern in sido_patterns:
        if pattern in addr:
            sido = pattern
            break

    # 시/군/구 추출 (첫 번째 시/군/구 단위)
    import re
    # 시, 군, 구 패턴 매칭
    match = re.search(r'([가-힣]+(?:시|군|구))', addr)
    if match:
        sigungu = match.group(1)
        # 시도명이 시군구에 포함되어 있으면 다음 매칭 찾기
        if sido and (sigungu == sido + '시' or sigungu == sido + '도'):
            matches = re.findall(r'([가-힣]+(?:시|군|구))', addr)
            if len(matches) > 1:
                sigungu = matches[1]

    return sido, sigungu

def process_data(data, purpose_filter=None):
    """데이터 처리"""
    by_manager = {}
    by_branch = {}
    by_month = {}
    by_client = {}
    by_purpose = {}
    by_defect = {}
    by_defect_month = {}
    by_defect_purpose = {}  # 부적합-검사목적별 데이터
    by_defect_purpose_month = {}  # 부적합-검사목적별-월별 데이터
    by_purpose_month = {}  # 목적별-월별 데이터
    by_region = {}  # 지역별 데이터
    by_region_manager = {}  # 지역-담당자별 데이터
    by_purpose_manager = {}  # 목적별-담당자 데이터
    by_purpose_region = {}  # 목적별-지역 데이터
    by_sample_type = {}  # 검체유형별 데이터
    by_sample_type_month = {}  # 검체유형별-월별 데이터
    by_sample_type_manager = {}  # 검체유형별-담당자 데이터
    by_sample_type_purpose = {}  # 검체유형별-목적 데이터
    purposes = set()
    sample_types = set()  # 검체유형 목록
    total_sales = 0
    total_count = 0

    # 주소 컬럼 자동 감지
    address_columns = ['거래처 주소', '채품지주소', '채품장소', '주소', '시료주소', '업체주소', '거래처주소', '검체주소', '시료채취장소']

    for row in data:
        purpose = str(row.get('검사목적', '') or '').strip()
        purposes.add(purpose) if purpose else None

        # 검사목적 필터 적용
        if purpose_filter and purpose_filter != '전체' and purpose != purpose_filter:
            continue

        manager = row.get('영업담당', '미지정')
        sales = row.get('공급가액', 0) or 0
        date = row.get('접수일자')
        client = str(row.get('거래처', '') or '').strip() or '미지정'
        defect = str(row.get('부적합항목', '') or '').strip()
        sample_type = str(row.get('검체유형', '') or '').strip()
        if sample_type:
            sample_types.add(sample_type)

        if isinstance(sales, str):
            sales = float(sales.replace(',', '').replace('원', '')) if sales else 0

        # 매니저별
        if manager not in by_manager:
            by_manager[manager] = {'sales': 0, 'count': 0, 'clients': {}}
        by_manager[manager]['sales'] += sales
        by_manager[manager]['count'] += 1
        if client not in by_manager[manager]['clients']:
            by_manager[manager]['clients'][client] = {'sales': 0, 'count': 0}
        by_manager[manager]['clients'][client]['sales'] += sales
        by_manager[manager]['clients'][client]['count'] += 1

        # 지사별
        branch = MANAGER_TO_BRANCH.get(manager, '기타')
        if branch not in by_branch:
            by_branch[branch] = {'sales': 0, 'count': 0, 'managers': set()}
        by_branch[branch]['sales'] += sales
        by_branch[branch]['count'] += 1
        by_branch[branch]['managers'].add(manager)

        # 월별
        month = 0
        if date:
            if hasattr(date, 'month'):
                month = date.month
            else:
                try:
                    month = int(str(date).split('-')[1])
                except:
                    month = 0

        if month > 0:
            if month not in by_month:
                by_month[month] = {'sales': 0, 'count': 0}
            by_month[month]['sales'] += sales
            by_month[month]['count'] += 1

        # 거래처별
        if client not in by_client:
            by_client[client] = {'sales': 0, 'count': 0, 'purposes': {}}
        by_client[client]['sales'] += sales
        by_client[client]['count'] += 1
        if purpose:
            if purpose not in by_client[client]['purposes']:
                by_client[client]['purposes'][purpose] = {'sales': 0, 'count': 0}
            by_client[client]['purposes'][purpose]['sales'] += sales
            by_client[client]['purposes'][purpose]['count'] += 1

        # 검사목적별
        if purpose:
            if purpose not in by_purpose:
                by_purpose[purpose] = {'sales': 0, 'count': 0}
            by_purpose[purpose]['sales'] += sales
            by_purpose[purpose]['count'] += 1

            # 목적별-담당자 데이터
            if purpose not in by_purpose_manager:
                by_purpose_manager[purpose] = {}
            if manager not in by_purpose_manager[purpose]:
                by_purpose_manager[purpose][manager] = {'sales': 0, 'count': 0}
            by_purpose_manager[purpose][manager]['sales'] += sales
            by_purpose_manager[purpose][manager]['count'] += 1

            # 목적별-월별 데이터
            if month > 0:
                if purpose not in by_purpose_month:
                    by_purpose_month[purpose] = {}
                if month not in by_purpose_month[purpose]:
                    by_purpose_month[purpose][month] = {'sales': 0, 'count': 0, 'by_manager': {}}
                by_purpose_month[purpose][month]['sales'] += sales
                by_purpose_month[purpose][month]['count'] += 1
                # 담당자별 월별 목적 데이터
                if manager not in by_purpose_month[purpose][month]['by_manager']:
                    by_purpose_month[purpose][month]['by_manager'][manager] = {'sales': 0, 'count': 0}
                by_purpose_month[purpose][month]['by_manager'][manager]['sales'] += sales
                by_purpose_month[purpose][month]['by_manager'][manager]['count'] += 1

        # 부적합항목별
        if defect:
            if defect not in by_defect:
                by_defect[defect] = {'count': 0}
            by_defect[defect]['count'] += 1

            # 부적합항목 월별
            if month > 0:
                if defect not in by_defect_month:
                    by_defect_month[defect] = {}
                if month not in by_defect_month[defect]:
                    by_defect_month[defect][month] = 0
                by_defect_month[defect][month] += 1

            # 부적합항목-검사목적별
            if purpose:
                if purpose not in by_defect_purpose:
                    by_defect_purpose[purpose] = {}
                if defect not in by_defect_purpose[purpose]:
                    by_defect_purpose[purpose][defect] = {'count': 0}
                by_defect_purpose[purpose][defect]['count'] += 1

                # 부적합항목-검사목적별-월별
                if month > 0:
                    if purpose not in by_defect_purpose_month:
                        by_defect_purpose_month[purpose] = {}
                    if defect not in by_defect_purpose_month[purpose]:
                        by_defect_purpose_month[purpose][defect] = {}
                    if month not in by_defect_purpose_month[purpose][defect]:
                        by_defect_purpose_month[purpose][defect][month] = 0
                    by_defect_purpose_month[purpose][defect][month] += 1

        # 검체유형별
        if sample_type:
            if sample_type not in by_sample_type:
                by_sample_type[sample_type] = {'sales': 0, 'count': 0}
            by_sample_type[sample_type]['sales'] += sales
            by_sample_type[sample_type]['count'] += 1

            # 검체유형별-담당자 데이터
            if sample_type not in by_sample_type_manager:
                by_sample_type_manager[sample_type] = {}
            if manager not in by_sample_type_manager[sample_type]:
                by_sample_type_manager[sample_type][manager] = {'sales': 0, 'count': 0, 'by_purpose': {}}
            by_sample_type_manager[sample_type][manager]['sales'] += sales
            by_sample_type_manager[sample_type][manager]['count'] += 1
            # 담당자별 목적 데이터 추가
            if purpose:
                if purpose not in by_sample_type_manager[sample_type][manager]['by_purpose']:
                    by_sample_type_manager[sample_type][manager]['by_purpose'][purpose] = {'sales': 0, 'count': 0}
                by_sample_type_manager[sample_type][manager]['by_purpose'][purpose]['sales'] += sales
                by_sample_type_manager[sample_type][manager]['by_purpose'][purpose]['count'] += 1

            # 검체유형별-목적 데이터
            if purpose:
                if sample_type not in by_sample_type_purpose:
                    by_sample_type_purpose[sample_type] = {}
                if purpose not in by_sample_type_purpose[sample_type]:
                    by_sample_type_purpose[sample_type][purpose] = {'sales': 0, 'count': 0}
                by_sample_type_purpose[sample_type][purpose]['sales'] += sales
                by_sample_type_purpose[sample_type][purpose]['count'] += 1

            # 검체유형별-월별 데이터
            if month > 0:
                if sample_type not in by_sample_type_month:
                    by_sample_type_month[sample_type] = {}
                if month not in by_sample_type_month[sample_type]:
                    by_sample_type_month[sample_type][month] = {'sales': 0, 'count': 0, 'by_manager': {}, 'by_purpose': {}}
                by_sample_type_month[sample_type][month]['sales'] += sales
                by_sample_type_month[sample_type][month]['count'] += 1
                # 담당자별 월별 검체유형 데이터
                if manager not in by_sample_type_month[sample_type][month]['by_manager']:
                    by_sample_type_month[sample_type][month]['by_manager'][manager] = {'sales': 0, 'count': 0}
                by_sample_type_month[sample_type][month]['by_manager'][manager]['sales'] += sales
                by_sample_type_month[sample_type][month]['by_manager'][manager]['count'] += 1
                # 목적별 월별 검체유형 데이터
                if purpose:
                    if purpose not in by_sample_type_month[sample_type][month]['by_purpose']:
                        by_sample_type_month[sample_type][month]['by_purpose'][purpose] = {'sales': 0, 'count': 0}
                    by_sample_type_month[sample_type][month]['by_purpose'][purpose]['sales'] += sales
                    by_sample_type_month[sample_type][month]['by_purpose'][purpose]['count'] += 1

        # 지역별 분석
        address = None
        for col in address_columns:
            if row.get(col):
                address = row.get(col)
                break

        sido, sigungu = extract_region(address)

        if sido:
            region_key = sido
            if sigungu:
                region_key = f"{sido} {sigungu}"

            # 지역별 통계
            if region_key not in by_region:
                by_region[region_key] = {'sales': 0, 'count': 0, 'sido': sido, 'sigungu': sigungu or '', 'managers': {}}
            by_region[region_key]['sales'] += sales
            by_region[region_key]['count'] += 1

            # 지역-담당자별 통계
            if manager not in by_region[region_key]['managers']:
                by_region[region_key]['managers'][manager] = {'sales': 0, 'count': 0}
            by_region[region_key]['managers'][manager]['sales'] += sales
            by_region[region_key]['managers'][manager]['count'] += 1

            # 담당자-지역별 통계
            if manager not in by_region_manager:
                by_region_manager[manager] = {}
            if region_key not in by_region_manager[manager]:
                by_region_manager[manager][region_key] = {'sales': 0, 'count': 0, 'sido': sido, 'sigungu': sigungu or ''}
            by_region_manager[manager][region_key]['sales'] += sales
            by_region_manager[manager][region_key]['count'] += 1

            # 목적별-지역 데이터
            if purpose:
                if purpose not in by_purpose_region:
                    by_purpose_region[purpose] = {}
                if region_key not in by_purpose_region[purpose]:
                    by_purpose_region[purpose][region_key] = {'sales': 0, 'count': 0}
                by_purpose_region[purpose][region_key]['sales'] += sales
                by_purpose_region[purpose][region_key]['count'] += 1

        total_sales += sales
        total_count += 1

    # 정렬
    sorted_managers = sorted(by_manager.items(), key=lambda x: x[1]['sales'], reverse=True)
    sorted_branches = sorted(by_branch.items(), key=lambda x: x[1]['sales'], reverse=True)
    sorted_clients = sorted(by_client.items(), key=lambda x: x[1]['sales'], reverse=True)
    sorted_purposes = sorted(by_purpose.items(), key=lambda x: x[1]['sales'], reverse=True)
    sorted_defects = sorted(by_defect.items(), key=lambda x: x[1]['count'], reverse=True)

    # 매니저별 TOP 10 거래처
    manager_top_clients = {}
    for mgr, data in sorted_managers:
        clients = sorted(data['clients'].items(), key=lambda x: x[1]['sales'], reverse=True)[:10]
        manager_top_clients[mgr] = clients

    # 고효율 업체 (높은 단가)
    high_efficiency = [(c, d) for c, d in sorted_clients if d['count'] > 0]
    high_efficiency = sorted(high_efficiency, key=lambda x: x[1]['sales'] / x[1]['count'] if x[1]['count'] > 0 else 0, reverse=True)[:20]

    # 대량 업체 (많은 건수)
    high_volume = sorted(by_client.items(), key=lambda x: x[1]['count'], reverse=True)[:20]

    # 지역별 정렬 (매출 기준)
    sorted_regions = sorted(by_region.items(), key=lambda x: x[1]['sales'], reverse=True)

    # 지역별 TOP 담당자
    region_top_managers = {}
    for region, data in sorted_regions:
        managers = sorted(data['managers'].items(), key=lambda x: x[1]['sales'], reverse=True)
        region_top_managers[region] = [
            {'name': m, 'sales': d['sales'], 'count': d['count']}
            for m, d in managers[:5]
        ]

    # 담당자별 지역 분포
    manager_regions = {}
    for mgr, regions in by_region_manager.items():
        sorted_mgr_regions = sorted(regions.items(), key=lambda x: x[1]['sales'], reverse=True)
        manager_regions[mgr] = [
            {'region': r, 'sales': d['sales'], 'count': d['count'], 'sido': d['sido'], 'sigungu': d['sigungu']}
            for r, d in sorted_mgr_regions[:10]
        ]

    # 목적별 담당자 데이터 정리
    purpose_managers = {}
    for purpose, managers in by_purpose_manager.items():
        sorted_pm = sorted(managers.items(), key=lambda x: x[1]['sales'], reverse=True)
        purpose_managers[purpose] = [
            {'name': m, 'sales': d['sales'], 'count': d['count']}
            for m, d in sorted_pm[:20]
        ]

    # 목적별 지역 데이터 정리
    purpose_regions = {}
    for purpose, regions in by_purpose_region.items():
        sorted_pr = sorted(regions.items(), key=lambda x: x[1]['sales'], reverse=True)
        purpose_regions[purpose] = [
            {'region': r, 'sales': d['sales'], 'count': d['count']}
            for r, d in sorted_pr[:20]
        ]

    # 검체유형별 정렬
    sorted_sample_types = sorted(by_sample_type.items(), key=lambda x: x[1]['sales'], reverse=True)

    # 검체유형별 담당자 데이터 정리
    sample_type_managers = {}
    for st, managers in by_sample_type_manager.items():
        sorted_stm = sorted(managers.items(), key=lambda x: x[1]['sales'], reverse=True)
        sample_type_managers[st] = [
            {'name': m, 'sales': d['sales'], 'count': d['count'], 'by_purpose': d.get('by_purpose', {})}
            for m, d in sorted_stm[:20]
        ]

    # 검체유형별 목적 데이터 정리
    sample_type_purposes = {}
    for st, purposes_data in by_sample_type_purpose.items():
        sorted_stp = sorted(purposes_data.items(), key=lambda x: x[1]['sales'], reverse=True)
        sample_type_purposes[st] = [
            {'name': p, 'sales': d['sales'], 'count': d['count']}
            for p, d in sorted_stp[:20]
        ]

    return {
        'by_manager': [(m, {'sales': d['sales'], 'count': d['count']}) for m, d in sorted_managers],
        'by_branch': [(k, {'sales': v['sales'], 'count': v['count'], 'managers': len(v['managers'])})
                      for k, v in sorted_branches],
        'by_month': sorted(by_month.items()),
        'by_client': [(c, {'sales': d['sales'], 'count': d['count'], 'avg': d['sales']/d['count'] if d['count'] > 0 else 0})
                      for c, d in sorted_clients[:50]],
        'by_purpose': sorted_purposes,
        'by_defect': sorted_defects[:30],
        'by_defect_month': {d: sorted(months.items()) for d, months in by_defect_month.items()},
        'by_defect_purpose': {p: sorted(defects.items(), key=lambda x: x[1]['count'], reverse=True)[:30] for p, defects in by_defect_purpose.items()},
        'by_defect_purpose_month': {p: {d: sorted(months.items()) for d, months in defects.items()} for p, defects in by_defect_purpose_month.items()},
        'by_purpose_month': {p: {m: {'sales': d['sales'], 'count': d['count'], 'by_manager': d.get('by_manager', {})} for m, d in months.items()} for p, months in by_purpose_month.items()},
        'manager_top_clients': manager_top_clients,
        'high_efficiency': [(c, {'sales': d['sales'], 'count': d['count'], 'avg': d['sales']/d['count'] if d['count'] > 0 else 0})
                           for c, d in high_efficiency],
        'high_volume': [(c, {'sales': d['sales'], 'count': d['count'], 'avg': d['sales']/d['count'] if d['count'] > 0 else 0})
                       for c, d in high_volume],
        'by_region': [(r, {'sales': d['sales'], 'count': d['count'], 'sido': d['sido'], 'sigungu': d['sigungu']})
                      for r, d in sorted_regions[:50]],
        'region_top_managers': region_top_managers,
        'manager_regions': manager_regions,
        'purpose_managers': purpose_managers,
        'purpose_regions': purpose_regions,
        'purposes': sorted(list(purposes)),
        'by_sample_type': sorted_sample_types,
        'by_sample_type_month': {st: {m: {'sales': d['sales'], 'count': d['count'], 'by_manager': d.get('by_manager', {}), 'by_purpose': d.get('by_purpose', {})} for m, d in months.items()} for st, months in by_sample_type_month.items()},
        'sample_type_managers': sample_type_managers,
        'sample_type_purposes': sample_type_purposes,
        'sample_types': sorted(list(sample_types)),
        'total_sales': total_sales,
        'total_count': total_count
    }

HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>경영지표 대시보드</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Malgun Gothic', sans-serif; background: #f5f7fa; padding: 20px; }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px;
        }
        .header h1 { font-size: 24px; }
        .controls { display: flex; gap: 10px; margin: 15px 0; flex-wrap: wrap; align-items: center; }
        .controls select { padding: 8px 15px; border-radius: 5px; border: 1px solid #ddd; font-size: 14px; }
        .date-group { display: flex; align-items: center; gap: 5px; background: rgba(255,255,255,0.2); padding: 8px 12px; border-radius: 5px; }
        .date-group label { color: white; font-size: 13px; margin-right: 5px; }
        .date-group select { padding: 5px 8px; font-size: 13px; }
        .range-separator { color: white; font-weight: bold; padding: 0 10px; }
        .compare-box {
            display: flex; align-items: center; gap: 8px;
            background: rgba(255,255,255,0.2); padding: 8px 15px; border-radius: 5px;
        }
        .compare-box input[type="checkbox"] { width: 18px; height: 18px; cursor: pointer; }
        .compare-box label { color: white; cursor: pointer; }
        .compare-box select { padding: 5px 10px; }
        .summary { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .card { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .card h3 { color: #666; font-size: 14px; margin-bottom: 10px; }
        .card .value { font-size: 28px; font-weight: bold; color: #333; }
        .card .compare-value { font-size: 14px; color: #764ba2; margin-top: 5px; padding-top: 5px; border-top: 1px dashed #ddd; }
        .card .diff { font-size: 12px; margin-top: 3px; }
        .card .diff.positive { color: #2ecc71; }
        .card .diff.negative { color: #e74c3c; }
        .positive { color: #2ecc71; font-weight: bold; }
        .negative { color: #e74c3c; font-weight: bold; }
        .charts { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }
        .chart-container { background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
        .chart-container h3 { margin-bottom: 15px; color: #333; }
        .chart-container.full { grid-column: 1 / -1; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 8px 10px; text-align: left; border-bottom: 1px solid #eee; font-size: 13px; }
        th { background: #f8f9fa; font-weight: 600; }
        tr:hover { background: #f8f9fa; }
        .tabs { display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap; }
        .tab { padding: 10px 20px; background: white; border: none; border-radius: 5px; cursor: pointer; font-size: 14px; }
        .tab.active { background: #667eea; color: white; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .btn-search {
            padding: 8px 20px; background: #fff; color: #667eea;
            border: 2px solid #fff; border-radius: 5px; font-size: 14px; font-weight: bold; cursor: pointer;
        }
        .btn-search:hover { background: rgba(255,255,255,0.9); }
        .btn-search:disabled { opacity: 0.6; cursor: not-allowed; }
        .toast {
            position: fixed; top: 20px; right: 20px; padding: 15px 25px;
            background: #2ecc71; color: white; border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2); z-index: 1000; display: none;
        }
        .toast.error { background: #e74c3c; }
        .toast.loading { background: #3498db; }
        .legend-custom { display: flex; gap: 20px; margin-bottom: 10px; font-size: 13px; }
        .legend-item { display: flex; align-items: center; gap: 5px; }
        .legend-color { width: 12px; height: 12px; border-radius: 2px; }
        .sub-select { margin-bottom: 15px; }
        .sub-select select { padding: 8px 15px; border-radius: 5px; border: 1px solid #ddd; }
        .scroll-table { max-height: 400px; overflow-y: auto; }
        th.sortable { cursor: pointer; user-select: none; position: relative; padding-right: 20px; }
        th.sortable:hover { background: #e9ecef; }
        th.sortable::after { content: '⇅'; position: absolute; right: 5px; opacity: 0.3; font-size: 11px; }
        th.sortable.asc::after { content: '▲'; opacity: 1; color: #667eea; }
        th.sortable.desc::after { content: '▼'; opacity: 1; color: #667eea; }
    </style>
</head>
<body>
    <div id="toast" class="toast"></div>
    <div class="header">
        <h1>📊 경영지표 대시보드</h1>
        <div class="controls">
            <div class="date-group">
                <label>📅 조회기간:</label>
                <select id="yearSelect" onchange="updateDateSelectors()">
                    <option value="2025">2025년</option>
                    <option value="2024">2024년</option>
                </select>
                <select id="monthSelect" onchange="updateDaySelector()">
                    <option value="">전체</option>
                </select>
                <select id="daySelect">
                    <option value="">전체</option>
                </select>
            </div>
            <div class="compare-box">
                <input type="checkbox" id="rangeCheck" onchange="toggleRangeMode()">
                <label for="rangeCheck">기간범위</label>
            </div>
            <div id="rangeDateGroup" class="date-group" style="display:none;">
                <span class="range-separator">~</span>
                <select id="endYearSelect" onchange="updateEndDateSelectors()">
                    <option value="2025">2025년</option>
                    <option value="2024">2024년</option>
                </select>
                <select id="endMonthSelect" onchange="updateEndDaySelector()">
                    <option value="">전체</option>
                </select>
                <select id="endDaySelect">
                    <option value="">전체</option>
                </select>
            </div>
            <div class="compare-box">
                <input type="checkbox" id="compareCheck" onchange="toggleCompare()">
                <label for="compareCheck">비교</label>
            </div>
            <div id="compareDateGroup" class="date-group" style="display:none;">
                <select id="compareYearSelect">
                    <option value="2024">2024년</option>
                    <option value="2025">2025년</option>
                </select>
                <select id="compareMonthSelect">
                    <option value="">전체</option>
                </select>
                <select id="compareDaySelect">
                    <option value="">전체</option>
                </select>
            </div>
            <div id="compareRangeDateGroup" style="display:none;">
                <span class="range-separator">~</span>
                <div class="date-group">
                    <select id="compareEndYearSelect">
                        <option value="2024">2024년</option>
                        <option value="2025">2025년</option>
                    </select>
                    <select id="compareEndMonthSelect">
                        <option value="">전체</option>
                    </select>
                    <select id="compareEndDaySelect">
                        <option value="">전체</option>
                    </select>
                </div>
            </div>
            <select id="purposeSelect">
                <option value="전체">검사목적: 전체</option>
            </select>
            <button id="btnSearch" class="btn-search" onclick="loadData()">조회하기</button>
        </div>
    </div>

    <div class="summary" id="summary">
        <div class="card">
            <h3>총 매출</h3>
            <div class="value" id="totalSales">-</div>
            <div class="compare-value" id="compareTotalSales" style="display:none;"></div>
            <div class="diff" id="diffTotalSales"></div>
        </div>
        <div class="card">
            <h3>총 건수</h3>
            <div class="value" id="totalCount">-</div>
            <div class="compare-value" id="compareTotalCount" style="display:none;"></div>
            <div class="diff" id="diffTotalCount"></div>
        </div>
        <div class="card">
            <h3>평균 단가</h3>
            <div class="value" id="avgPrice">-</div>
            <div class="compare-value" id="compareAvgPrice" style="display:none;"></div>
            <div class="diff" id="diffAvgPrice"></div>
        </div>
    </div>

    <div class="tabs">
        <button class="tab active" onclick="showTab('personal')">👤 개인별</button>
        <button class="tab" onclick="showTab('team')">🏢 팀별</button>
        <button class="tab" onclick="showTab('monthly')">📅 월별</button>
        <button class="tab" onclick="showTab('client')">🏭 업체별</button>
        <button class="tab" onclick="showTab('region')">📍 지역별</button>
        <button class="tab" onclick="showTab('purpose')">🎯 목적별</button>
        <button class="tab" onclick="showTab('sampleType')">🧪 유형</button>
        <button class="tab" onclick="showTab('defect')">⚠️ 부적합</button>
        <button class="tab" onclick="showTab('foodItem')">🔬 검사항목</button>
        <button class="tab" onclick="showTab('aiAnalysis')" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white;">🤖 AI 분석</button>
        <button class="tab" onclick="showTab('companyInfo')" style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white;">🏢 기업 정보</button>
    </div>

    <!-- 개인별 탭 -->
    <div id="personal" class="tab-content active">
        <div class="charts">
            <div class="chart-container">
                <h3>영업담당별 매출 TOP 15</h3>
                <div id="managerLegend" class="legend-custom" style="display:none;"></div>
                <canvas id="managerChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>영업담당별 상세</h3>
                <div class="scroll-table">
                    <table id="managerTable">
                        <thead id="managerTableHead"><tr><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- 팀별 탭 -->
    <div id="team" class="tab-content">
        <div class="charts">
            <div class="chart-container">
                <h3>지사/센터별 매출</h3>
                <div id="branchLegend" class="legend-custom" style="display:none;"></div>
                <canvas id="branchChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>지사/센터별 상세</h3>
                <table id="branchTable">
                    <thead id="branchTableHead"><tr><th>지사/센터</th><th>매출액</th><th>건수</th><th>담당자수</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- 월별 탭 -->
    <div id="monthly" class="tab-content">
        <div class="charts">
            <div class="chart-container full">
                <h3>월별 매출 추이</h3>
                <div id="monthlyLegend" class="legend-custom" style="display:none;"></div>
                <div style="height: 300px;"><canvas id="monthlyChart"></canvas></div>
            </div>
        </div>
    </div>

    <!-- 업체별 탭 -->
    <div id="client" class="tab-content">
        <div class="sub-select" style="margin-bottom: 20px; padding: 15px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); display: flex; align-items: center; gap: 20px; flex-wrap: wrap;">
            <div>
                <span id="clientYearLabel" style="font-weight: bold; color: #667eea; font-size: 16px;">📅 2025년</span>
            </div>
            <div>
                <label style="margin-right: 10px; font-weight: bold;">👤 담당자 필터:</label>
                <select id="clientManagerFilter" onchange="updateClientTables()">
                    <option value="">전체 담당자</option>
                </select>
            </div>
        </div>
        <div class="charts">
            <div class="chart-container">
                <h3>🏆 매출 TOP 20 업체</h3>
                <div class="scroll-table">
                    <table id="clientTopTable">
                        <thead id="clientTopTableHead"><tr><th>순위</th><th>거래처</th><th>매출액</th><th>건수</th><th>평균단가</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            <div class="chart-container">
                <h3>💎 고효율 업체 (높은 단가)</h3>
                <div class="scroll-table">
                    <table id="clientEffTable">
                        <thead id="clientEffTableHead"><tr><th>거래처</th><th>평균단가</th><th>매출액</th><th>건수</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            <div class="chart-container">
                <h3>📦 대량 업체 (많은 건수)</h3>
                <div class="scroll-table">
                    <table id="clientVolTable">
                        <thead id="clientVolTableHead"><tr><th>거래처</th><th>건수</th><th>매출액</th><th>평균단가</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- 지역별 탭 -->
    <div id="region" class="tab-content">
        <div class="sub-select" style="margin-bottom: 20px; padding: 15px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); display: flex; align-items: center; gap: 20px; flex-wrap: wrap;">
            <div>
                <span id="regionYearLabel" style="font-weight: bold; color: #667eea; font-size: 16px;">📅 2025년</span>
            </div>
            <div>
                <label style="margin-right: 10px; font-weight: bold;">👤 담당자 필터:</label>
                <select id="regionManagerFilter" onchange="updateRegionTables()">
                    <option value="">전체 담당자</option>
                </select>
            </div>
        </div>
        <div class="charts">
            <div class="chart-container">
                <h3>📍 지역별 매출 TOP 15</h3>
                <canvas id="regionChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>지역별 상세 (시/도, 시/군/구)</h3>
                <div class="scroll-table">
                    <table id="regionTable">
                        <thead><tr><th>순위</th><th>지역</th><th>매출액</th><th>건수</th><th>평균단가</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container full">
                <h3>🏆 지역별 TOP 담당자</h3>
                <div class="sub-select">
                    <select id="regionSelect" onchange="updateRegionManagers()">
                        <option value="">지역 선택</option>
                    </select>
                </div>
                <div class="scroll-table">
                    <table id="regionManagerTable">
                        <thead><tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            <div class="chart-container full">
                <h3>👤 담당자별 지역 분포</h3>
                <div class="sub-select">
                    <select id="managerRegionSelect" onchange="updateManagerRegions()">
                        <option value="">담당자 선택</option>
                    </select>
                </div>
                <div class="scroll-table">
                    <table id="managerRegionTable">
                        <thead><tr><th>순위</th><th>지역</th><th>매출액</th><th>건수</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- 목적별 탭 -->
    <div id="purpose" class="tab-content">
        <div class="sub-select" style="margin-bottom: 20px; padding: 15px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
            <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap; margin-bottom: 15px;">
                <span id="purposeYearLabel" style="font-weight: bold; color: #667eea; font-size: 16px;">📅 2025년</span>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <label style="font-weight: bold;">👤 담당자:</label>
                    <select id="purposeManagerFilter" onchange="updatePurposeTab(); updatePurposeMonthlyChart();" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="">전체</option>
                    </select>
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <label style="font-weight: bold;">📍 지역:</label>
                    <select id="purposeRegionFilter" onchange="updatePurposeTab()" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="">전체</option>
                    </select>
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <label style="font-weight: bold;">TOP:</label>
                    <select id="purposeTopN" onchange="updatePurposeTab()" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="10">10</option>
                        <option value="15" selected>15</option>
                        <option value="20">20</option>
                        <option value="50">50</option>
                    </select>
                </div>
                <button onclick="selectAllPurposes()" style="padding: 5px 10px; background: #667eea; color: white; border: none; border-radius: 5px; cursor: pointer;">전체선택</button>
                <button onclick="clearAllPurposes()" style="padding: 5px 10px; background: #999; color: white; border: none; border-radius: 5px; cursor: pointer;">선택해제</button>
            </div>
            <div id="purposeCheckboxes" style="display: flex; flex-wrap: wrap; gap: 10px; max-height: 100px; overflow-y: auto; padding: 10px; background: #f8f9fa; border-radius: 5px;">
                <!-- 검사목적 체크박스들이 여기에 동적으로 추가됨 -->
            </div>
        </div>
        <div class="charts">
            <div class="chart-container">
                <h3>🎯 목적별 매출 TOP <span id="purposeChartTopN">15</span> <span id="purposeChartFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: 검사목적 체크박스, TOP 필터, 담당자 필터, 지역 필터</div>
                <canvas id="purposeChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>📊 목적별 상세 <span id="purposeTableFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: 검사목적 체크박스, TOP 필터, 담당자 필터, 지역 필터</div>
                <div class="scroll-table" style="max-height: 450px;">
                    <table id="purposeTable">
                        <thead id="purposeTableHead"><tr><th>순위</th><th>검사목적</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container full">
                <h3>📈 목적별 월별 추이 <span id="purposeMonthlyFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 5px;">📌 적용: 아래 드롭다운에서 선택한 검사목적 + 담당자 필터</div>
                <div class="sub-select" style="margin-bottom: 10px;">
                    <select id="purposeMonthlySelect" onchange="updatePurposeMonthlyChart()" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="">목적 선택</option>
                    </select>
                </div>
                <div style="height: 300px;"><canvas id="purposeMonthlyChart"></canvas></div>
            </div>
        </div>
        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container">
                <h3>👤 목적별 담당자 실적 <span id="purposeManagerFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: 검사목적 체크박스, TOP 필터, 담당자 필터</div>
                <div class="scroll-table" style="max-height: 400px;">
                    <table id="purposeManagerTable">
                        <thead id="purposeManagerTableHead"><tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            <div class="chart-container">
                <h3>📍 목적별 지역 실적 <span id="purposeRegionFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: 검사목적 체크박스, TOP 필터, 지역 필터</div>
                <div class="scroll-table" style="max-height: 400px;">
                    <table id="purposeRegionTable">
                        <thead id="purposeRegionTableHead"><tr><th>순위</th><th>지역</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- 유형 탭 (검체유형) -->
    <div id="sampleType" class="tab-content">
        <div class="sub-select" style="margin-bottom: 20px; padding: 15px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
            <div style="display: flex; align-items: center; gap: 20px; flex-wrap: wrap; margin-bottom: 15px;">
                <span id="sampleTypeYearLabel" style="font-weight: bold; color: #667eea; font-size: 16px;">📅 2025년</span>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <label style="font-weight: bold;">👤 담당자:</label>
                    <select id="sampleTypeManagerFilter" onchange="updateSampleTypeTab(); updateSampleTypeMonthlyChart();" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="">전체</option>
                    </select>
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <label style="font-weight: bold;">🎯 검사목적:</label>
                    <select id="sampleTypePurposeFilter" onchange="updateSampleTypeTab(); updateSampleTypeMonthlyChart();" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="">전체</option>
                    </select>
                </div>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <label style="font-weight: bold;">TOP:</label>
                    <select id="sampleTypeTopN" onchange="updateSampleTypeTab()" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="10">10</option>
                        <option value="15" selected>15</option>
                        <option value="20">20</option>
                        <option value="50">50</option>
                    </select>
                </div>
            </div>
        </div>
        <div class="charts">
            <div class="chart-container">
                <h3>🧪 검체유형별 매출 TOP <span id="sampleTypeChartTopN">15</span> <span id="sampleTypeChartFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: TOP 필터, 담당자 필터, 검사목적 필터</div>
                <canvas id="sampleTypeChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>📊 검체유형별 상세 <span id="sampleTypeTableFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: TOP 필터, 담당자 필터, 검사목적 필터</div>
                <div class="scroll-table" style="max-height: 450px;">
                    <table id="sampleTypeTable">
                        <thead id="sampleTypeTableHead"><tr><th>순위</th><th>검체유형</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container full">
                <h3>📈 검체유형별 월별 추이 <span id="sampleTypeMonthlyFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 5px;">📌 적용: 아래 드롭다운에서 선택한 검체유형 + 담당자 필터 + 검사목적 필터</div>
                <div class="sub-select" style="margin-bottom: 10px;">
                    <select id="sampleTypeMonthlySelect" onchange="updateSampleTypeMonthlyChart()" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="">검체유형 선택</option>
                    </select>
                </div>
                <div style="height: 300px;"><canvas id="sampleTypeMonthlyChart"></canvas></div>
            </div>
        </div>
        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container">
                <h3>👤 검체유형별 담당자 실적 <span id="sampleTypeManagerTableLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: TOP 필터, 담당자 필터</div>
                <div class="scroll-table" style="max-height: 400px;">
                    <table id="sampleTypeManagerTable">
                        <thead id="sampleTypeManagerTableHead"><tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            <div class="chart-container">
                <h3>🎯 검체유형별 목적 실적 <span id="sampleTypePurposeTableLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div style="font-size: 11px; color: #888; margin-bottom: 10px;">📌 적용: TOP 필터, 검사목적 필터</div>
                <div class="scroll-table" style="max-height: 400px;">
                    <table id="sampleTypePurposeTable">
                        <thead id="sampleTypePurposeTableHead"><tr><th>순위</th><th>검사목적</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>

    <!-- 부적합 탭 -->
    <div id="defect" class="tab-content">
        <div class="sub-select" style="margin-bottom: 20px; padding: 15px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
            <div style="display: flex; align-items: center; gap: 10px;">
                <label style="font-weight: bold;">🎯 검사목적:</label>
                <select id="defectPurposeFilter" onchange="updateDefectTab()" style="padding: 5px 10px; border-radius: 5px; border: 1px solid #ddd;">
                    <option value="">전체</option>
                </select>
            </div>
        </div>
        <div class="charts">
            <div class="chart-container">
                <h3>⚠️ 부적합항목 TOP 15 <span id="defectChartFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <canvas id="defectChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>부적합항목 상세 <span id="defectTableFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div class="scroll-table">
                    <table id="defectTable">
                        <thead><tr><th>순위</th><th>부적합항목</th><th>건수</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>
        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container full">
                <h3>부적합항목 월별 추이 <span id="defectMonthlyFilterLabel" style="font-size: 12px; color: #667eea;"></span></h3>
                <div class="sub-select">
                    <select id="defectSelect" onchange="updateDefectMonthly()">
                        <option value="">항목 선택</option>
                    </select>
                </div>
                <div style="height: 250px;"><canvas id="defectMonthlyChart"></canvas></div>
            </div>
        </div>
    </div>

    <!-- 검사항목 탭 -->
    <div id="foodItem" class="tab-content">
        <div class="filter-row" style="margin-bottom: 15px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center;">
            <label>검사목적:</label>
            <select id="foodItemPurposeFilter" onchange="onPurposeChange()" style="padding: 5px;">
                <option value="전체">전체</option>
            </select>
            <label>검체유형:</label>
            <input type="text" id="foodItemSampleTypeInput" placeholder="검체유형 입력..."
                   oninput="filterSampleTypeDropdown()" style="padding: 5px; width: 150px;">
            <select id="foodItemSampleTypeFilter" onchange="onSampleTypeChange()" style="padding: 5px; width: 200px;">
                <option value="전체">전체</option>
            </select>
            <label>항목명1:</label>
            <select id="foodItemItem1Filter" onchange="onItemSelect(1)" style="padding: 5px; width: 180px;">
                <option value="전체">전체</option>
            </select>
            <label>항목명2:</label>
            <select id="foodItemItem2Filter" onchange="onItemSelect(2)" style="padding: 5px; width: 180px;">
                <option value="전체">전체</option>
            </select>
            <label>항목명3:</label>
            <select id="foodItemItem3Filter" onchange="onItemSelect(3)" style="padding: 5px; width: 180px;">
                <option value="전체">전체</option>
            </select>
            <label>영업담당:</label>
            <select id="foodItemManagerFilter" onchange="updateFoodItemTab()" style="padding: 5px;">
                <option value="전체">전체</option>
            </select>
        </div>

        <div class="summary-cards" style="margin-bottom: 15px;">
            <div class="summary-card">
                <div class="label">총 건수</div>
                <div class="value" id="foodItemTotalCount">-</div>
            </div>
            <div class="summary-card">
                <div class="label">총 항목수수료</div>
                <div class="value" id="foodItemTotalFee">-</div>
            </div>
        </div>

        <div class="charts">
            <div class="chart-container">
                <h3>항목별 건수 TOP 20</h3>
                <div style="height: 350px;"><canvas id="foodItemChart"></canvas></div>
            </div>
            <div class="chart-container">
                <h3>항목별 상세</h3>
                <div class="scroll-table" style="max-height: 350px;">
                    <table id="foodItemTable">
                        <thead id="foodItemTableHead"><tr><th>순위</th><th>항목명</th><th>건수</th><th>항목수수료</th><th>비중</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
        </div>

        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container">
                <h3>항목별 분석자 건수</h3>
                <div class="sub-select">
                    <select id="foodItemAnalyzerSelect" onchange="updateFoodItemAnalyzerTable()">
                        <option value="">항목 선택</option>
                    </select>
                </div>
                <div class="scroll-table" style="max-height: 300px;">
                    <table id="foodItemAnalyzerTable">
                        <thead id="foodItemAnalyzerTableHead"><tr><th>순위</th><th>분석자</th><th>건수</th><th>항목수수료</th></tr></thead>
                        <tbody></tbody>
                    </table>
                </div>
            </div>
            <div class="chart-container">
                <h3>월별 추이</h3>
                <div class="sub-select">
                    <select id="foodItemMonthlySelect" onchange="updateFoodItemMonthlyChart()">
                        <option value="">항목 선택</option>
                    </select>
                </div>
                <div style="height: 250px;"><canvas id="foodItemMonthlyChart"></canvas></div>
            </div>
        </div>

        <div class="charts" style="margin-top: 20px;">
            <div class="chart-container">
                <h3>항목수수료 연도별 추이</h3>
                <div style="height: 250px;"><canvas id="foodItemFeeYearlyChart"></canvas></div>
            </div>
            <div class="chart-container">
                <h3>영업담당별 항목수수료</h3>
                <div style="height: 250px;"><canvas id="foodItemManagerFeeChart"></canvas></div>
            </div>
        </div>
    </div>

    <!-- AI 분석 탭 -->
    <div id="aiAnalysis" class="tab-content">
        <div style="max-width: 1200px; margin: 0 auto;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
                <h2 style="margin: 0 0 10px 0;">🤖 AI 데이터 분석</h2>
                <p style="margin: 0; opacity: 0.9;">자연어로 질문하면 데이터를 분석해드립니다.</p>
            </div>

            <div style="background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px;">
                <div style="display: flex; gap: 10px;">
                    <input type="text" id="aiQueryInput" placeholder="예: 2025년 자가품질위탁검사 이물 항목 월별 매출 보여줘"
                           style="flex: 1; padding: 15px; font-size: 16px; border: 2px solid #e0e0e0; border-radius: 8px; outline: none;"
                           onkeypress="if(event.key==='Enter') runAiAnalysis()">
                    <button onclick="runAiAnalysis()"
                            style="padding: 15px 30px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; border: none; border-radius: 8px; cursor: pointer; font-size: 16px; font-weight: bold;">
                        분석하기
                    </button>
                </div>
                <div style="margin-top: 10px; color: #888; font-size: 13px;">
                    💡 예시 질문:
                    <span style="cursor: pointer; color: #667eea; margin-left: 10px;" onclick="setAiQuery('2025년 자가품질위탁검사 이물 항목 월별 매출 보여줘')">월별 매출</span> |
                    <span style="cursor: pointer; color: #667eea; margin-left: 5px;" onclick="setAiQuery('기타가공품에서 이물 항목 빠지면 연매출 영향은?')">항목 제외 영향</span> |
                    <span style="cursor: pointer; color: #667eea; margin-left: 5px;" onclick="setAiQuery('올해 가장 많이 접수된 항목 TOP 10')">TOP 항목</span>
                </div>
            </div>

            <div id="aiLoading" style="display: none; text-align: center; padding: 40px;">
                <div style="font-size: 40px; animation: spin 1s linear infinite;">⚙️</div>
                <p style="color: #666; margin-top: 10px;">AI가 분석 중입니다...</p>
            </div>

            <div id="aiResult" style="display: none;">
                <div id="aiDescription" style="background: #f0f7ff; padding: 15px; border-radius: 8px; margin-bottom: 20px; border-left: 4px solid #667eea;">
                </div>

                <div class="charts">
                    <div class="chart-container">
                        <h3>📊 분석 차트</h3>
                        <div style="height: 350px;"><canvas id="aiChart"></canvas></div>
                    </div>
                    <div class="chart-container">
                        <h3>📋 분석 결과</h3>
                        <div id="aiTableContainer" class="scroll-table" style="max-height: 350px;">
                        </div>
                    </div>
                </div>

                <div id="aiInsight" style="background: #fff8e1; padding: 15px; border-radius: 8px; margin-top: 20px; border-left: 4px solid #ffc107;">
                </div>
            </div>

            <div id="aiError" style="display: none; background: #ffebee; padding: 20px; border-radius: 8px; color: #c62828; border-left: 4px solid #c62828;">
            </div>

            <!-- 목표 달성 분석 섹션 -->
            <div style="background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; padding: 20px; border-radius: 10px; margin-top: 30px; margin-bottom: 20px;">
                <h2 style="margin: 0 0 10px 0;">🎯 목표 달성 분석</h2>
                <p style="margin: 0; opacity: 0.9;">영업담당별, 검사목적별, 항목별, 지역별 종합 분석 및 개선점 제안</p>
            </div>

            <div style="background: white; padding: 20px; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin-bottom: 20px;">
                <div style="display: flex; gap: 15px; align-items: center; flex-wrap: wrap; margin-bottom: 15px;">
                    <label style="font-weight: bold;">목표 연도:</label>
                    <select id="goalYear" style="padding: 10px; border-radius: 5px; border: 1px solid #ddd;">
                        <option value="2026">2026년</option>
                        <option value="2027">2027년</option>
                    </select>
                    <label style="font-weight: bold;">목표 매출:</label>
                    <input type="number" id="goalTarget" value="70" style="padding: 10px; width: 100px; border-radius: 5px; border: 1px solid #ddd;">
                    <span>억원</span>
                    <button onclick="runGoalAnalysis()"
                            style="padding: 12px 25px; background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); color: white; border: none; border-radius: 8px; cursor: pointer; font-size: 15px; font-weight: bold;">
                        🔍 종합 분석 실행
                    </button>
                </div>

                <!-- 세부 필터 선택 섹션 -->
                <div style="border-top: 1px solid #eee; padding-top: 15px;">
                    <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
                        <span style="font-weight: bold; color: #667eea;">📊 세부 필터 선택 (체크한 항목만 분석)</span>
                        <button onclick="toggleGoalFilters()" id="filterToggleBtn" style="padding: 5px 15px; background: #f0f0f0; border: 1px solid #ddd; border-radius: 5px; cursor: pointer; font-size: 13px;">
                            ▼ 필터 열기
                        </button>
                    </div>
                    <div id="goalFiltersPanel" style="display: none;">
                        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                            <!-- 영업담당 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>👤 영업담당</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalManagerAll" checked onchange="toggleAllGoalFilters('manager')"> 전체</label>
                                </div>
                                <div id="goalManagerFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;"></div>
                            </div>
                            <!-- 팀 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>👥 팀</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalTeamAll" checked onchange="toggleAllGoalFilters('team')"> 전체</label>
                                </div>
                                <div id="goalTeamFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;"></div>
                            </div>
                            <!-- 월 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>📅 월</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalMonthAll" checked onchange="toggleAllGoalFilters('month')"> 전체</label>
                                </div>
                                <div id="goalMonthFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;">
                                    <label><input type="checkbox" class="goalMonthFilter" value="1"> 1월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="2"> 2월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="3"> 3월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="4"> 4월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="5"> 5월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="6"> 6월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="7"> 7월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="8"> 8월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="9"> 9월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="10"> 10월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="11"> 11월</label>
                                    <label><input type="checkbox" class="goalMonthFilter" value="12"> 12월</label>
                                </div>
                            </div>
                            <!-- 검사목적 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>🎯 검사목적</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalPurposeAll" checked onchange="toggleAllGoalFilters('purpose')"> 전체</label>
                                </div>
                                <div id="goalPurposeFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;"></div>
                            </div>
                            <!-- 지역 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>📍 지역</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalRegionAll" checked onchange="toggleAllGoalFilters('region')"> 전체</label>
                                </div>
                                <div id="goalRegionFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;"></div>
                            </div>
                            <!-- 검체유형 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>🧪 검체유형</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalSampleTypeAll" checked onchange="toggleAllGoalFilters('sampleType')"> 전체</label>
                                </div>
                                <div id="goalSampleTypeFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;"></div>
                            </div>
                            <!-- 분석자 -->
                            <div style="background: #f8f9fa; padding: 12px; border-radius: 8px;">
                                <div style="font-weight: bold; margin-bottom: 8px; display: flex; justify-content: space-between;">
                                    <span>🔬 분석자</span>
                                    <label style="font-size: 12px;"><input type="checkbox" id="goalAnalyzerAll" checked onchange="toggleAllGoalFilters('analyzer')"> 전체</label>
                                </div>
                                <div id="goalAnalyzerFilters" style="max-height: 120px; overflow-y: auto; font-size: 13px;"></div>
                            </div>
                        </div>
                        <div style="margin-top: 10px; text-align: center;">
                            <small style="color: #888;">💡 전체 체크 시 해당 필터는 적용하지 않음 (모든 데이터 포함)</small>
                        </div>
                    </div>
                </div>
            </div>

            <div id="goalLoading" style="display: none; text-align: center; padding: 40px;">
                <div style="font-size: 40px; animation: spin 1s linear infinite;">📊</div>
                <p style="color: #666; margin-top: 10px;">종합 분석 중입니다... (Gemini API 불필요)</p>
            </div>

            <div id="goalResult" style="display: none;">
                <!-- 현황 요약 -->
                <div id="goalSummary" style="background: #f8f9fa; padding: 20px; border-radius: 10px; margin-bottom: 20px;">
                </div>

                <!-- 추천사항 -->
                <div id="goalRecommendations" style="margin-bottom: 20px;">
                </div>

                <!-- 상세 분석 테이블들 -->
                <div class="charts">
                    <div class="chart-container">
                        <h3>👤 영업담당별 분석</h3>
                        <div class="scroll-table" style="max-height: 300px;">
                            <table id="goalManagerTable">
                                <thead><tr><th>담당자</th><th>2024</th><th>2025</th><th>성장률</th></tr></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                    <div class="chart-container">
                        <h3>🎯 검사목적별 분석</h3>
                        <div class="scroll-table" style="max-height: 300px;">
                            <table id="goalPurposeTable">
                                <thead><tr><th>검사목적</th><th>2024</th><th>2025</th><th>성장률</th><th>비중</th></tr></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                </div>

                <div class="charts" style="margin-top: 20px;">
                    <div class="chart-container">
                        <h3>📍 지역별 분석</h3>
                        <div class="scroll-table" style="max-height: 300px;">
                            <table id="goalRegionTable">
                                <thead><tr><th>지역</th><th>2024</th><th>2025</th><th>성장률</th></tr></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                    <div class="chart-container">
                        <h3>🔬 항목별 분석 (TOP 20)</h3>
                        <div class="scroll-table" style="max-height: 300px;">
                            <table id="goalItemTable">
                                <thead><tr><th>항목명</th><th>2024</th><th>2025</th><th>성장률</th></tr></thead>
                                <tbody></tbody>
                            </table>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <!-- 기업 정보 탭 -->
    <div id="companyInfo" class="tab-content">
        <div style="max-width: 1200px; margin: 0 auto; padding: 20px;">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px;">
                <h2 style="margin: 0; color: #333;">🏢 기업 정보 관리</h2>
                <div>
                    <button onclick="loadCompanyInfo()" style="padding: 10px 20px; background: #3498db; color: white; border: none; border-radius: 5px; cursor: pointer; margin-right: 10px;">📥 불러오기</button>
                    <button onclick="saveCompanyInfo()" style="padding: 10px 20px; background: #27ae60; color: white; border: none; border-radius: 5px; cursor: pointer;">💾 저장하기</button>
                </div>
            </div>

            <!-- 기본 정보 섹션 -->
            <div style="background: white; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <h3 style="margin-top: 0; color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">📋 기업 기본 정보</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                    <div>
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">기업명</label>
                        <input type="text" id="companyName" placeholder="회사명을 입력하세요" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box;">
                    </div>
                    <div>
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">설립연도</label>
                        <input type="text" id="foundedYear" placeholder="예: 2010" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box;">
                    </div>
                    <div style="grid-column: span 2;">
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">사업 분야</label>
                        <input type="text" id="businessField" placeholder="예: 식품 검사, 환경 분석, 품질 인증" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box;">
                    </div>
                    <div style="grid-column: span 2;">
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">주요 서비스</label>
                        <textarea id="mainServices" rows="2" placeholder="제공하는 주요 서비스를 설명하세요" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box; resize: vertical;"></textarea>
                    </div>
                </div>
            </div>

            <!-- 경영 목표 섹션 -->
            <div style="background: white; border-radius: 10px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <h3 style="margin-top: 0; color: #2c3e50; border-bottom: 2px solid #e74c3c; padding-bottom: 10px;">🎯 경영 목표 및 지표</h3>
                <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 15px;">
                    <div>
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">연간 매출 목표 (억원)</label>
                        <input type="number" id="revenueTarget" placeholder="예: 50" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box;">
                    </div>
                    <div>
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">연간 검사 건수 목표</label>
                        <input type="number" id="inspectionTarget" placeholder="예: 100000" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box;">
                    </div>
                    <div style="grid-column: span 2;">
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">핵심 성과 지표 (KPI)</label>
                        <textarea id="kpiDescription" rows="2" placeholder="예: 고객 만족도 95% 이상, 검사 정확도 99.9%, 납기 준수율 98%" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box; resize: vertical;"></textarea>
                    </div>
                    <div style="grid-column: span 2;">
                        <label style="display: block; font-weight: bold; margin-bottom: 5px;">경영 전략 및 중점 사항</label>
                        <textarea id="businessStrategy" rows="3" placeholder="올해의 주요 경영 전략과 중점 추진 사항을 입력하세요" style="width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 5px; box-sizing: border-box; resize: vertical;"></textarea>
                    </div>
                </div>
            </div>

            <!-- 부서 및 인력 정보 섹션 -->
            <div style="background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <h3 style="margin-top: 0; color: #2c3e50; border-bottom: 2px solid #9b59b6; padding-bottom: 10px;">👥 부서별 조직 및 업무</h3>
                <div style="overflow-x: auto;">
                    <table id="departmentTable" style="width: 100%; border-collapse: collapse; margin-top: 10px;">
                        <thead>
                            <tr style="background: #34495e; color: white;">
                                <th style="padding: 12px; text-align: left; width: 15%;">부서</th>
                                <th style="padding: 12px; text-align: center; width: 10%;">인원수</th>
                                <th style="padding: 12px; text-align: left; width: 15%;">책임자</th>
                                <th style="padding: 12px; text-align: left; width: 60%;">주요 업무</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">임원</td>
                                <td style="padding: 5px;"><input type="number" id="dept_executive_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_executive_head" placeholder="대표이사" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_executive_role" placeholder="경영 총괄, 전략 수립, 대외 협력" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">총무</td>
                                <td style="padding: 5px;"><input type="number" id="dept_admin_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_admin_head" placeholder="총무팀장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_admin_role" placeholder="인사, 총무, 시설 관리, 구매" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">재무</td>
                                <td style="padding: 5px;"><input type="number" id="dept_finance_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_finance_head" placeholder="재무팀장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_finance_role" placeholder="회계, 세무, 예산 관리, 자금 운용" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">품질보증</td>
                                <td style="padding: 5px;"><input type="number" id="dept_qa_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_qa_head" placeholder="품질보증팀장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_qa_role" placeholder="품질 관리, 인증 관리, 고객 불만 처리" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">고객지원</td>
                                <td style="padding: 5px;"><input type="number" id="dept_support_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_support_head" placeholder="고객지원팀장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_support_role" placeholder="고객 상담, 접수, 결과 발송, CS 관리" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">분석실</td>
                                <td style="padding: 5px;"><input type="number" id="dept_lab_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_lab_head" placeholder="분석실장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_lab_role" placeholder="시료 분석, 검사 수행, 성적서 작성, 장비 관리" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">직영 영업부</td>
                                <td style="padding: 5px;"><input type="number" id="dept_sales_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_sales_head" placeholder="영업부장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_sales_role" placeholder="신규 고객 발굴, 기존 고객 관리, 매출 확대" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">지사</td>
                                <td style="padding: 5px;"><input type="number" id="dept_branch_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_branch_head" placeholder="지사장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_branch_role" placeholder="지역 영업, 시료 수거, 현장 서비스" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                            <tr style="border-bottom: 1px solid #eee;">
                                <td style="padding: 10px; font-weight: bold; background: #f8f9fa;">마케팅</td>
                                <td style="padding: 5px;"><input type="number" id="dept_marketing_count" value="0" min="0" style="width: 60px; padding: 5px; text-align: center; border: 1px solid #ddd; border-radius: 3px;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_marketing_head" placeholder="마케팅팀장" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                                <td style="padding: 5px;"><input type="text" id="dept_marketing_role" placeholder="홍보, 브랜딩, 온라인 마케팅, 이벤트 기획" style="width: 100%; padding: 5px; border: 1px solid #ddd; border-radius: 3px; box-sizing: border-box;"></td>
                            </tr>
                        </tbody>
                    </table>
                </div>
                <div style="margin-top: 15px; padding: 10px; background: #ecf0f1; border-radius: 5px;">
                    <strong>총 인원:</strong> <span id="totalEmployees">0</span>명
                </div>
            </div>

            <!-- 영업부 인력 상세 -->
            <div style="background: white; border-radius: 10px; padding: 20px; margin-top: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h3 style="margin: 0; color: #2c3e50; border-bottom: 2px solid #3498db; padding-bottom: 10px;">👔 직영 영업부 인력</h3>
                    <button onclick="addSalesPerson()" style="padding: 8px 15px; background: #3498db; color: white; border: none; border-radius: 5px; cursor: pointer;">+ 인력 추가</button>
                </div>
                <div id="salesPersonList">
                    <!-- 동적으로 추가되는 영업부 인력 -->
                </div>
                <div id="salesPersonEmpty" style="color: #888; text-align: center; padding: 20px;">
                    아직 등록된 영업 담당자가 없습니다. [+ 인력 추가] 버튼을 클릭해 추가하세요.
                </div>
            </div>

            <!-- 지사 인력 상세 -->
            <div style="background: white; border-radius: 10px; padding: 20px; margin-top: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
                    <h3 style="margin: 0; color: #2c3e50; border-bottom: 2px solid #e67e22; padding-bottom: 10px;">🏬 지사 인력</h3>
                    <button onclick="addBranchPerson()" style="padding: 8px 15px; background: #e67e22; color: white; border: none; border-radius: 5px; cursor: pointer;">+ 인력 추가</button>
                </div>
                <div id="branchPersonList">
                    <!-- 동적으로 추가되는 지사 인력 -->
                </div>
                <div id="branchPersonEmpty" style="color: #888; text-align: center; padding: 20px;">
                    아직 등록된 지사 담당자가 없습니다. [+ 인력 추가] 버튼을 클릭해 추가하세요.
                </div>
            </div>

            <!-- AI 분석 참고사항 -->
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 10px; padding: 20px; margin-top: 20px; color: white;">
                <h3 style="margin-top: 0;">💡 AI 분석에 활용됩니다</h3>
                <p style="margin-bottom: 0; opacity: 0.9;">
                    입력하신 기업 정보는 AI 분석 탭에서 질문할 때 자동으로 참고되어,
                    귀사의 상황에 맞는 맞춤형 분석과 조언을 제공합니다.
                    정확한 정보를 입력할수록 더 유용한 인사이트를 얻을 수 있습니다.
                </p>
            </div>
        </div>
    </div>

    <style>
        @keyframes spin {
            from { transform: rotate(0deg); }
            to { transform: rotate(360deg); }
        }
    </style>

    <script>
        let charts = {};
        let currentData = null;
        let compareData = null;
        let foodItemData = null;
        let compareFoodItemData = null;

        function formatCurrency(value) {
            if (value >= 100000000) return (value/100000000).toFixed(1) + '억';
            if (value >= 10000) return (value/10000).toFixed(0) + '만';
            return value.toLocaleString();
        }

        function formatDiff(current, compare) {
            if (!compare) return '';
            const diff = current - compare;
            const percent = compare > 0 ? ((diff / compare) * 100).toFixed(1) : 0;
            const sign = diff >= 0 ? '+' : '';
            return { diff, percent, sign, text: `${sign}${formatCurrency(Math.abs(diff))} (${sign}${percent}%)` };
        }

        function showToast(message, type = 'success', duration = 3000) {
            const toast = document.getElementById('toast');
            toast.textContent = message;
            toast.className = 'toast ' + type;
            toast.style.display = 'block';
            if (type !== 'loading') setTimeout(() => { toast.style.display = 'none'; }, duration);
        }

        function hideToast() { document.getElementById('toast').style.display = 'none'; }

        // 테이블 정렬 함수
        function sortTable(tableId, colIndex, type = 'string') {
            const table = document.getElementById(tableId);
            const thead = table.querySelector('thead');
            const tbody = table.querySelector('tbody');
            const rows = Array.from(tbody.querySelectorAll('tr'));
            const th = thead.querySelectorAll('th')[colIndex];

            // 현재 정렬 상태 확인
            const isAsc = th.classList.contains('asc');

            // 모든 헤더에서 정렬 클래스 제거
            thead.querySelectorAll('th').forEach(h => h.classList.remove('asc', 'desc'));

            // 새로운 정렬 방향 설정
            th.classList.add(isAsc ? 'desc' : 'asc');

            // 정렬
            rows.sort((a, b) => {
                let aVal = a.cells[colIndex]?.textContent?.trim() || '';
                let bVal = b.cells[colIndex]?.textContent?.trim() || '';

                // 숫자 파싱 (억, 만, %, +, - 등 처리)
                if (type === 'number' || type === 'currency') {
                    aVal = parseTableNumber(aVal);
                    bVal = parseTableNumber(bVal);
                }

                if (type === 'number' || type === 'currency') {
                    return isAsc ? bVal - aVal : aVal - bVal;
                } else {
                    return isAsc ? bVal.localeCompare(aVal, 'ko') : aVal.localeCompare(bVal, 'ko');
                }
            });

            // 정렬된 행 다시 삽입
            rows.forEach(row => tbody.appendChild(row));

            // 순위 컬럼 업데이트 (첫 번째 컬럼이 순위인 경우)
            const firstHeader = thead.querySelector('th')?.textContent?.trim();
            if (firstHeader === '순위') {
                rows.forEach((row, i) => {
                    if (row.cells[0]) row.cells[0].textContent = i + 1;
                });
            }
        }

        // 테이블 숫자 파싱 (억, 만, %, 콤마 등 처리)
        function parseTableNumber(str) {
            if (!str) return 0;
            str = str.replace(/[,\s]/g, '').replace(/\(.*\)/g, ''); // 콤마, 공백, 괄호 제거

            // 억 단위
            if (str.includes('억')) {
                const match = str.match(/([-+]?\d+\.?\d*)억/);
                if (match) return parseFloat(match[1]) * 100000000;
            }
            // 만 단위
            if (str.includes('만')) {
                const match = str.match(/([-+]?\d+\.?\d*)만/);
                if (match) return parseFloat(match[1]) * 10000;
            }
            // % 제거
            str = str.replace(/%/g, '');
            // +/- 기호 처리
            const num = parseFloat(str.replace(/[^-\d.]/g, ''));
            return isNaN(num) ? 0 : num;
        }

        // 테이블에 정렬 기능 적용
        function makeSortable(tableId, columnTypes) {
            const table = document.getElementById(tableId);
            if (!table) return;

            const headers = table.querySelectorAll('thead th');
            headers.forEach((th, index) => {
                if (columnTypes[index] !== 'none') {
                    th.classList.add('sortable');
                    th.onclick = () => sortTable(tableId, index, columnTypes[index] || 'string');
                }
            });
        }

        // 날짜 선택기 초기화 및 관련 함수들
        function initDateSelectors() {
            // 월 선택기 초기화
            const months = ['monthSelect', 'endMonthSelect', 'compareMonthSelect', 'compareEndMonthSelect'];
            months.forEach(id => {
                const select = document.getElementById(id);
                select.innerHTML = '<option value="">전체</option>';
                for (let i = 1; i <= 12; i++) {
                    select.innerHTML += `<option value="${i}">${i}월</option>`;
                }
            });
        }

        function updateDaySelector() {
            const year = parseInt(document.getElementById('yearSelect').value);
            const month = parseInt(document.getElementById('monthSelect').value);
            updateDayOptions('daySelect', year, month);
        }

        function updateEndDaySelector() {
            const year = parseInt(document.getElementById('endYearSelect').value);
            const month = parseInt(document.getElementById('endMonthSelect').value);
            updateDayOptions('endDaySelect', year, month);
        }

        function updateCompareDaySelector() {
            const year = parseInt(document.getElementById('compareYearSelect').value);
            const month = parseInt(document.getElementById('compareMonthSelect').value);
            updateDayOptions('compareDaySelect', year, month);
        }

        function updateCompareEndDaySelector() {
            const year = parseInt(document.getElementById('compareEndYearSelect').value);
            const month = parseInt(document.getElementById('compareEndMonthSelect').value);
            updateDayOptions('compareEndDaySelect', year, month);
        }

        function updateDayOptions(selectId, year, month) {
            const select = document.getElementById(selectId);
            const currentValue = select.value;
            select.innerHTML = '<option value="">전체</option>';

            if (!month) return;

            const daysInMonth = new Date(year, month, 0).getDate();
            for (let i = 1; i <= daysInMonth; i++) {
                select.innerHTML += `<option value="${i}">${i}일</option>`;
            }

            // 이전 값 복원 (유효한 경우)
            if (currentValue && parseInt(currentValue) <= daysInMonth) {
                select.value = currentValue;
            }
        }

        function updateDateSelectors() {
            updateDaySelector();
        }

        function updateEndDateSelectors() {
            updateEndDaySelector();
        }

        function toggleRangeMode() {
            const rangeMode = document.getElementById('rangeCheck').checked;
            document.getElementById('rangeDateGroup').style.display = rangeMode ? 'flex' : 'none';

            // 범위 모드일 때 일 선택기 숨김 (시작)
            document.getElementById('daySelect').style.display = rangeMode ? 'inline-block' : 'inline-block';

            // 비교 모드가 활성화되어 있으면 비교 범위도 표시
            if (document.getElementById('compareCheck').checked) {
                document.getElementById('compareRangeDateGroup').style.display = rangeMode ? 'flex' : 'none';
            }
        }

        function toggleCompare() {
            const compareEnabled = document.getElementById('compareCheck').checked;
            const rangeMode = document.getElementById('rangeCheck').checked;

            document.getElementById('compareDateGroup').style.display = compareEnabled ? 'flex' : 'none';
            document.getElementById('compareRangeDateGroup').style.display = (compareEnabled && rangeMode) ? 'flex' : 'none';
        }

        function showTab(tabId) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelector(`[onclick="showTab('${tabId}')"]`).classList.add('active');
            document.getElementById(tabId).classList.add('active');
        }

        function getDateParams(prefix = '') {
            const year = document.getElementById(prefix + 'yearSelect').value;
            const month = document.getElementById(prefix + 'monthSelect').value;
            const day = document.getElementById(prefix + 'daySelect').value;
            return { year, month, day };
        }

        function getEndDateParams(prefix = '') {
            const year = document.getElementById(prefix + 'endYearSelect').value;
            const month = document.getElementById(prefix + 'endMonthSelect').value;
            const day = document.getElementById(prefix + 'endDaySelect').value;
            return { year, month, day };
        }

        function buildDateQuery(start, end = null) {
            let query = `year=${start.year}`;
            if (start.month) query += `&month=${start.month}`;
            if (start.day) query += `&day=${start.day}`;
            if (end) {
                query += `&end_year=${end.year}`;
                if (end.month) query += `&end_month=${end.month}`;
                if (end.day) query += `&end_day=${end.day}`;
            }
            return query;
        }

        function formatDateLabel(start, end = null) {
            let label = `${start.year}년`;
            if (start.month) label += ` ${start.month}월`;
            if (start.day) label += ` ${start.day}일`;
            if (end) {
                let endLabel = `${end.year}년`;
                if (end.month) endLabel += ` ${end.month}월`;
                if (end.day) endLabel += ` ${end.day}일`;
                label += ` ~ ${endLabel}`;
            }
            return label;
        }

        async function loadData() {
            const rangeMode = document.getElementById('rangeCheck').checked;
            const compareEnabled = document.getElementById('compareCheck').checked;
            const purpose = document.getElementById('purposeSelect').value;
            const btn = document.getElementById('btnSearch');

            // 시작 날짜
            const startDate = getDateParams('');
            let endDate = null;
            if (rangeMode) {
                endDate = getEndDateParams('');
            }

            btn.disabled = true;
            btn.textContent = '로딩중...';
            showToast('데이터를 불러오는 중입니다...', 'loading');

            try {
                const dateQuery = buildDateQuery(startDate, endDate);
                const response = await fetch(`/api/data?${dateQuery}&purpose=${encodeURIComponent(purpose)}`);
                currentData = await response.json();
                currentData.dateLabel = formatDateLabel(startDate, endDate);
                currentData.year = startDate.year;  // 호환성 유지

                // 검사목적 드롭다운 업데이트
                updatePurposeSelect(currentData.purposes);

                if (compareEnabled) {
                    const compareStartDate = {
                        year: document.getElementById('compareYearSelect').value,
                        month: document.getElementById('compareMonthSelect').value,
                        day: document.getElementById('compareDaySelect').value
                    };
                    let compareEndDate = null;
                    if (rangeMode) {
                        compareEndDate = {
                            year: document.getElementById('compareEndYearSelect').value,
                            month: document.getElementById('compareEndMonthSelect').value,
                            day: document.getElementById('compareEndDaySelect').value
                        };
                    }

                    const compareDateQuery = buildDateQuery(compareStartDate, compareEndDate);
                    const compareResponse = await fetch(`/api/data?${compareDateQuery}&purpose=${encodeURIComponent(purpose)}`);
                    compareData = await compareResponse.json();
                    compareData.dateLabel = formatDateLabel(compareStartDate, compareEndDate);
                    compareData.year = compareStartDate.year;  // 호환성 유지
                } else {
                    compareData = null;
                }

                updateAll();

                // 검사항목 데이터도 함께 로드
                loadFoodItemData();

                let msg = `${currentData.dateLabel} 데이터 로드 완료 (${currentData.total_count.toLocaleString()}건)`;
                if (compareData) msg = `${currentData.dateLabel} vs ${compareData.dateLabel} 비교 로드 완료`;
                showToast(msg, 'success');

            } catch (error) {
                console.error('Error:', error);
                showToast('데이터 로드 중 오류가 발생했습니다.', 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = '조회하기';
            }
        }

        function updatePurposeSelect(purposes) {
            const select = document.getElementById('purposeSelect');
            const currentValue = select.value;
            select.innerHTML = '<option value="전체">검사목적: 전체</option>';
            purposes.forEach(p => {
                if (p) select.innerHTML += `<option value="${p}">${p}</option>`;
            });
            if (purposes.includes(currentValue)) select.value = currentValue;
        }

        function updateAll() {
            const steps = [
                ['updateSummary', updateSummary],
                ['updateManagerChart', updateManagerChart],
                ['updateBranchChart', updateBranchChart],
                ['updateMonthlyChart', updateMonthlyChart],
                ['updateManagerTable', updateManagerTable],
                ['updateBranchTable', updateBranchTable],
                ['updateClientTables', updateClientTables],
                ['updateRegionTables', updateRegionTables],
                ['updateRegionSelects', updateRegionSelects],
                ['updatePurposeCheckboxes', updatePurposeCheckboxes],
                ['updatePurposeTab', updatePurposeTab],
                ['updateSampleTypeFilters', updateSampleTypeFilters],
                ['updateSampleTypeTab', updateSampleTypeTab],
                ['updateDefectPurposeFilter', updateDefectPurposeFilter],
                ['updateDefectTab', updateDefectTab],
                ['applyAllSortable', applyAllSortable]
            ];

            for (const [name, fn] of steps) {
                try {
                    console.log(`[UPDATE] ${name} 시작...`);
                    fn();
                    console.log(`[UPDATE] ${name} 완료 ✓`);
                } catch (e) {
                    console.error(`[UPDATE ERROR] ${name} 실패:`, e);
                    throw e;
                }
            }
            console.log('[UPDATE] 모든 업데이트 완료');
        }

        // 모든 테이블에 정렬 기능 적용
        function applyAllSortable() {
            // 비교 모드 여부에 따라 컬럼 타입 결정
            const hasCompare = !!compareData;

            // 업체별 탭 테이블 (비교 모드)
            if (hasCompare) {
                // 순위, 거래처, 2025년, 2024년, 증감, 2025건수, 2024건수
                makeSortable('clientTopTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number']);
                // 거래처, 평균단가, 2025년, 2024년, 증감, 2025건수, 2024건수
                makeSortable('clientEffTable', ['string', 'currency', 'currency', 'currency', 'currency', 'number', 'number']);
                // 거래처, 2025건수, 2024건수, 증감, 2025매출, 2024매출
                makeSortable('clientVolTable', ['string', 'number', 'number', 'number', 'currency', 'currency']);
            } else {
                makeSortable('clientTopTable', ['number', 'string', 'currency', 'number', 'currency']);
                makeSortable('clientEffTable', ['string', 'currency', 'currency', 'number']);
                makeSortable('clientVolTable', ['string', 'number', 'currency', 'currency']);
            }

            // 지역별 테이블
            if (hasCompare) {
                makeSortable('regionTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number']);
            } else {
                makeSortable('regionTable', ['number', 'string', 'currency', 'number', 'currency']);
            }

            // 담당자 테이블 (개인별 탭)
            if (hasCompare) {
                makeSortable('managerTable', ['string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
            } else {
                makeSortable('managerTable', ['string', 'currency', 'number', 'number']);
            }

            // 지사/센터 테이블
            if (hasCompare) {
                makeSortable('branchTable', ['string', 'currency', 'currency', 'currency', 'number', 'number']);
            } else {
                makeSortable('branchTable', ['string', 'currency', 'number', 'number']);
            }

            // 목적별 탭 테이블
            if (hasCompare) {
                makeSortable('purposeTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
                makeSortable('purposeManagerTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
                makeSortable('purposeRegionTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
            } else {
                makeSortable('purposeTable', ['number', 'string', 'currency', 'number', 'currency', 'number']);
                makeSortable('purposeManagerTable', ['number', 'string', 'currency', 'number', 'currency', 'number']);
                makeSortable('purposeRegionTable', ['number', 'string', 'currency', 'number', 'currency', 'number']);
            }

            // 검체유형 탭 테이블
            if (hasCompare) {
                makeSortable('sampleTypeTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
                makeSortable('sampleTypeManagerTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
                makeSortable('sampleTypePurposeTable', ['number', 'string', 'currency', 'currency', 'currency', 'number', 'number', 'number']);
            } else {
                makeSortable('sampleTypeTable', ['number', 'string', 'currency', 'number', 'currency', 'number']);
                makeSortable('sampleTypeManagerTable', ['number', 'string', 'currency', 'number', 'currency', 'number']);
                makeSortable('sampleTypePurposeTable', ['number', 'string', 'currency', 'number', 'currency', 'number']);
            }

            // 부적합 탭 테이블
            if (hasCompare) {
                makeSortable('defectTable', ['number', 'string', 'number', 'number', 'number', 'number']);
            } else {
                makeSortable('defectTable', ['number', 'string', 'number', 'number']);
            }
        }

        function updateSummary() {
            document.getElementById('totalSales').textContent = formatCurrency(currentData.total_sales);
            document.getElementById('totalCount').textContent = currentData.total_count.toLocaleString() + '건';
            const avgPrice = currentData.total_count > 0 ? currentData.total_sales / currentData.total_count : 0;
            document.getElementById('avgPrice').textContent = formatCurrency(avgPrice);

            if (compareData) {
                const compAvg = compareData.total_count > 0 ? compareData.total_sales / compareData.total_count : 0;
                const compLabel = compareData.dateLabel || compareData.year + '년';
                document.getElementById('compareTotalSales').textContent = `${compLabel}: ${formatCurrency(compareData.total_sales)}`;
                document.getElementById('compareTotalSales').style.display = 'block';
                const salesDiff = formatDiff(currentData.total_sales, compareData.total_sales);
                document.getElementById('diffTotalSales').textContent = salesDiff.text;
                document.getElementById('diffTotalSales').className = 'diff ' + (salesDiff.diff >= 0 ? 'positive' : 'negative');

                document.getElementById('compareTotalCount').textContent = `${compLabel}: ${compareData.total_count.toLocaleString()}건`;
                document.getElementById('compareTotalCount').style.display = 'block';
                const countDiff = formatDiff(currentData.total_count, compareData.total_count);
                document.getElementById('diffTotalCount').textContent = countDiff.text;
                document.getElementById('diffTotalCount').className = 'diff ' + (countDiff.diff >= 0 ? 'positive' : 'negative');

                document.getElementById('compareAvgPrice').textContent = `${compLabel}: ${formatCurrency(compAvg)}`;
                document.getElementById('compareAvgPrice').style.display = 'block';
                const avgDiff = formatDiff(avgPrice, compAvg);
                document.getElementById('diffAvgPrice').textContent = avgDiff.text;
                document.getElementById('diffAvgPrice').className = 'diff ' + (avgDiff.diff >= 0 ? 'positive' : 'negative');
            } else {
                ['compareTotalSales', 'compareTotalCount', 'compareAvgPrice'].forEach(id => {
                    document.getElementById(id).style.display = 'none';
                });
                ['diffTotalSales', 'diffTotalCount', 'diffAvgPrice'].forEach(id => {
                    document.getElementById(id).textContent = '';
                });
            }
        }

        function updateManagerChart() {
            const top15 = currentData.by_manager.slice(0, 15);
            const ctx = document.getElementById('managerChart').getContext('2d');
            if (charts.manager) charts.manager.destroy();

            const datasets = [{ label: currentData.year + '년', data: top15.map(d => d[1].sales), backgroundColor: 'rgba(102, 126, 234, 0.8)' }];

            if (compareData) {
                const compareMap = Object.fromEntries(compareData.by_manager);
                datasets.push({ label: compareData.year + '년', data: top15.map(d => compareMap[d[0]]?.sales || 0), backgroundColor: 'rgba(118, 75, 162, 0.6)' });
                document.getElementById('managerLegend').innerHTML = `<div class="legend-item"><div class="legend-color" style="background:rgba(102,126,234,0.8)"></div>${currentData.year}년</div><div class="legend-item"><div class="legend-color" style="background:rgba(118,75,162,0.6)"></div>${compareData.year}년</div>`;
                document.getElementById('managerLegend').style.display = 'flex';
            } else {
                document.getElementById('managerLegend').style.display = 'none';
            }

            charts.manager = new Chart(ctx, {
                type: 'bar',
                data: { labels: top15.map(d => d[0]), datasets },
                options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => formatCurrency(v) } } } }
            });

            // 업체별 탭 담당자 필터 드롭다운 업데이트
            const clientManagerFilter = document.getElementById('clientManagerFilter');
            const currentFilter = clientManagerFilter.value;
            clientManagerFilter.innerHTML = '<option value="">전체 담당자</option>';
            currentData.by_manager.forEach(m => {
                clientManagerFilter.innerHTML += `<option value="${m[0]}">${m[0]}</option>`;
            });
            if (currentFilter) clientManagerFilter.value = currentFilter;
        }

        function updateBranchChart() {
            const ctx = document.getElementById('branchChart').getContext('2d');
            if (charts.branch) charts.branch.destroy();

            if (compareData) {
                const labels = currentData.by_branch.map(d => d[0]);
                const compareMap = Object.fromEntries(compareData.by_branch);
                document.getElementById('branchLegend').innerHTML = `<div class="legend-item"><div class="legend-color" style="background:rgba(102,126,234,0.8)"></div>${currentData.year}년</div><div class="legend-item"><div class="legend-color" style="background:rgba(118,75,162,0.6)"></div>${compareData.year}년</div>`;
                document.getElementById('branchLegend').style.display = 'flex';
                charts.branch = new Chart(ctx, {
                    type: 'bar',
                    data: { labels, datasets: [
                        { label: currentData.year + '년', data: currentData.by_branch.map(d => d[1].sales), backgroundColor: 'rgba(102, 126, 234, 0.8)' },
                        { label: compareData.year + '년', data: labels.map(l => compareMap[l]?.sales || 0), backgroundColor: 'rgba(118, 75, 162, 0.6)' }
                    ]},
                    options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => formatCurrency(v) } } } }
                });
            } else {
                document.getElementById('branchLegend').style.display = 'none';
                charts.branch = new Chart(ctx, {
                    type: 'pie',
                    data: { labels: currentData.by_branch.map(d => d[0]), datasets: [{ data: currentData.by_branch.map(d => d[1].sales), backgroundColor: ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe', '#43e97b', '#fa709a', '#fee140'] }] },
                    options: { responsive: true, plugins: { legend: { position: 'right' } } }
                });
            }
        }

        function updateMonthlyChart() {
            const ctx = document.getElementById('monthlyChart').getContext('2d');
            if (charts.monthly) charts.monthly.destroy();

            const labels = []; for (let i = 1; i <= 12; i++) labels.push(i + '월');
            const currentMap = Object.fromEntries(currentData.by_month);
            const datasets = [{ label: currentData.year + '년', data: labels.map((_, i) => currentMap[i+1]?.sales || 0), borderColor: '#667eea', backgroundColor: 'rgba(102, 126, 234, 0.1)', fill: true, tension: 0.4 }];

            if (compareData) {
                const compareMap = Object.fromEntries(compareData.by_month);
                datasets.push({ label: compareData.year + '년', data: labels.map((_, i) => compareMap[i+1]?.sales || 0), borderColor: '#764ba2', backgroundColor: 'rgba(118, 75, 162, 0.1)', fill: true, tension: 0.4 });
                document.getElementById('monthlyLegend').innerHTML = `<div class="legend-item"><div class="legend-color" style="background:#667eea"></div>${currentData.year}년</div><div class="legend-item"><div class="legend-color" style="background:#764ba2"></div>${compareData.year}년</div>`;
                document.getElementById('monthlyLegend').style.display = 'flex';
            } else {
                document.getElementById('monthlyLegend').style.display = 'none';
            }

            charts.monthly = new Chart(ctx, {
                type: 'line', data: { labels, datasets },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => formatCurrency(v) } } } }
            });
        }

        function updateManagerTable() {
            const thead = document.getElementById('managerTableHead');
            const tbody = document.querySelector('#managerTable tbody');

            if (compareData) {
                thead.innerHTML = `<tr><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                const compareMap = Object.fromEntries(compareData.by_manager);
                tbody.innerHTML = currentData.by_manager.map(d => {
                    const compSales = compareMap[d[0]]?.sales || 0;
                    const compCount = compareMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    const countDiff = d[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${(d[1].sales / currentData.total_sales * 100).toFixed(1)}%</td></tr>`;
                }).join('');
            } else {
                thead.innerHTML = `<tr><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr>`;
                tbody.innerHTML = currentData.by_manager.map(d => `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${d[1].count}</td><td>${(d[1].sales / currentData.total_sales * 100).toFixed(1)}%</td></tr>`).join('');
            }
        }

        function updateBranchTable() {
            const thead = document.getElementById('branchTableHead');
            const tbody = document.querySelector('#branchTable tbody');

            if (compareData) {
                thead.innerHTML = `<tr><th>지사/센터</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                const compareMap = Object.fromEntries(compareData.by_branch);
                tbody.innerHTML = currentData.by_branch.map(d => {
                    const compSales = compareMap[d[0]]?.sales || 0;
                    const compCount = compareMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    const countDiff = d[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('');
            } else {
                thead.innerHTML = `<tr><th>지사/센터</th><th>매출액</th><th>건수</th><th>담당자수</th></tr>`;
                tbody.innerHTML = currentData.by_branch.map(d => `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${d[1].count}</td><td>${d[1].managers}명</td></tr>`).join('');
            }
        }

        function updateClientTables() {
            const selectedManager = document.getElementById('clientManagerFilter').value;

            // 연도 라벨 업데이트
            const yearLabel = document.getElementById('clientYearLabel');
            const currLabel = currentData.dateLabel || currentData.year + '년';
            if (compareData) {
                const compLabel = compareData.dateLabel || compareData.year + '년';
                yearLabel.textContent = `📅 ${currLabel} vs ${compLabel}`;
            } else {
                yearLabel.textContent = `📅 ${currLabel}`;
            }

            let clientData, effData, volData;
            let compareClientMap = {};

            // 비교 데이터 맵 생성
            if (compareData) {
                compareData.by_client.forEach(c => {
                    compareClientMap[c[0]] = c[1];
                });
            }

            if (selectedManager && currentData.manager_top_clients[selectedManager]) {
                // 담당자별 데이터 사용
                const managerClients = currentData.manager_top_clients[selectedManager];

                // 매출순 정렬
                clientData = managerClients.map(c => [c[0], {
                    sales: c[1].sales,
                    count: c[1].count,
                    avg: c[1].count > 0 ? c[1].sales / c[1].count : 0
                }]);

                // 고효율 (단가순)
                effData = [...clientData].sort((a, b) => b[1].avg - a[1].avg).slice(0, 20);

                // 대량 (건수순)
                volData = [...clientData].sort((a, b) => b[1].count - a[1].count).slice(0, 20);

                clientData = clientData.slice(0, 20);
            } else {
                // 전체 데이터 사용
                clientData = currentData.by_client.slice(0, 20);
                effData = currentData.high_efficiency;
                volData = currentData.high_volume;
            }

            // TOP 20 업체 (비교 모드 지원)
            const topThead = document.getElementById('clientTopTableHead');
            const topTbody = document.querySelector('#clientTopTable tbody');

            if (compareData) {
                topThead.innerHTML = `<tr><th>순위</th><th>거래처</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                topTbody.innerHTML = clientData.map((d, i) => {
                    const compSales = compareClientMap[d[0]]?.sales || 0;
                    const compCount = compareClientMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    const countDiff = d[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('') || '<tr><td colspan="9">데이터 없음</td></tr>';
            } else {
                topThead.innerHTML = `<tr><th>순위</th><th>거래처</th><th>매출액</th><th>건수</th><th>평균단가</th></tr>`;
                topTbody.innerHTML = clientData.map((d, i) =>
                    `<tr><td>${i+1}</td><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${d[1].count}</td><td>${formatCurrency(d[1].avg)}</td></tr>`
                ).join('') || '<tr><td colspan="5">데이터 없음</td></tr>';
            }

            // 고효율 업체 (비교 모드 지원)
            const effThead = document.getElementById('clientEffTableHead');
            const effTbody = document.querySelector('#clientEffTable tbody');

            if (compareData) {
                effThead.innerHTML = `<tr><th>거래처</th><th>평균단가</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                effTbody.innerHTML = effData.map(d => {
                    const compSales = compareClientMap[d[0]]?.sales || 0;
                    const compCount = compareClientMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    const countDiff = d[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].avg)}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('') || '<tr><td colspan="9">데이터 없음</td></tr>';
            } else {
                effThead.innerHTML = `<tr><th>거래처</th><th>평균단가</th><th>매출액</th><th>건수</th></tr>`;
                effTbody.innerHTML = effData.map(d =>
                    `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].avg)}</td><td>${formatCurrency(d[1].sales)}</td><td>${d[1].count}</td></tr>`
                ).join('') || '<tr><td colspan="4">데이터 없음</td></tr>';
            }

            // 대량 업체 (비교 모드 지원)
            const volThead = document.getElementById('clientVolTableHead');
            const volTbody = document.querySelector('#clientVolTable tbody');

            if (compareData) {
                volThead.innerHTML = `<tr><th>거래처</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>${currentData.year}년 매출</th><th>${compareData.year}년 매출</th></tr>`;
                volTbody.innerHTML = volData.map(d => {
                    const compCount = compareClientMap[d[0]]?.count || 0;
                    const compSales = compareClientMap[d[0]]?.sales || 0;
                    const diff = d[1].count - compCount;
                    const diffRate = compCount > 0 ? ((diff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${diff.toLocaleString()}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${diffRate}%</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td></tr>`;
                }).join('') || '<tr><td colspan="7">데이터 없음</td></tr>';
            } else {
                volThead.innerHTML = `<tr><th>거래처</th><th>건수</th><th>매출액</th><th>평균단가</th></tr>`;
                volTbody.innerHTML = volData.map(d =>
                    `<tr><td>${d[0]}</td><td>${d[1].count}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(d[1].avg)}</td></tr>`
                ).join('') || '<tr><td colspan="4">데이터 없음</td></tr>';
            }
        }

        function updateDefectPurposeFilter() {
            const filter = document.getElementById('defectPurposeFilter');
            const currentValue = filter.value;
            filter.innerHTML = '<option value="">전체</option>';
            if (currentData.purposes) {
                currentData.purposes.forEach(p => {
                    if (p) filter.innerHTML += `<option value="${p}">${p}</option>`;
                });
            }
            if (currentValue) filter.value = currentValue;
        }

        function updateDefectTab() {
            const selectedPurpose = document.getElementById('defectPurposeFilter').value;

            // 필터 라벨 업데이트
            const filterLabel = selectedPurpose ? `[${selectedPurpose}]` : '';
            document.getElementById('defectChartFilterLabel').textContent = filterLabel;
            document.getElementById('defectTableFilterLabel').textContent = filterLabel;
            document.getElementById('defectMonthlyFilterLabel').textContent = filterLabel;

            // 데이터 선택 (목적 필터 적용)
            let defectData = currentData.by_defect;
            let compareDefectData = compareData?.by_defect;

            if (selectedPurpose && currentData.by_defect_purpose && currentData.by_defect_purpose[selectedPurpose]) {
                defectData = currentData.by_defect_purpose[selectedPurpose];
            }
            if (selectedPurpose && compareData?.by_defect_purpose && compareData.by_defect_purpose[selectedPurpose]) {
                compareDefectData = compareData.by_defect_purpose[selectedPurpose];
            }

            updateDefectChart(defectData, compareDefectData);
            updateDefectTable(defectData, compareDefectData);
            updateDefectSelect(defectData);
        }

        function updateDefectChart(defectData, compareDefectData) {
            const ctx = document.getElementById('defectChart').getContext('2d');
            if (charts.defect) charts.defect.destroy();

            const top15 = defectData.slice(0, 15);
            const datasets = [{ label: currentData.year + '년', data: top15.map(d => d[1].count), backgroundColor: 'rgba(231, 76, 60, 0.8)' }];

            if (compareData && compareDefectData) {
                const compareMap = Object.fromEntries(compareDefectData);
                datasets.push({ label: compareData.year + '년', data: top15.map(d => compareMap[d[0]]?.count || 0), backgroundColor: 'rgba(155, 89, 182, 0.6)' });
            }

            charts.defect = new Chart(ctx, {
                type: 'bar',
                data: { labels: top15.map(d => d[0]), datasets },
                options: { responsive: true, plugins: { legend: { display: compareData ? true : false } }, scales: { y: { ticks: { callback: v => v.toLocaleString() } } } }
            });
        }

        function updateDefectTable(defectData, compareDefectData) {
            const thead = document.querySelector('#defectTable thead');
            const tbody = document.querySelector('#defectTable tbody');
            const totalDefects = defectData.reduce((sum, d) => sum + d[1].count, 0);

            if (compareData && compareDefectData) {
                const compareMap = Object.fromEntries(compareDefectData);
                thead.innerHTML = `<tr><th>순위</th><th>부적합항목</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = defectData.map((d, i) => {
                    const compCount = compareMap[d[0]]?.count || 0;
                    const diff = d[1].count - compCount;
                    const diffRate = compCount > 0 ? ((diff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td>${d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${diff.toLocaleString()}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${diffRate}%</td><td>${(d[1].count / totalDefects * 100).toFixed(1)}%</td></tr>`;
                }).join('');
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>부적합항목</th><th>건수</th><th>비중</th></tr>`;
                tbody.innerHTML = defectData.map((d, i) =>
                    `<tr><td>${i+1}</td><td>${d[0]}</td><td>${d[1].count}</td><td>${(d[1].count / totalDefects * 100).toFixed(1)}%</td></tr>`
                ).join('');
            }
        }

        function updateDefectSelect(defectData) {
            const select = document.getElementById('defectSelect');
            select.innerHTML = '<option value="">항목 선택</option>';
            defectData.slice(0, 15).forEach(d => {
                select.innerHTML += `<option value="${d[0]}">${d[0]}</option>`;
            });
        }

        function updateDefectMonthly() {
            const defect = document.getElementById('defectSelect').value;
            const selectedPurpose = document.getElementById('defectPurposeFilter').value;
            const ctx = document.getElementById('defectMonthlyChart').getContext('2d');
            if (charts.defectMonthly) charts.defectMonthly.destroy();

            // 목적 필터에 따른 월별 데이터 선택
            let monthSource = currentData.by_defect_month;
            let compareMonthSource = compareData?.by_defect_month;

            if (selectedPurpose) {
                monthSource = currentData.by_defect_purpose_month?.[selectedPurpose] || {};
                compareMonthSource = compareData?.by_defect_purpose_month?.[selectedPurpose] || {};
            }

            if (!defect || !monthSource[defect]) {
                return;
            }

            const labels = []; for (let i = 1; i <= 12; i++) labels.push(i + '월');
            const monthData = Object.fromEntries(monthSource[defect] || []);
            const values = labels.map((_, i) => monthData[i+1] || 0);

            const datasets = [{
                label: currentData.year + '년',
                data: values,
                borderColor: '#e74c3c',
                backgroundColor: 'rgba(231, 76, 60, 0.1)',
                fill: true,
                tension: 0.4
            }];

            // 전년도 비교 데이터 추가
            if (compareData && compareMonthSource && compareMonthSource[defect]) {
                const compareMonthData = Object.fromEntries(compareMonthSource[defect] || []);
                const compareValues = labels.map((_, i) => compareMonthData[i+1] || 0);
                datasets.push({
                    label: compareData.year + '년',
                    data: compareValues,
                    borderColor: '#9b59b6',
                    backgroundColor: 'rgba(155, 89, 182, 0.1)',
                    fill: true,
                    tension: 0.4
                });
            }

            charts.defectMonthly = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: true } } }
            });
        }

        // 지역별 함수들
        function initRegionChart() {
            if (!currentData.by_region || currentData.by_region.length === 0) {
                // 지역 데이터가 없으면 안내 메시지 표시
                const ctx = document.getElementById('regionChart').getContext('2d');
                if (charts.region) charts.region.destroy();
                ctx.font = '14px Malgun Gothic';
                ctx.fillStyle = '#999';
                ctx.textAlign = 'center';
                ctx.fillText('지역 데이터가 없습니다. (주소 컬럼 확인 필요)', ctx.canvas.width / 2, ctx.canvas.height / 2);
                return;
            }

            const ctx = document.getElementById('regionChart').getContext('2d');
            if (charts.region) charts.region.destroy();

            const top15 = currentData.by_region.slice(0, 15);
            charts.region = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: top15.map(d => d[0]),
                    datasets: [{ label: '매출', data: top15.map(d => d[1].sales), backgroundColor: 'rgba(52, 152, 219, 0.7)' }]
                },
                options: { responsive: true, plugins: { legend: { display: false } }, scales: { y: { ticks: { callback: v => formatCurrency(v) } } } }
            });
        }

        function updateRegionTables() {
            if (!currentData.by_region) return;

            // 연도 라벨 업데이트
            const yearLabel = document.getElementById('regionYearLabel');
            const currLabel = currentData.dateLabel || currentData.year + '년';
            if (compareData) {
                const compLabel = compareData.dateLabel || compareData.year + '년';
                yearLabel.textContent = `📅 ${currLabel} vs ${compLabel}`;
            } else {
                yearLabel.textContent = `📅 ${currLabel}`;
            }

            // 담당자 필터 확인
            const selectedManager = document.getElementById('regionManagerFilter').value;
            let regionData = currentData.by_region;
            let compareRegionData = compareData ? compareData.by_region : null;

            // 담당자가 선택된 경우 해당 담당자의 지역 데이터만 표시
            if (selectedManager && currentData.manager_regions && currentData.manager_regions[selectedManager]) {
                const managerRegions = currentData.manager_regions[selectedManager];
                regionData = managerRegions.map(r => [r.region, {sales: r.sales, count: r.count}]);
                // 비교 데이터도 담당자 필터 적용
                if (compareData && compareData.manager_regions && compareData.manager_regions[selectedManager]) {
                    const compareManagerRegions = compareData.manager_regions[selectedManager];
                    compareRegionData = compareManagerRegions.map(r => [r.region, {sales: r.sales, count: r.count}]);
                } else {
                    compareRegionData = null;
                }
            }

            const thead = document.querySelector('#regionTable thead');
            const tbody = document.querySelector('#regionTable tbody');

            // 비교 모드일 때 테이블 헤더 및 데이터 변경
            if (compareData && compareRegionData) {
                thead.innerHTML = `<tr><th>순위</th><th style="white-space:nowrap">지역</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                const compareMap = Object.fromEntries(compareRegionData);

                tbody.innerHTML = regionData.map((d, i) => {
                    const compData = compareMap[d[0]] || {sales: 0, count: 0};
                    const diff = formatDiff(d[1].sales, compData.sales);
                    const diffRate = compData.sales > 0 ? ((diff.diff / compData.sales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    const diffClass = diff.diff >= 0 ? 'positive' : 'negative';
                    const diffText = diff.text ? `<span class="${diffClass}">${diff.text} (${diff.diff >= 0 ? '+' : ''}${diffRate}%)</span>` : '-';
                    const countDiff = d[1].count - compData.count;
                    const countDiffRate = compData.count > 0 ? ((countDiff / compData.count) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td style="white-space:nowrap">${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compData.sales)}</td><td>${diffText}</td><td>${d[1].count.toLocaleString()}</td><td>${compData.count.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('') || '<tr><td colspan="9">지역 데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th style="white-space:nowrap">지역</th><th>매출액</th><th>건수</th><th>평균단가</th></tr>`;
                tbody.innerHTML = regionData.map((d, i) => {
                    const avg = d[1].count > 0 ? d[1].sales / d[1].count : 0;
                    return `<tr><td>${i+1}</td><td style="white-space:nowrap">${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${d[1].count}</td><td>${formatCurrency(avg)}</td></tr>`;
                }).join('') || '<tr><td colspan="5">지역 데이터 없음</td></tr>';
            }

            // 차트 초기화 및 업데이트
            if (!charts.region) {
                initRegionChart();
            }
            updateRegionChart(regionData, compareRegionData);
        }

        function updateRegionChart(regionData, compareRegionData) {
            const top15 = regionData.slice(0, 15);
            if (!charts.region) return;

            charts.region.data.labels = top15.map(d => d[0]);

            if (compareData && compareRegionData) {
                const compareMap = Object.fromEntries(compareRegionData);
                charts.region.data.datasets = [
                    { label: currentData.year + '년', data: top15.map(d => d[1].sales), backgroundColor: 'rgba(102, 126, 234, 0.8)' },
                    { label: compareData.year + '년', data: top15.map(d => (compareMap[d[0]]?.sales || 0)), backgroundColor: 'rgba(118, 75, 162, 0.6)' }
                ];
                charts.region.options.plugins.legend = { display: true };
            } else {
                charts.region.data.datasets = [
                    { label: '매출액', data: top15.map(d => d[1].sales), backgroundColor: 'rgba(102, 126, 234, 0.8)' }
                ];
                charts.region.options.plugins.legend = { display: false };
            }
            charts.region.update();
        }

        function updateRegionSelects() {
            if (!currentData.by_region) return;

            // 지역 선택 드롭다운
            const regionSelect = document.getElementById('regionSelect');
            regionSelect.innerHTML = '<option value="">지역 선택</option>';
            currentData.by_region.forEach(d => {
                regionSelect.innerHTML += `<option value="${d[0]}">${d[0]}</option>`;
            });

            // 담당자 선택 드롭다운 (담당자별 지역 분포용)
            const managerRegionSelect = document.getElementById('managerRegionSelect');
            managerRegionSelect.innerHTML = '<option value="">담당자 선택</option>';
            currentData.by_manager.forEach(m => {
                managerRegionSelect.innerHTML += `<option value="${m[0]}">${m[0]}</option>`;
            });

            // 지역별 탭 담당자 필터
            const regionManagerFilter = document.getElementById('regionManagerFilter');
            const currentFilter = regionManagerFilter.value;
            regionManagerFilter.innerHTML = '<option value="">전체 담당자</option>';
            currentData.by_manager.forEach(m => {
                regionManagerFilter.innerHTML += `<option value="${m[0]}">${m[0]}</option>`;
            });
            if (currentFilter) regionManagerFilter.value = currentFilter;
        }

        function updateRegionManagers() {
            const region = document.getElementById('regionSelect').value;
            const thead = document.querySelector('#regionManagerTable thead');
            const tbody = document.querySelector('#regionManagerTable tbody');

            if (!region || !currentData.region_top_managers || !currentData.region_top_managers[region]) {
                thead.innerHTML = '<tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr>';
                tbody.innerHTML = '<tr><td colspan="5">지역을 선택해주세요</td></tr>';
                return;
            }

            const managers = currentData.region_top_managers[region];
            const totalSales = managers.reduce((sum, m) => sum + m.sales, 0);

            if (compareData && compareData.region_top_managers && compareData.region_top_managers[region]) {
                const compareManagers = compareData.region_top_managers[region];
                const compareMap = {};
                compareManagers.forEach(m => { compareMap[m.name] = m; });

                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                tbody.innerHTML = managers.map((m, i) => {
                    const compData = compareMap[m.name] || {sales: 0, count: 0};
                    const diff = formatDiff(m.sales, compData.sales);
                    const diffRate = compData.sales > 0 ? ((diff.diff / compData.sales) * 100).toFixed(1) : (m.sales > 0 ? 100 : 0);
                    const diffClass = diff.diff >= 0 ? 'positive' : 'negative';
                    const diffText = diff.text ? `<span class="${diffClass}">${diff.text} (${diff.diff >= 0 ? '+' : ''}${diffRate}%)</span>` : '-';
                    const countDiff = m.count - compData.count;
                    const countDiffRate = compData.count > 0 ? ((countDiff / compData.count) * 100).toFixed(1) : (m.count > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td>${m.name}</td><td>${formatCurrency(m.sales)}</td><td>${formatCurrency(compData.sales)}</td><td>${diffText}</td><td>${m.count.toLocaleString()}</td><td>${compData.count.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('') || '<tr><td colspan="9">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = '<tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr>';
                tbody.innerHTML = managers.map((m, i) =>
                    `<tr><td>${i+1}</td><td>${m.name}</td><td>${formatCurrency(m.sales)}</td><td>${m.count}</td><td>${(m.sales / totalSales * 100).toFixed(1)}%</td></tr>`
                ).join('') || '<tr><td colspan="5">데이터 없음</td></tr>';
            }
        }

        function updateManagerRegions() {
            const manager = document.getElementById('managerRegionSelect').value;
            const thead = document.querySelector('#managerRegionTable thead');
            const tbody = document.querySelector('#managerRegionTable tbody');

            if (!manager || !currentData.manager_regions || !currentData.manager_regions[manager]) {
                thead.innerHTML = '<tr><th>순위</th><th>지역</th><th>매출액</th><th>건수</th><th>비중</th></tr>';
                tbody.innerHTML = '<tr><td colspan="5">담당자를 선택해주세요</td></tr>';
                return;
            }

            const regions = currentData.manager_regions[manager];
            const totalSales = regions.reduce((sum, r) => sum + r.sales, 0);

            if (compareData && compareData.manager_regions && compareData.manager_regions[manager]) {
                const compareRegions = compareData.manager_regions[manager];
                const compareMap = {};
                compareRegions.forEach(r => { compareMap[r.region] = r; });

                thead.innerHTML = `<tr><th>순위</th><th>지역</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                tbody.innerHTML = regions.map((r, i) => {
                    const compData = compareMap[r.region] || {sales: 0, count: 0};
                    const diff = formatDiff(r.sales, compData.sales);
                    const diffRate = compData.sales > 0 ? ((diff.diff / compData.sales) * 100).toFixed(1) : (r.sales > 0 ? 100 : 0);
                    const diffClass = diff.diff >= 0 ? 'positive' : 'negative';
                    const diffText = diff.text ? `<span class="${diffClass}">${diff.text} (${diff.diff >= 0 ? '+' : ''}${diffRate}%)</span>` : '-';
                    const countDiff = r.count - compData.count;
                    const countDiffRate = compData.count > 0 ? ((countDiff / compData.count) * 100).toFixed(1) : (r.count > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td>${r.region}</td><td>${formatCurrency(r.sales)}</td><td>${formatCurrency(compData.sales)}</td><td>${diffText}</td><td>${r.count.toLocaleString()}</td><td>${compData.count.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('') || '<tr><td colspan="9">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = '<tr><th>순위</th><th>지역</th><th>매출액</th><th>건수</th><th>비중</th></tr>';
                tbody.innerHTML = regions.map((r, i) =>
                    `<tr><td>${i+1}</td><td>${r.region}</td><td>${formatCurrency(r.sales)}</td><td>${r.count}</td><td>${(r.sales / totalSales * 100).toFixed(1)}%</td></tr>`
                ).join('') || '<tr><td colspan="5">데이터 없음</td></tr>';
            }
        }

        // 목적별 탭 함수들
        function updatePurposeCheckboxes() {
            const container = document.getElementById('purposeCheckboxes');
            container.innerHTML = '';

            if (!currentData.purposes) return;

            currentData.purposes.forEach(p => {
                if (!p) return;
                const label = document.createElement('label');
                label.style.cssText = 'display: flex; align-items: center; gap: 5px; background: white; padding: 5px 10px; border-radius: 5px; cursor: pointer; border: 1px solid #ddd;';
                label.innerHTML = `<input type="checkbox" value="${p}" onchange="updatePurposeTab()" checked> ${p}`;
                container.appendChild(label);
            });

            // 필터 드롭다운 업데이트
            updatePurposeFilters();
        }

        function updatePurposeFilters() {
            // 담당자 필터
            const managerFilter = document.getElementById('purposeManagerFilter');
            const currentManager = managerFilter.value;
            managerFilter.innerHTML = '<option value="">전체</option>';
            if (currentData.by_manager) {
                currentData.by_manager.forEach(m => {
                    managerFilter.innerHTML += `<option value="${m[0]}">${m[0]}</option>`;
                });
            }
            if (currentManager) managerFilter.value = currentManager;

            // 지역 필터
            const regionFilter = document.getElementById('purposeRegionFilter');
            const currentRegion = regionFilter.value;
            regionFilter.innerHTML = '<option value="">전체</option>';
            if (currentData.by_region) {
                // 시/도 단위로 그룹화
                const sidos = [...new Set(currentData.by_region.map(r => r[1].sido))].filter(s => s);
                sidos.forEach(sido => {
                    regionFilter.innerHTML += `<option value="${sido}">${sido}</option>`;
                });
            }
            if (currentRegion) regionFilter.value = currentRegion;

            // 월별 추이 목적 선택 드롭다운
            const monthlySelect = document.getElementById('purposeMonthlySelect');
            monthlySelect.innerHTML = '<option value="">목적 선택</option>';
            if (currentData.purposes) {
                currentData.purposes.forEach(p => {
                    if (p) monthlySelect.innerHTML += `<option value="${p}">${p}</option>`;
                });
            }
        }

        function selectAllPurposes() {
            document.querySelectorAll('#purposeCheckboxes input[type="checkbox"]').forEach(cb => cb.checked = true);
            updatePurposeTab();
        }

        function clearAllPurposes() {
            document.querySelectorAll('#purposeCheckboxes input[type="checkbox"]').forEach(cb => cb.checked = false);
            updatePurposeTab();
        }

        function getSelectedPurposes() {
            const checkboxes = document.querySelectorAll('#purposeCheckboxes input[type="checkbox"]:checked');
            return Array.from(checkboxes).map(cb => cb.value);
        }

        function updatePurposeTab() {
            // 연도 라벨 업데이트
            const yearLabel = document.getElementById('purposeYearLabel');
            const currLabel = currentData.dateLabel || currentData.year + '년';
            if (compareData) {
                const compLabel = compareData.dateLabel || compareData.year + '년';
                yearLabel.textContent = `📅 ${currLabel} vs ${compLabel}`;
            } else {
                yearLabel.textContent = `📅 ${currLabel}`;
            }

            const selectedPurposes = getSelectedPurposes();
            const topN = parseInt(document.getElementById('purposeTopN').value) || 15;
            const selectedManager = document.getElementById('purposeManagerFilter').value;
            const selectedRegion = document.getElementById('purposeRegionFilter').value;
            document.getElementById('purposeChartTopN').textContent = topN;

            if (selectedPurposes.length === 0) {
                document.querySelector('#purposeTable tbody').innerHTML = '<tr><td colspan="7">검사목적을 선택해주세요</td></tr>';
                document.querySelector('#purposeManagerTable tbody').innerHTML = '<tr><td colspan="7">검사목적을 선택해주세요</td></tr>';
                document.querySelector('#purposeRegionTable tbody').innerHTML = '<tr><td colspan="7">검사목적을 선택해주세요</td></tr>';
                if (charts.purpose) charts.purpose.destroy();
                return;
            }

            // 담당자/지역 필터에 따른 목적별 데이터 계산
            let purposeData = {};
            let comparePurposeData = {};

            if (selectedManager && currentData.purpose_managers) {
                // 특정 담당자의 목적별 데이터만 집계
                selectedPurposes.forEach(purpose => {
                    if (currentData.purpose_managers[purpose]) {
                        const managerInfo = currentData.purpose_managers[purpose].find(m => m.name === selectedManager);
                        if (managerInfo) {
                            purposeData[purpose] = { sales: managerInfo.sales, count: managerInfo.count };
                        }
                    }
                });
                // 비교 데이터
                if (compareData && compareData.purpose_managers) {
                    selectedPurposes.forEach(purpose => {
                        if (compareData.purpose_managers[purpose]) {
                            const managerInfo = compareData.purpose_managers[purpose].find(m => m.name === selectedManager);
                            if (managerInfo) {
                                comparePurposeData[purpose] = { sales: managerInfo.sales, count: managerInfo.count };
                            }
                        }
                    });
                }
            } else if (selectedRegion && currentData.purpose_regions) {
                // 특정 지역의 목적별 데이터만 집계
                selectedPurposes.forEach(purpose => {
                    if (currentData.purpose_regions[purpose]) {
                        let totalSales = 0, totalCount = 0;
                        currentData.purpose_regions[purpose].forEach(r => {
                            if (r.region.startsWith(selectedRegion)) {
                                totalSales += r.sales;
                                totalCount += r.count;
                            }
                        });
                        if (totalSales > 0) {
                            purposeData[purpose] = { sales: totalSales, count: totalCount };
                        }
                    }
                });
                // 비교 데이터
                if (compareData && compareData.purpose_regions) {
                    selectedPurposes.forEach(purpose => {
                        if (compareData.purpose_regions[purpose]) {
                            let totalSales = 0, totalCount = 0;
                            compareData.purpose_regions[purpose].forEach(r => {
                                if (r.region.startsWith(selectedRegion)) {
                                    totalSales += r.sales;
                                    totalCount += r.count;
                                }
                            });
                            if (totalSales > 0) {
                                comparePurposeData[purpose] = { sales: totalSales, count: totalCount };
                            }
                        }
                    });
                }
            } else {
                // 전체 데이터 사용
                currentData.by_purpose.forEach(p => {
                    if (selectedPurposes.includes(p[0])) {
                        purposeData[p[0]] = p[1];
                    }
                });
                if (compareData && compareData.by_purpose) {
                    compareData.by_purpose.forEach(p => {
                        if (selectedPurposes.includes(p[0])) {
                            comparePurposeData[p[0]] = p[1];
                        }
                    });
                }
            }

            // 정렬 및 상위 N개 추출
            const sortedPurposes = Object.entries(purposeData).sort((a, b) => b[1].sales - a[1].sales);
            const topPurposes = sortedPurposes.slice(0, topN);
            const totalSales = sortedPurposes.reduce((sum, p) => sum + p[1].sales, 0);

            // 현재 적용된 필터 라벨 표시
            let filterInfo = [];
            if (selectedManager) filterInfo.push(`담당자: ${selectedManager}`);
            if (selectedRegion) filterInfo.push(`지역: ${selectedRegion}`);
            const filterLabel = filterInfo.length > 0 ? `[${filterInfo.join(', ')}]` : '';
            document.getElementById('purposeChartFilterLabel').textContent = filterLabel;
            document.getElementById('purposeTableFilterLabel').textContent = filterLabel;
            document.getElementById('purposeManagerFilterLabel').textContent = selectedManager ? `[${selectedManager}]` : '';
            document.getElementById('purposeRegionFilterLabel').textContent = selectedRegion ? `[${selectedRegion}]` : '';

            // 목적별 차트 (막대 차트, 연도 비교 지원)
            const ctx = document.getElementById('purposeChart').getContext('2d');
            if (charts.purpose) charts.purpose.destroy();

            const datasets = [{
                label: currLabel,
                data: topPurposes.map(p => p[1].sales),
                backgroundColor: 'rgba(102, 126, 234, 0.8)'
            }];

            if (compareData && Object.keys(comparePurposeData).length > 0) {
                datasets.push({
                    label: compareData.dateLabel || compareData.year + '년',
                    data: topPurposes.map(p => comparePurposeData[p[0]]?.sales || 0),
                    backgroundColor: 'rgba(118, 75, 162, 0.6)'
                });
            }

            charts.purpose = new Chart(ctx, {
                type: 'bar',
                data: { labels: topPurposes.map(p => p[0]), datasets },
                options: {
                    responsive: true,
                    plugins: { legend: { display: compareData ? true : false } },
                    scales: { y: { ticks: { callback: v => formatCurrency(v) } } }
                }
            });

            // 목적별 테이블 (연도 비교 지원)
            const thead = document.getElementById('purposeTableHead');
            const tbody = document.querySelector('#purposeTable tbody');

            if (compareData && Object.keys(comparePurposeData).length > 0) {
                thead.innerHTML = `<tr><th>순위</th><th>검사목적</th><th>${currLabel}</th><th>${compareData.dateLabel || compareData.year + '년'}</th><th>증감</th><th>${currLabel} 건수</th><th>${compareData.dateLabel || compareData.year + '년'} 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedPurposes.map((p, i) => {
                    const compSales = comparePurposeData[p[0]]?.sales || 0;
                    const compCount = comparePurposeData[p[0]]?.count || 0;
                    const diff = p[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (p[1].sales > 0 ? 100 : 0);
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    const diffText = `<span class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</span>`;
                    const countDiff = p[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (p[1].count > 0 ? 100 : 0);
                    const ratio = totalSales > 0 ? (p[1].sales / totalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${p[0]}</td><td>${formatCurrency(p[1].sales)}</td><td>${formatCurrency(compSales)}</td><td>${diffText}</td><td>${p[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="10">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>검사목적</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedPurposes.map((p, i) => {
                    const avg = p[1].count > 0 ? p[1].sales / p[1].count : 0;
                    const ratio = totalSales > 0 ? (p[1].sales / totalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${p[0]}</td><td>${formatCurrency(p[1].sales)}</td><td>${p[1].count}</td><td>${formatCurrency(avg)}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            }

            // 목적별 담당자 테이블
            updatePurposeManagerTable(selectedPurposes, topN, selectedManager, selectedRegion);

            // 목적별 지역 테이블
            updatePurposeRegionTable(selectedPurposes, topN, selectedManager, selectedRegion);
        }

        function updatePurposeManagerTable(selectedPurposes, topN, selectedManager, selectedRegion) {
            const thead = document.getElementById('purposeManagerTableHead');
            const tbody = document.querySelector('#purposeManagerTable tbody');

            if (!currentData.purpose_managers) {
                tbody.innerHTML = '<tr><td colspan="6">담당자 데이터 없음</td></tr>';
                return;
            }

            // 담당자별 데이터 집계
            const managerData = {};
            const compareManagerData = {};

            selectedPurposes.forEach(purpose => {
                if (currentData.purpose_managers[purpose]) {
                    currentData.purpose_managers[purpose].forEach(m => {
                        // 담당자 필터가 있으면 해당 담당자만
                        if (selectedManager && m.name !== selectedManager) return;
                        if (!managerData[m.name]) managerData[m.name] = { sales: 0, count: 0 };
                        managerData[m.name].sales += m.sales;
                        managerData[m.name].count += m.count;
                    });
                }
                if (compareData && compareData.purpose_managers && compareData.purpose_managers[purpose]) {
                    compareData.purpose_managers[purpose].forEach(m => {
                        if (selectedManager && m.name !== selectedManager) return;
                        if (!compareManagerData[m.name]) compareManagerData[m.name] = { sales: 0, count: 0 };
                        compareManagerData[m.name].sales += m.sales;
                        compareManagerData[m.name].count += m.count;
                    });
                }
            });

            const sortedManagers = Object.entries(managerData).sort((a, b) => b[1].sales - a[1].sales).slice(0, topN);
            const managerTotalSales = sortedManagers.reduce((sum, m) => sum + m[1].sales, 0);

            if (compareData && Object.keys(compareManagerData).length > 0) {
                const compLabel = compareData.dateLabel || compareData.year + '년';
                const currLabel = currentData.dateLabel || currentData.year + '년';
                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>${currLabel}</th><th>${compLabel}</th><th>증감</th><th>${currLabel} 건수</th><th>${compLabel} 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedManagers.map(([name, data], i) => {
                    const compSales = compareManagerData[name]?.sales || 0;
                    const compCount = compareManagerData[name]?.count || 0;
                    const diff = data.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (data.sales > 0 ? 100 : 0);
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    const diffText = `<span class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</span>`;
                    const countDiff = data.count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (data.count > 0 ? 100 : 0);
                    const ratio = managerTotalSales > 0 ? (data.sales / managerTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(data.sales)}</td><td>${formatCurrency(compSales)}</td><td>${diffText}</td><td>${data.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="10">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedManagers.map(([name, data], i) => {
                    const avg = data.count > 0 ? data.sales / data.count : 0;
                    const ratio = managerTotalSales > 0 ? (data.sales / managerTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(data.sales)}</td><td>${data.count}</td><td>${formatCurrency(avg)}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            }
        }

        function updatePurposeRegionTable(selectedPurposes, topN, selectedManager, selectedRegion) {
            const thead = document.getElementById('purposeRegionTableHead');
            const tbody = document.querySelector('#purposeRegionTable tbody');

            if (!currentData.purpose_regions) {
                tbody.innerHTML = '<tr><td colspan="6">지역 데이터 없음</td></tr>';
                return;
            }

            // 지역별 데이터 집계
            const regionData = {};
            const compareRegionData = {};

            selectedPurposes.forEach(purpose => {
                if (currentData.purpose_regions[purpose]) {
                    currentData.purpose_regions[purpose].forEach(r => {
                        // 지역 필터가 있으면 해당 지역만
                        if (selectedRegion && !r.region.startsWith(selectedRegion)) return;
                        if (!regionData[r.region]) regionData[r.region] = { sales: 0, count: 0 };
                        regionData[r.region].sales += r.sales;
                        regionData[r.region].count += r.count;
                    });
                }
                if (compareData && compareData.purpose_regions && compareData.purpose_regions[purpose]) {
                    compareData.purpose_regions[purpose].forEach(r => {
                        if (selectedRegion && !r.region.startsWith(selectedRegion)) return;
                        if (!compareRegionData[r.region]) compareRegionData[r.region] = { sales: 0, count: 0 };
                        compareRegionData[r.region].sales += r.sales;
                        compareRegionData[r.region].count += r.count;
                    });
                }
            });

            const sortedRegions = Object.entries(regionData).sort((a, b) => b[1].sales - a[1].sales).slice(0, topN);
            const regionTotalSales = sortedRegions.reduce((sum, r) => sum + r[1].sales, 0);

            if (compareData && Object.keys(compareRegionData).length > 0) {
                const compLabel = compareData.dateLabel || compareData.year + '년';
                const currLabel = currentData.dateLabel || currentData.year + '년';
                thead.innerHTML = `<tr><th>순위</th><th>지역</th><th>${currLabel}</th><th>${compLabel}</th><th>증감</th><th>${currLabel} 건수</th><th>${compLabel} 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedRegions.map(([region, data], i) => {
                    const compSales = compareRegionData[region]?.sales || 0;
                    const compCount = compareRegionData[region]?.count || 0;
                    const diff = data.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (data.sales > 0 ? 100 : 0);
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    const diffText = `<span class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</span>`;
                    const countDiff = data.count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (data.count > 0 ? 100 : 0);
                    const ratio = regionTotalSales > 0 ? (data.sales / regionTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${region}</td><td>${formatCurrency(data.sales)}</td><td>${formatCurrency(compSales)}</td><td>${diffText}</td><td>${data.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="10">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>지역</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedRegions.map(([region, data], i) => {
                    const avg = data.count > 0 ? data.sales / data.count : 0;
                    const ratio = regionTotalSales > 0 ? (data.sales / regionTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${region}</td><td>${formatCurrency(data.sales)}</td><td>${data.count}</td><td>${formatCurrency(avg)}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            }
        }

        function updatePurposeMonthlyChart() {
            const purpose = document.getElementById('purposeMonthlySelect').value;
            const selectedManager = document.getElementById('purposeManagerFilter').value;
            const ctx = document.getElementById('purposeMonthlyChart').getContext('2d');
            if (charts.purposeMonthly) charts.purposeMonthly.destroy();

            // 필터 라벨 업데이트
            document.getElementById('purposeMonthlyFilterLabel').textContent = selectedManager ? `[${selectedManager}]` : '';

            if (!purpose) {
                ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
                return;
            }

            // 월별 라벨
            const labels = [];
            for (let i = 1; i <= 12; i++) labels.push(i + '월');

            // 현재 데이터에서 해당 목적의 월별 매출 가져오기
            const purposeMonthData = currentData.by_purpose_month && currentData.by_purpose_month[purpose]
                ? currentData.by_purpose_month[purpose] : {};

            // 담당자 필터가 있으면 해당 담당자의 데이터만 사용
            function getMonthlyValue(monthData, month) {
                if (!monthData || !monthData[month]) return 0;
                if (selectedManager && monthData[month].by_manager) {
                    return monthData[month].by_manager[selectedManager]?.sales || 0;
                }
                return monthData[month].sales || 0;
            }

            let chartLabel = (currentData.dateLabel || currentData.year + '년') + ' - ' + purpose;
            if (selectedManager) chartLabel += ` (${selectedManager})`;

            const datasets = [{
                label: chartLabel,
                data: labels.map((_, i) => getMonthlyValue(purposeMonthData, i + 1)),
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                fill: true,
                tension: 0.4
            }];

            // 비교 데이터
            if (compareData && compareData.by_purpose_month && compareData.by_purpose_month[purpose]) {
                const comparePurposeMonthData = compareData.by_purpose_month[purpose];

                let compareChartLabel = (compareData.dateLabel || compareData.year + '년') + ' - ' + purpose;
                if (selectedManager) compareChartLabel += ` (${selectedManager})`;

                datasets.push({
                    label: compareChartLabel,
                    data: labels.map((_, i) => getMonthlyValue(comparePurposeMonthData, i + 1)),
                    borderColor: '#764ba2',
                    backgroundColor: 'rgba(118, 75, 162, 0.1)',
                    fill: true,
                    tension: 0.4
                });
            }

            charts.purposeMonthly = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } },
                    scales: { y: { ticks: { callback: v => formatCurrency(v) } } }
                }
            });
        }

        // 검체유형 탭 함수들
        function updateSampleTypeFilters() {
            // 담당자 필터
            const managerFilter = document.getElementById('sampleTypeManagerFilter');
            const currentManager = managerFilter.value;
            managerFilter.innerHTML = '<option value="">전체</option>';
            if (currentData.by_manager) {
                currentData.by_manager.forEach(m => {
                    managerFilter.innerHTML += `<option value="${m[0]}">${m[0]}</option>`;
                });
            }
            if (currentManager) managerFilter.value = currentManager;

            // 검사목적 필터
            const purposeFilter = document.getElementById('sampleTypePurposeFilter');
            const currentPurpose = purposeFilter.value;
            purposeFilter.innerHTML = '<option value="">전체</option>';
            if (currentData.purposes) {
                currentData.purposes.forEach(p => {
                    if (p) purposeFilter.innerHTML += `<option value="${p}">${p}</option>`;
                });
            }
            if (currentPurpose) purposeFilter.value = currentPurpose;

            // 월별 검체유형 선택
            const monthlySelect = document.getElementById('sampleTypeMonthlySelect');
            const currentValue = monthlySelect.value;
            monthlySelect.innerHTML = '<option value="">검체유형 선택</option>';
            if (currentData.sample_types) {
                currentData.sample_types.forEach(st => {
                    if (st) monthlySelect.innerHTML += `<option value="${st}">${st}</option>`;
                });
            }
            if (currentValue && currentData.sample_types && currentData.sample_types.includes(currentValue)) {
                monthlySelect.value = currentValue;
            }
        }

        function updateSampleTypeTab() {
            // 연도 라벨 업데이트
            const yearLabel = document.getElementById('sampleTypeYearLabel');
            const currLabel = currentData.dateLabel || currentData.year + '년';
            if (compareData) {
                const compLabel = compareData.dateLabel || compareData.year + '년';
                yearLabel.textContent = `📅 ${currLabel} vs ${compLabel}`;
            } else {
                yearLabel.textContent = `📅 ${currLabel}`;
            }

            const selectedManager = document.getElementById('sampleTypeManagerFilter').value;
            const selectedPurpose = document.getElementById('sampleTypePurposeFilter').value;
            const topN = parseInt(document.getElementById('sampleTypeTopN').value) || 15;

            // TOP N 표시 업데이트
            document.getElementById('sampleTypeChartTopN').textContent = topN;

            // 필터 라벨 업데이트
            let filterLabel = '';
            if (selectedManager) filterLabel += `[${selectedManager}]`;
            if (selectedPurpose) filterLabel += `[${selectedPurpose}]`;
            document.getElementById('sampleTypeChartFilterLabel').textContent = filterLabel;
            document.getElementById('sampleTypeTableFilterLabel').textContent = filterLabel;
            document.getElementById('sampleTypeManagerTableLabel').textContent = selectedManager ? `[${selectedManager}]` : '';
            document.getElementById('sampleTypePurposeTableLabel').textContent = selectedPurpose ? `[${selectedPurpose}]` : '';

            // 담당자/목적 필터에 따른 검체유형별 데이터 계산
            let sampleTypeData = {};
            let compareSampleTypeData = {};

            if (selectedManager && selectedPurpose && currentData.sample_type_managers) {
                // 담당자 + 목적 둘 다 필터: 해당 담당자의 해당 목적 매출만
                Object.keys(currentData.sample_type_managers).forEach(st => {
                    const managerInfo = currentData.sample_type_managers[st].find(m => m.name === selectedManager);
                    if (managerInfo && managerInfo.by_purpose && managerInfo.by_purpose[selectedPurpose]) {
                        const purposeData = managerInfo.by_purpose[selectedPurpose];
                        sampleTypeData[st] = { sales: purposeData.sales, count: purposeData.count };
                    }
                });
                if (compareData && compareData.sample_type_managers) {
                    Object.keys(compareData.sample_type_managers).forEach(st => {
                        const managerInfo = compareData.sample_type_managers[st].find(m => m.name === selectedManager);
                        if (managerInfo && managerInfo.by_purpose && managerInfo.by_purpose[selectedPurpose]) {
                            const purposeData = managerInfo.by_purpose[selectedPurpose];
                            compareSampleTypeData[st] = { sales: purposeData.sales, count: purposeData.count };
                        }
                    });
                }
            } else if (selectedManager && currentData.sample_type_managers) {
                // 특정 담당자의 검체유형별 데이터만 집계
                Object.keys(currentData.sample_type_managers).forEach(st => {
                    const managerInfo = currentData.sample_type_managers[st].find(m => m.name === selectedManager);
                    if (managerInfo) {
                        sampleTypeData[st] = { sales: managerInfo.sales, count: managerInfo.count };
                    }
                });
                if (compareData && compareData.sample_type_managers) {
                    Object.keys(compareData.sample_type_managers).forEach(st => {
                        const managerInfo = compareData.sample_type_managers[st].find(m => m.name === selectedManager);
                        if (managerInfo) {
                            compareSampleTypeData[st] = { sales: managerInfo.sales, count: managerInfo.count };
                        }
                    });
                }
            } else if (selectedPurpose && currentData.sample_type_purposes) {
                // 특정 목적의 검체유형별 데이터만 집계
                Object.keys(currentData.sample_type_purposes).forEach(st => {
                    const purposeInfo = currentData.sample_type_purposes[st].find(p => p.name === selectedPurpose);
                    if (purposeInfo) {
                        sampleTypeData[st] = { sales: purposeInfo.sales, count: purposeInfo.count };
                    }
                });
                if (compareData && compareData.sample_type_purposes) {
                    Object.keys(compareData.sample_type_purposes).forEach(st => {
                        const purposeInfo = compareData.sample_type_purposes[st].find(p => p.name === selectedPurpose);
                        if (purposeInfo) {
                            compareSampleTypeData[st] = { sales: purposeInfo.sales, count: purposeInfo.count };
                        }
                    });
                }
            } else {
                // 전체 데이터 사용
                if (currentData.by_sample_type) {
                    currentData.by_sample_type.forEach(([st, data]) => {
                        sampleTypeData[st] = data;
                    });
                }
                if (compareData && compareData.by_sample_type) {
                    compareData.by_sample_type.forEach(([st, data]) => {
                        compareSampleTypeData[st] = data;
                    });
                }
            }

            // 정렬 및 TOP N 적용
            const sortedData = Object.entries(sampleTypeData)
                .sort((a, b) => b[1].sales - a[1].sales)
                .slice(0, topN);

            // 전체 합계 계산 (비중 계산용)
            const totalSales = sortedData.reduce((sum, [_, d]) => sum + d.sales, 0);

            // 검체유형별 차트 (막대 차트, 연도 비교 지원)
            const ctx = document.getElementById('sampleTypeChart').getContext('2d');
            if (charts.sampleType) charts.sampleType.destroy();

            const chartLabels = sortedData.map(([st, _]) => st);
            const chartDatasets = [{
                label: currentData.dateLabel || currentData.year + '년',
                data: sortedData.map(([_, d]) => d.sales),
                backgroundColor: 'rgba(52, 152, 219, 0.7)'
            }];

            if (compareData && Object.keys(compareSampleTypeData).length > 0) {
                chartDatasets.push({
                    label: compareData.dateLabel || compareData.year + '년',
                    data: sortedData.map(([st, _]) => compareSampleTypeData[st]?.sales || 0),
                    backgroundColor: 'rgba(155, 89, 182, 0.6)'
                });
            }

            charts.sampleType = new Chart(ctx, {
                type: 'bar',
                data: { labels: chartLabels, datasets: chartDatasets },
                options: {
                    responsive: true,
                    plugins: { legend: { display: compareData ? true : false } },
                    scales: { y: { ticks: { callback: v => formatCurrency(v) } } }
                }
            });

            // 검체유형별 테이블 (연도 비교 지원)
            const thead = document.getElementById('sampleTypeTableHead');
            const tbody = document.querySelector('#sampleTypeTable tbody');

            if (compareData && Object.keys(compareSampleTypeData).length > 0) {
                thead.innerHTML = `<tr><th>순위</th><th>검체유형</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedData.map(([st, d], i) => {
                    const compSales = compareSampleTypeData[st]?.sales || 0;
                    const compCount = compareSampleTypeData[st]?.count || 0;
                    const diff = d.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d.sales > 0 ? 100 : 0);
                    const countDiff = d.count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d.count > 0 ? 100 : 0);
                    const percent = totalSales > 0 ? (d.sales / totalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${st}</td><td>${formatCurrency(d.sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="10">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>검체유형</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedData.map(([st, d], i) => {
                    const avg = d.count > 0 ? d.sales / d.count : 0;
                    const percent = totalSales > 0 ? (d.sales / totalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${st}</td><td>${formatCurrency(d.sales)}</td><td>${d.count}</td><td>${formatCurrency(avg)}</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            }

            // 검체유형별 담당자 테이블
            updateSampleTypeManagerTable(selectedManager, selectedPurpose, topN, totalSales);

            // 검체유형별 목적 테이블
            updateSampleTypePurposeTable(selectedPurpose, topN, totalSales);
        }

        function updateSampleTypeManagerTable(selectedManager, selectedPurpose, topN, totalSales) {
            const thead = document.getElementById('sampleTypeManagerTableHead');
            const tbody = document.querySelector('#sampleTypeManagerTable tbody');

            // 필터 라벨 업데이트
            let filterLabel = '';
            if (selectedManager) filterLabel += `[${selectedManager}]`;
            if (selectedPurpose) filterLabel += `[${selectedPurpose}]`;
            document.getElementById('sampleTypeManagerTableLabel').textContent = filterLabel;

            // 검체유형의 담당자 데이터 집계 (목적 필터 적용)
            let managerData = {};
            if (currentData.sample_type_managers) {
                Object.entries(currentData.sample_type_managers).forEach(([st, managers]) => {
                    managers.forEach(m => {
                        if (!selectedManager || m.name === selectedManager) {
                            // 목적 필터가 있으면 해당 목적의 매출만 집계
                            let sales = 0, count = 0;
                            if (selectedPurpose && m.by_purpose) {
                                const purposeData = m.by_purpose[selectedPurpose];
                                if (purposeData) {
                                    sales = purposeData.sales;
                                    count = purposeData.count;
                                }
                            } else {
                                sales = m.sales;
                                count = m.count;
                            }

                            if (sales > 0) {
                                if (!managerData[m.name]) {
                                    managerData[m.name] = { sales: 0, count: 0 };
                                }
                                managerData[m.name].sales += sales;
                                managerData[m.name].count += count;
                            }
                        }
                    });
                });
            }

            const sortedManagers = Object.entries(managerData)
                .sort((a, b) => b[1].sales - a[1].sales)
                .slice(0, topN);

            const managerTotalSales = sortedManagers.reduce((sum, [_, d]) => sum + d.sales, 0);

            let compareManagerData = {};
            if (compareData && compareData.sample_type_managers) {
                Object.entries(compareData.sample_type_managers).forEach(([st, managers]) => {
                    managers.forEach(m => {
                        if (!selectedManager || m.name === selectedManager) {
                            let sales = 0, count = 0;
                            if (selectedPurpose && m.by_purpose) {
                                const purposeData = m.by_purpose[selectedPurpose];
                                if (purposeData) {
                                    sales = purposeData.sales;
                                    count = purposeData.count;
                                }
                            } else {
                                sales = m.sales;
                                count = m.count;
                            }

                            if (sales > 0) {
                                if (!compareManagerData[m.name]) {
                                    compareManagerData[m.name] = { sales: 0, count: 0 };
                                }
                                compareManagerData[m.name].sales += sales;
                                compareManagerData[m.name].count += count;
                            }
                        }
                    });
                });
            }

            if (compareData && Object.keys(compareManagerData).length > 0) {
                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedManagers.map(([name, d], i) => {
                    const compSales = compareManagerData[name]?.sales || 0;
                    const compCount = compareManagerData[name]?.count || 0;
                    const diff = d.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d.sales > 0 ? 100 : 0);
                    const countDiff = d.count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d.count > 0 ? 100 : 0);
                    const percent = managerTotalSales > 0 ? (d.sales / managerTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(d.sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="10">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedManagers.map(([name, d], i) => {
                    const avg = d.count > 0 ? d.sales / d.count : 0;
                    const percent = managerTotalSales > 0 ? (d.sales / managerTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(d.sales)}</td><td>${d.count}</td><td>${formatCurrency(avg)}</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            }
        }

        function updateSampleTypePurposeTable(selectedPurpose, topN, totalSales) {
            const thead = document.getElementById('sampleTypePurposeTableHead');
            const tbody = document.querySelector('#sampleTypePurposeTable tbody');

            // 모든 검체유형의 목적 데이터 집계
            let purposeData = {};
            if (currentData.sample_type_purposes) {
                Object.values(currentData.sample_type_purposes).forEach(purposes => {
                    purposes.forEach(p => {
                        if (!selectedPurpose || p.name === selectedPurpose) {
                            if (!purposeData[p.name]) {
                                purposeData[p.name] = { sales: 0, count: 0 };
                            }
                            purposeData[p.name].sales += p.sales;
                            purposeData[p.name].count += p.count;
                        }
                    });
                });
            }

            const sortedPurposes = Object.entries(purposeData)
                .sort((a, b) => b[1].sales - a[1].sales)
                .slice(0, topN);

            const purposeTotalSales = sortedPurposes.reduce((sum, [_, d]) => sum + d.sales, 0);

            let comparePurposeData = {};
            if (compareData && compareData.sample_type_purposes) {
                Object.values(compareData.sample_type_purposes).forEach(purposes => {
                    purposes.forEach(p => {
                        if (!selectedPurpose || p.name === selectedPurpose) {
                            if (!comparePurposeData[p.name]) {
                                comparePurposeData[p.name] = { sales: 0, count: 0 };
                            }
                            comparePurposeData[p.name].sales += p.sales;
                            comparePurposeData[p.name].count += p.count;
                        }
                    });
                });
            }

            if (compareData && Object.keys(comparePurposeData).length > 0) {
                thead.innerHTML = `<tr><th>순위</th><th>검사목적</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedPurposes.map(([name, d], i) => {
                    const compSales = comparePurposeData[name]?.sales || 0;
                    const compCount = comparePurposeData[name]?.count || 0;
                    const diff = d.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d.sales > 0 ? 100 : 0);
                    const countDiff = d.count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d.count > 0 ? 100 : 0);
                    const percent = purposeTotalSales > 0 ? (d.sales / purposeTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(d.sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="10">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = `<tr><th>순위</th><th>검사목적</th><th>매출액</th><th>건수</th><th>평균단가</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedPurposes.map(([name, d], i) => {
                    const avg = d.count > 0 ? d.sales / d.count : 0;
                    const percent = purposeTotalSales > 0 ? (d.sales / purposeTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(d.sales)}</td><td>${d.count}</td><td>${formatCurrency(avg)}</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            }
        }

        function updateSampleTypeMonthlyChart() {
            const sampleType = document.getElementById('sampleTypeMonthlySelect').value;
            const selectedManager = document.getElementById('sampleTypeManagerFilter').value;
            const selectedPurpose = document.getElementById('sampleTypePurposeFilter').value;
            const ctx = document.getElementById('sampleTypeMonthlyChart').getContext('2d');
            if (charts.sampleTypeMonthly) charts.sampleTypeMonthly.destroy();

            // 필터 라벨 업데이트
            let filterLabel = '';
            if (selectedManager) filterLabel += `[${selectedManager}]`;
            if (selectedPurpose) filterLabel += `[${selectedPurpose}]`;
            document.getElementById('sampleTypeMonthlyFilterLabel').textContent = filterLabel;

            if (!sampleType) {
                ctx.clearRect(0, 0, ctx.canvas.width, ctx.canvas.height);
                return;
            }

            // 월별 라벨
            const labels = [];
            for (let i = 1; i <= 12; i++) labels.push(i + '월');

            // 현재 데이터에서 해당 검체유형의 월별 매출 가져오기
            const sampleTypeMonthData = currentData.by_sample_type_month && currentData.by_sample_type_month[sampleType]
                ? currentData.by_sample_type_month[sampleType] : {};

            // 담당자/목적 필터가 있으면 해당 필터의 데이터만 사용
            function getMonthlyValue(monthData, month) {
                if (!monthData || !monthData[month]) return 0;
                if (selectedManager && monthData[month].by_manager) {
                    return monthData[month].by_manager[selectedManager]?.sales || 0;
                }
                if (selectedPurpose && monthData[month].by_purpose) {
                    return monthData[month].by_purpose[selectedPurpose]?.sales || 0;
                }
                return monthData[month].sales || 0;
            }

            let chartLabel = (currentData.dateLabel || currentData.year + '년') + ' - ' + sampleType;
            if (selectedManager) chartLabel += ` (${selectedManager})`;
            if (selectedPurpose) chartLabel += ` (${selectedPurpose})`;

            const datasets = [{
                label: chartLabel,
                data: labels.map((_, i) => getMonthlyValue(sampleTypeMonthData, i + 1)),
                borderColor: '#3498db',
                backgroundColor: 'rgba(52, 152, 219, 0.1)',
                fill: true,
                tension: 0.4
            }];

            // 비교 데이터
            if (compareData && compareData.by_sample_type_month && compareData.by_sample_type_month[sampleType]) {
                const compareSampleTypeMonthData = compareData.by_sample_type_month[sampleType];

                let compareChartLabel = (compareData.dateLabel || compareData.year + '년') + ' - ' + sampleType;
                if (selectedManager) compareChartLabel += ` (${selectedManager})`;
                if (selectedPurpose) compareChartLabel += ` (${selectedPurpose})`;

                datasets.push({
                    label: compareChartLabel,
                    data: labels.map((_, i) => getMonthlyValue(compareSampleTypeMonthData, i + 1)),
                    borderColor: '#9b59b6',
                    backgroundColor: 'rgba(155, 89, 182, 0.1)',
                    fill: true,
                    tension: 0.4
                });
            }

            charts.sampleTypeMonthly = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } },
                    scales: { y: { ticks: { callback: v => formatCurrency(v) } } }
                }
            });
        }

        // ========== 검사항목 탭 함수들 ==========
        let allSampleTypes = [];  // 전체 검체유형 목록 저장

        async function loadFoodItemData() {
            const year = document.getElementById('yearSelect').value;
            const purpose = document.getElementById('foodItemPurposeFilter').value;
            const sampleType = document.getElementById('foodItemSampleTypeFilter').value;
            const sampleTypeInput = document.getElementById('foodItemSampleTypeInput').value.trim();
            const item = getSelectedItem();  // 최종 선택된 항목
            const manager = document.getElementById('foodItemManagerFilter').value;

            // 와일드카드 매칭을 위한 sample_type 결정
            let sampleTypeParam = sampleType;
            if (sampleTypeInput && (sampleTypeInput.includes('잔류농약') || sampleTypeInput.includes('항생물질'))) {
                // 와일드카드 패턴으로 전송 (백엔드에서 처리)
                sampleTypeParam = sampleTypeInput + '*';
            }

            showToast('검사항목 데이터 로딩 중...', 'loading');

            try {
                const response = await fetch(`/api/food_item?year=${year}&purpose=${purpose}&sample_type=${encodeURIComponent(sampleTypeParam)}&item=${encodeURIComponent(item)}&manager=${manager}`);
                foodItemData = await response.json();
                foodItemData.year = parseInt(year);

                // 비교 모드일 경우
                if (document.getElementById('compareCheck').checked) {
                    const compareYear = document.getElementById('compareYearSelect').value;
                    const compareResponse = await fetch(`/api/food_item?year=${compareYear}&purpose=${purpose}&sample_type=${encodeURIComponent(sampleTypeParam)}&item=${encodeURIComponent(item)}&manager=${manager}`);
                    compareFoodItemData = await compareResponse.json();
                    compareFoodItemData.year = parseInt(compareYear);
                } else {
                    compareFoodItemData = null;
                }

                // 필터 드롭다운 초기화 (첫 로드 시에만)
                if (allSampleTypes.length === 0) {
                    initFoodItemFilters();
                }

                updateFoodItemDisplay();
                hideToast();
            } catch (error) {
                console.error('Food item data load error:', error);
                showToast('검사항목 데이터 로드 실패', 'error');
            }
        }

        function initFoodItemFilters() {
            if (!foodItemData) return;

            // 검사목적 필터
            const purposeSelect = document.getElementById('foodItemPurposeFilter');
            purposeSelect.innerHTML = '<option value="전체">전체</option>';
            foodItemData.purposes.forEach(p => {
                purposeSelect.innerHTML += `<option value="${p}">${p}</option>`;
            });

            // 검체유형 필터 (전체 저장)
            allSampleTypes = [...foodItemData.sample_types];
            updateSampleTypeDropdownByPurpose();

            // 영업담당 필터
            const managerSelect = document.getElementById('foodItemManagerFilter');
            managerSelect.innerHTML = '<option value="전체">전체</option>';
            foodItemData.managers.forEach(m => {
                managerSelect.innerHTML += `<option value="${m}">${m}</option>`;
            });

            // 항목 필터 업데이트
            updateItemFilters();
        }

        // 검사목적에 따른 검체유형 필터링
        function updateSampleTypeDropdownByPurpose() {
            const purpose = document.getElementById('foodItemPurposeFilter').value;
            let types = [];

            if (purpose === '전체') {
                types = allSampleTypes;
            } else if (foodItemData.by_purpose_sample_type && foodItemData.by_purpose_sample_type[purpose]) {
                types = foodItemData.by_purpose_sample_type[purpose];
            } else {
                types = allSampleTypes;
            }

            updateSampleTypeDropdown(types);
        }

        function updateSampleTypeDropdown(types) {
            const select = document.getElementById('foodItemSampleTypeFilter');
            const currentValue = select.value;
            select.innerHTML = '<option value="전체">전체</option>';
            types.slice(0, 100).forEach(st => {
                select.innerHTML += `<option value="${st}">${st}</option>`;
            });
            if (types.includes(currentValue)) {
                select.value = currentValue;
            }
        }

        function filterSampleTypeDropdown() {
            const input = document.getElementById('foodItemSampleTypeInput').value.toLowerCase();
            const purpose = document.getElementById('foodItemPurposeFilter').value;

            // 검사목적에 맞는 검체유형만 필터링
            let baseTypes = [];
            if (purpose === '전체') {
                baseTypes = allSampleTypes;
            } else if (foodItemData.by_purpose_sample_type && foodItemData.by_purpose_sample_type[purpose]) {
                baseTypes = foodItemData.by_purpose_sample_type[purpose];
            } else {
                baseTypes = allSampleTypes;
            }
            if (!input) {
                updateSampleTypeDropdown(baseTypes);
                return;
            }
            const filtered = baseTypes.filter(st => st.toLowerCase().includes(input));
            updateSampleTypeDropdown(filtered);
            if (filtered.length === 1) {
                document.getElementById('foodItemSampleTypeFilter').value = filtered[0];
            }
        }

        // 잔류농약/항생물질 여부 확인
        function isSpecialSampleType() {
            const sampleType = document.getElementById('foodItemSampleTypeFilter').value;
            const inputValue = document.getElementById('foodItemSampleTypeInput').value.trim();
            return sampleType.startsWith('잔류농약') || sampleType.startsWith('항생물질') ||
                   inputValue.includes('잔류농약') || inputValue.includes('항생물질');
        }

        // 검체유형에 따른 항목 목록 가져오기 (검사목적+검체유형 기반)
        function getItemsForSampleType() {
            if (!foodItemData) return [];

            // 잔류농약/항생물질은 항목 선택 불필요
            if (isSpecialSampleType()) {
                return [];
            }

            const purpose = document.getElementById('foodItemPurposeFilter').value;
            const sampleType = document.getElementById('foodItemSampleTypeFilter').value;

            let items = [];

            // 검사목적+검체유형 조합으로 항목 조회
            if (purpose !== '전체' && sampleType !== '전체') {
                const key = `${purpose}|${sampleType}`;
                if (foodItemData.by_purpose_sample_type_item && foodItemData.by_purpose_sample_type_item[key]) {
                    items = foodItemData.by_purpose_sample_type_item[key];
                }
            } else if (sampleType !== '전체' && foodItemData.by_sample_type_item && foodItemData.by_sample_type_item[sampleType]) {
                // 검체유형만 선택된 경우
                items = foodItemData.by_sample_type_item[sampleType].map(i => i[0]);
            } else if (purpose !== '전체') {
                // 검사목적만 선택된 경우 - 해당 목적의 모든 항목
                const purposeItems = new Set();
                Object.keys(foodItemData.by_purpose_sample_type_item || {}).forEach(key => {
                    if (key.startsWith(purpose + '|')) {
                        foodItemData.by_purpose_sample_type_item[key].forEach(item => {
                            purposeItems.add(item);
                        });
                    }
                });
                items = [...purposeItems].sort();
            } else {
                items = foodItemData.items.slice(0, 200);
            }

            return items;
        }

        // 항목 드롭다운 업데이트 (cascading)
        function updateItemFilters() {
            if (!foodItemData) return;

            const item1Select = document.getElementById('foodItemItem1Filter');
            const item2Select = document.getElementById('foodItemItem2Filter');
            const item3Select = document.getElementById('foodItemItem3Filter');

            // 잔류농약/항생물질인 경우 항목 드롭다운 비활성화
            if (isSpecialSampleType()) {
                item1Select.innerHTML = '<option value="전체">해당없음</option>';
                item2Select.innerHTML = '<option value="전체">해당없음</option>';
                item3Select.innerHTML = '<option value="전체">해당없음</option>';
                item1Select.disabled = true;
                item2Select.disabled = true;
                item3Select.disabled = true;
                return;
            }

            // 활성화
            item1Select.disabled = false;
            item2Select.disabled = false;
            item3Select.disabled = false;

            const items = getItemsForSampleType();
            const selected1 = item1Select.value;
            const selected2 = item2Select.value;
            const selected3 = item3Select.value;

            // 항목명1: 모든 항목
            item1Select.innerHTML = '<option value="전체">전체</option>';
            items.forEach(item => {
                item1Select.innerHTML += `<option value="${item}">${item}</option>`;
            });
            if (items.includes(selected1)) item1Select.value = selected1;

            // 항목명2: 항목명1에서 선택한 것 제외
            const items2 = items.filter(i => i !== selected1 || selected1 === '전체');
            item2Select.innerHTML = '<option value="전체">전체</option>';
            items2.forEach(item => {
                item2Select.innerHTML += `<option value="${item}">${item}</option>`;
            });
            if (items2.includes(selected2)) item2Select.value = selected2;

            // 항목명3: 항목명1, 2에서 선택한 것 제외
            const items3 = items.filter(i =>
                (i !== selected1 || selected1 === '전체') &&
                (i !== selected2 || selected2 === '전체')
            );
            item3Select.innerHTML = '<option value="전체">전체</option>';
            items3.forEach(item => {
                item3Select.innerHTML += `<option value="${item}">${item}</option>`;
            });
            if (items3.includes(selected3)) item3Select.value = selected3;
        }

        // 검사목적 변경 시 호출
        function onPurposeChange() {
            // 검체유형, 항목 선택 초기화
            document.getElementById('foodItemSampleTypeInput').value = '';
            document.getElementById('foodItemSampleTypeFilter').value = '전체';
            document.getElementById('foodItemItem1Filter').value = '전체';
            document.getElementById('foodItemItem2Filter').value = '전체';
            document.getElementById('foodItemItem3Filter').value = '전체';
            updateSampleTypeDropdownByPurpose();
            updateItemFilters();
            loadFoodItemData();
        }

        // 검체유형 변경 시 호출
        function onSampleTypeChange() {
            // 항목 선택 초기화
            document.getElementById('foodItemItem1Filter').value = '전체';
            document.getElementById('foodItemItem2Filter').value = '전체';
            document.getElementById('foodItemItem3Filter').value = '전체';
            updateItemFilters();
            loadFoodItemData();
        }

        // 항목 선택 시 호출 (cascading 업데이트)
        function onItemSelect(level) {
            // 하위 레벨 초기화
            if (level === 1) {
                document.getElementById('foodItemItem2Filter').value = '전체';
                document.getElementById('foodItemItem3Filter').value = '전체';
            } else if (level === 2) {
                document.getElementById('foodItemItem3Filter').value = '전체';
            }
            updateItemFilters();
            loadFoodItemData();
        }

        // 최종 선택된 항목 가져오기 (3 -> 2 -> 1 순서로 확인)
        function getSelectedItem() {
            const item3 = document.getElementById('foodItemItem3Filter').value;
            if (item3 !== '전체') return item3;
            const item2 = document.getElementById('foodItemItem2Filter').value;
            if (item2 !== '전체') return item2;
            const item1 = document.getElementById('foodItemItem1Filter').value;
            if (item1 !== '전체') return item1;
            return '전체';
        }

        function updateFoodItemTab() {
            updateItemFilters();
            loadFoodItemData();
        }

        function updateFoodItemDisplay() {
            if (!foodItemData) return;

            // 요약 카드 업데이트
            document.getElementById('foodItemTotalCount').textContent = foodItemData.total_count.toLocaleString() + '건';
            document.getElementById('foodItemTotalFee').textContent = formatCurrency(foodItemData.total_fee);

            // 차트 업데이트
            updateFoodItemChart();
            updateFoodItemTable();
            updateFoodItemSelects();
            updateFoodItemFeeCharts();
        }

        function updateFoodItemChart() {
            const ctx = document.getElementById('foodItemChart').getContext('2d');
            if (charts.foodItem) charts.foodItem.destroy();

            const top20 = foodItemData.by_item.slice(0, 20);
            const labels = top20.map(d => d[0].length > 15 ? d[0].substring(0, 15) + '...' : d[0]);

            const datasets = [{
                label: foodItemData.year + '년',
                data: top20.map(d => d[1].count),
                backgroundColor: 'rgba(52, 152, 219, 0.7)'
            }];

            if (compareFoodItemData) {
                const compareMap = Object.fromEntries(compareFoodItemData.by_item);
                datasets.push({
                    label: compareFoodItemData.year + '년',
                    data: top20.map(d => compareMap[d[0]]?.count || 0),
                    backgroundColor: 'rgba(155, 89, 182, 0.7)'
                });
            }

            charts.foodItem = new Chart(ctx, {
                type: 'bar',
                data: { labels, datasets },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: !!compareFoodItemData } }
                }
            });
        }

        function updateFoodItemTable() {
            const thead = document.getElementById('foodItemTableHead');
            const tbody = document.querySelector('#foodItemTable tbody');
            const totalCount = foodItemData.total_count || 1;

            if (compareFoodItemData) {
                const compareMap = Object.fromEntries(compareFoodItemData.by_item);
                thead.innerHTML = `<tr><th>순위</th><th>항목명</th><th>${foodItemData.year}년 건수</th><th>${compareFoodItemData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th><th>${foodItemData.year}년 수수료</th><th>비중</th></tr>`;
                tbody.innerHTML = foodItemData.by_item.slice(0, 50).map((d, i) => {
                    const compCount = compareMap[d[0]]?.count || 0;
                    const compFee = compareMap[d[0]]?.fee || 0;
                    const countDiff = d[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    const ratio = (d[1].count / totalCount * 100).toFixed(1);
                    return `<tr><td>${i+1}</td><td title="${d[0]}">${d[0].length > 20 ? d[0].substring(0, 20) + '...' : d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td><td>${formatCurrency(d[1].fee)}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = '<tr><th>순위</th><th>항목명</th><th>건수</th><th>항목수수료</th><th>비중</th></tr>';
                tbody.innerHTML = foodItemData.by_item.slice(0, 50).map((d, i) => {
                    const ratio = (d[1].count / totalCount * 100).toFixed(1);
                    return `<tr><td>${i+1}</td><td title="${d[0]}">${d[0].length > 20 ? d[0].substring(0, 20) + '...' : d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${formatCurrency(d[1].fee)}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="5">데이터 없음</td></tr>';
            }
        }

        function updateFoodItemSelects() {
            // 분석자 테이블용 항목 선택
            const analyzerSelect = document.getElementById('foodItemAnalyzerSelect');
            analyzerSelect.innerHTML = '<option value="">항목 선택</option>';
            foodItemData.by_item.slice(0, 50).forEach(d => {
                analyzerSelect.innerHTML += `<option value="${d[0]}">${d[0].length > 30 ? d[0].substring(0, 30) + '...' : d[0]}</option>`;
            });

            // 월별 추이용 항목 선택
            const monthlySelect = document.getElementById('foodItemMonthlySelect');
            monthlySelect.innerHTML = '<option value="">항목 선택</option>';
            foodItemData.by_item.slice(0, 50).forEach(d => {
                monthlySelect.innerHTML += `<option value="${d[0]}">${d[0].length > 30 ? d[0].substring(0, 30) + '...' : d[0]}</option>`;
            });
        }

        function updateFoodItemAnalyzerTable() {
            const item = document.getElementById('foodItemAnalyzerSelect').value;
            const thead = document.getElementById('foodItemAnalyzerTableHead');
            const tbody = document.querySelector('#foodItemAnalyzerTable tbody');

            if (!item || !foodItemData.by_item_analyzer || !foodItemData.by_item_analyzer[item]) {
                tbody.innerHTML = '<tr><td colspan="4">항목을 선택하세요</td></tr>';
                return;
            }

            const analyzerData = foodItemData.by_item_analyzer[item];

            if (compareFoodItemData && compareFoodItemData.by_item_analyzer && compareFoodItemData.by_item_analyzer[item]) {
                const compareAnalyzerData = compareFoodItemData.by_item_analyzer[item];
                const compareMap = Object.fromEntries(compareAnalyzerData);
                thead.innerHTML = `<tr><th>순위</th><th>분석자</th><th>${foodItemData.year}년 건수</th><th>${compareFoodItemData.year}년 건수</th><th>건수 증감</th><th>증감율(%)</th></tr>`;
                tbody.innerHTML = analyzerData.map((d, i) => {
                    const compCount = compareMap[d[0]]?.count || 0;
                    const countDiff = d[1].count - compCount;
                    const countDiffRate = compCount > 0 ? ((countDiff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td>${d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiff.toLocaleString()}</td><td class="${countDiff >= 0 ? 'positive' : 'negative'}">${countDiff >= 0 ? '+' : ''}${countDiffRate}%</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
            } else {
                thead.innerHTML = '<tr><th>순위</th><th>분석자</th><th>건수</th><th>항목수수료</th></tr>';
                tbody.innerHTML = analyzerData.map((d, i) =>
                    `<tr><td>${i+1}</td><td>${d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${formatCurrency(d[1].fee)}</td></tr>`
                ).join('') || '<tr><td colspan="4">데이터 없음</td></tr>';
            }
        }

        function updateFoodItemMonthlyChart() {
            const item = document.getElementById('foodItemMonthlySelect').value;
            const ctx = document.getElementById('foodItemMonthlyChart').getContext('2d');
            if (charts.foodItemMonthly) charts.foodItemMonthly.destroy();

            if (!item || !foodItemData.by_item_month || !foodItemData.by_item_month[item]) {
                return;
            }

            const labels = ['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
            const monthData = Object.fromEntries(foodItemData.by_item_month[item]);

            const datasets = [{
                label: foodItemData.year + '년',
                data: labels.map((_, i) => monthData[i+1] || 0),
                borderColor: '#3498db',
                backgroundColor: 'rgba(52, 152, 219, 0.1)',
                fill: true,
                tension: 0.4
            }];

            if (compareFoodItemData && compareFoodItemData.by_item_month && compareFoodItemData.by_item_month[item]) {
                const compareMonthData = Object.fromEntries(compareFoodItemData.by_item_month[item]);
                datasets.push({
                    label: compareFoodItemData.year + '년',
                    data: labels.map((_, i) => compareMonthData[i+1] || 0),
                    borderColor: '#9b59b6',
                    backgroundColor: 'rgba(155, 89, 182, 0.1)',
                    fill: true,
                    tension: 0.4
                });
            }

            charts.foodItemMonthly = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } }
                }
            });
        }

        function updateFoodItemFeeCharts() {
            // 월별 수수료 추이
            const feeCtx = document.getElementById('foodItemFeeYearlyChart').getContext('2d');
            if (charts.foodItemFeeYearly) charts.foodItemFeeYearly.destroy();

            const labels = ['1월','2월','3월','4월','5월','6월','7월','8월','9월','10월','11월','12월'];
            const monthFeeData = Object.fromEntries(foodItemData.by_month_fee);

            const feeDatasets = [{
                label: foodItemData.year + '년',
                data: labels.map((_, i) => monthFeeData[i+1]?.fee || 0),
                borderColor: '#27ae60',
                backgroundColor: 'rgba(39, 174, 96, 0.1)',
                fill: true,
                tension: 0.4
            }];

            if (compareFoodItemData) {
                const compareMonthFeeData = Object.fromEntries(compareFoodItemData.by_month_fee);
                feeDatasets.push({
                    label: compareFoodItemData.year + '년',
                    data: labels.map((_, i) => compareMonthFeeData[i+1]?.fee || 0),
                    borderColor: '#e67e22',
                    backgroundColor: 'rgba(230, 126, 34, 0.1)',
                    fill: true,
                    tension: 0.4
                });
            }

            charts.foodItemFeeYearly = new Chart(feeCtx, {
                type: 'line',
                data: { labels, datasets: feeDatasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: true } },
                    scales: { y: { ticks: { callback: v => formatCurrency(v) } } }
                }
            });

            // 영업담당별 수수료
            const managerCtx = document.getElementById('foodItemManagerFeeChart').getContext('2d');
            if (charts.foodItemManagerFee) charts.foodItemManagerFee.destroy();

            const managerData = foodItemData.by_manager_item.slice(0, 15);
            const managerLabels = managerData.map(d => d[0]);

            const managerDatasets = [{
                label: foodItemData.year + '년',
                data: managerData.map(d => d[1].fee),
                backgroundColor: 'rgba(52, 152, 219, 0.7)'
            }];

            if (compareFoodItemData) {
                const compareManagerMap = Object.fromEntries(compareFoodItemData.by_manager_item);
                managerDatasets.push({
                    label: compareFoodItemData.year + '년',
                    data: managerData.map(d => compareManagerMap[d[0]]?.fee || 0),
                    backgroundColor: 'rgba(155, 89, 182, 0.7)'
                });
            }

            charts.foodItemManagerFee = new Chart(managerCtx, {
                type: 'bar',
                data: { labels: managerLabels, datasets: managerDatasets },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: { legend: { display: !!compareFoodItemData } },
                    scales: { y: { ticks: { callback: v => formatCurrency(v) } } }
                }
            });
        }

        // 페이지 로드 시 초기화
        initDateSelectors();
        showToast('조회 조건을 선택하고 [조회하기] 버튼을 클릭하세요.', 'loading', 5000);
        setTimeout(() => hideToast(), 5000);

        // ========== AI 분석 함수들 ==========
        function setAiQuery(query) {
            document.getElementById('aiQueryInput').value = query;
        }

        async function runAiAnalysis() {
            const query = document.getElementById('aiQueryInput').value.trim();
            if (!query) {
                alert('질문을 입력해주세요.');
                return;
            }

            // UI 상태 변경
            document.getElementById('aiLoading').style.display = 'block';
            document.getElementById('aiResult').style.display = 'none';
            document.getElementById('aiError').style.display = 'none';

            try {
                const response = await fetch('/api/ai/analyze', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({query: query})
                });

                const data = await response.json();
                document.getElementById('aiLoading').style.display = 'none';

                if (data.error) {
                    document.getElementById('aiError').innerHTML = `<strong>오류:</strong> ${data.error}`;
                    document.getElementById('aiError').style.display = 'block';
                    return;
                }

                // 결과 표시
                displayAiResult(data);
            } catch (error) {
                document.getElementById('aiLoading').style.display = 'none';
                document.getElementById('aiError').innerHTML = `<strong>오류:</strong> ${error.message}`;
                document.getElementById('aiError').style.display = 'block';
            }
        }

        function displayAiResult(data) {
            document.getElementById('aiResult').style.display = 'block';

            // direct_answer 타입 특별 처리
            if (data.analysis_type === 'direct_answer') {
                document.getElementById('aiDescription').innerHTML = `
                    <strong>📝 분석 내용:</strong> ${data.description || '직접 답변'}<br>
                    <div style="margin-top: 10px; padding: 15px; background: #e3f2fd; border-radius: 8px; font-size: 1.1em;">
                        ${data.direct_answer}
                    </div>
                `;
                document.getElementById('aiTableContainer').innerHTML = '';
                document.getElementById('aiInsight').innerHTML = '💡 <strong>인사이트:</strong> AI가 캐시된 데이터를 기반으로 직접 답변을 생성했습니다.';
                return;
            }

            // 설명 표시
            const desc = data.description || '분석 완료';
            const parsed = data.parsed_query || {};
            document.getElementById('aiDescription').innerHTML = `
                <strong>📝 분석 내용:</strong> ${desc}<br>
                <small style="color: #666;">조건: ${parsed.year || ''}년 /
                ${parsed.purpose || '전체 목적'} /
                ${parsed.sample_type || '전체 유형'} /
                ${parsed.item || '전체 항목'}
                ${parsed.exclude_item ? ' / 제외: ' + parsed.exclude_item : ''}</small>
            `;

            // 차트 그리기
            if (data.chart_data) {
                drawAiChart(data.chart_data, data.analysis_type);
            }

            // 테이블 표시
            displayAiTable(data);

            // 인사이트 표시
            displayAiInsight(data);
        }

        function drawAiChart(chartData, analysisType) {
            const ctx = document.getElementById('aiChart').getContext('2d');
            if (charts.aiChart) charts.aiChart.destroy();

            const colors = [
                'rgba(102, 126, 234, 0.7)',
                'rgba(118, 75, 162, 0.7)',
                'rgba(255, 193, 7, 0.7)',
                'rgba(76, 175, 80, 0.7)'
            ];

            const datasets = chartData.datasets.map((ds, i) => ({
                label: ds.label,
                data: ds.data,
                backgroundColor: colors[i % colors.length],
                borderColor: colors[i % colors.length].replace('0.7', '1'),
                borderWidth: 1
            }));

            charts.aiChart = new Chart(ctx, {
                type: analysisType === 'top_items' ? 'bar' : 'line',
                data: {
                    labels: chartData.labels,
                    datasets: datasets
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: datasets.length > 1 }
                    },
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                callback: function(value) {
                                    if (value >= 10000) return (value/10000).toFixed(0) + '만';
                                    return value;
                                }
                            }
                        }
                    }
                }
            });
        }

        function displayAiTable(data) {
            const container = document.getElementById('aiTableContainer');
            let html = '';

            if (data.analysis_type === 'monthly_trend' && data.chart_data) {
                html = `<table><thead><tr><th>월</th>`;
                data.chart_data.datasets.forEach(ds => {
                    html += `<th>${ds.label}</th>`;
                });
                html += `</tr></thead><tbody>`;

                data.chart_data.labels.forEach((label, i) => {
                    html += `<tr><td>${label}</td>`;
                    data.chart_data.datasets.forEach(ds => {
                        const val = ds.data[i];
                        html += `<td>${formatCurrency(val)}</td>`;
                    });
                    html += `</tr>`;
                });
                html += `</tbody></table>`;
            } else if (data.analysis_type === 'top_items' && data.top_items) {
                html = `<table><thead><tr><th>순위</th><th>항목명</th><th>건수</th><th>수수료</th></tr></thead><tbody>`;
                data.top_items.forEach((item, i) => {
                    html += `<tr><td>${i+1}</td><td>${item.name}</td><td>${item.count.toLocaleString()}</td><td>${formatCurrency(item.fee)}</td></tr>`;
                });
                html += `</tbody></table>`;
            } else if (data.analysis_type === 'comparison' && data.comparison) {
                const c = data.comparison;
                html = `<table><thead><tr><th>구분</th><th>건수</th><th>수수료</th></tr></thead><tbody>`;
                html += `<tr><td>전체</td><td>${c.with_item.count.toLocaleString()}</td><td>${formatCurrency(c.with_item.fee)}</td></tr>`;
                html += `<tr><td>제외 후</td><td>${c.without_item.count.toLocaleString()}</td><td>${formatCurrency(c.without_item.fee)}</td></tr>`;
                html += `<tr style="font-weight: bold; color: #c62828;"><td>차이</td><td>-${c.difference.count.toLocaleString()}</td><td>-${formatCurrency(c.difference.fee)}</td></tr>`;
                html += `</tbody></table>`;
            } else if (data.summary) {
                html = `<table><thead><tr><th>항목</th><th>값</th></tr></thead><tbody>`;
                html += `<tr><td>총 건수</td><td>${data.summary.total_count.toLocaleString()}건</td></tr>`;
                html += `<tr><td>총 수수료</td><td>${formatCurrency(data.summary.total_fee)}</td></tr>`;
                html += `<tr><td>평균 수수료</td><td>${formatCurrency(data.summary.avg_fee)}</td></tr>`;
                html += `</tbody></table>`;
            }

            container.innerHTML = html;
        }

        function displayAiInsight(data) {
            const insight = document.getElementById('aiInsight');
            let text = '💡 <strong>인사이트:</strong> ';

            if (data.analysis_type === 'monthly_trend') {
                text += `총 매출 ${formatCurrency(data.total_fee || 0)}`;
                if (data.total_diff) {
                    text += `, 제외 시 연간 <span style="color: #c62828; font-weight: bold;">-${formatCurrency(data.total_diff)}</span> 감소 예상`;
                }
            } else if (data.analysis_type === 'comparison' && data.comparison) {
                const pct = ((data.comparison.difference.fee / data.comparison.with_item.fee) * 100).toFixed(1);
                text += `해당 항목 제외 시 매출 <span style="color: #c62828; font-weight: bold;">${pct}%</span> 감소 (${formatCurrency(data.comparison.difference.fee)})`;
            } else if (data.analysis_type === 'top_items' && data.top_items) {
                text += `상위 ${data.top_items.length}개 항목 중 1위는 <strong>${data.top_items[0]?.name || '-'}</strong> (${data.top_items[0]?.count.toLocaleString() || 0}건)`;
            } else {
                text += `총 ${data.total_count?.toLocaleString() || 0}건의 데이터가 분석되었습니다.`;
            }

            insight.innerHTML = text;
        }

        // ========== 목표 달성 분석 함수들 ==========
        let goalFilterOptions = null;  // 필터 옵션 캐시

        function toggleGoalFilters() {
            const panel = document.getElementById('goalFiltersPanel');
            const btn = document.getElementById('filterToggleBtn');
            if (panel.style.display === 'none') {
                panel.style.display = 'block';
                btn.textContent = '▲ 필터 닫기';
            } else {
                panel.style.display = 'none';
                btn.textContent = '▼ 필터 열기';
            }
        }

        function toggleAllGoalFilters(type) {
            const allCheckbox = document.getElementById(`goal${type.charAt(0).toUpperCase() + type.slice(1)}All`);
            const checkboxes = document.querySelectorAll(`.goal${type.charAt(0).toUpperCase() + type.slice(1)}Filter`);
            checkboxes.forEach(cb => cb.checked = false);
        }

        function getSelectedGoalFilters() {
            const filters = {};

            // 영업담당
            if (!document.getElementById('goalManagerAll').checked) {
                filters.managers = [...document.querySelectorAll('.goalManagerFilter:checked')].map(cb => cb.value);
            }

            // 팀
            if (!document.getElementById('goalTeamAll').checked) {
                filters.teams = [...document.querySelectorAll('.goalTeamFilter:checked')].map(cb => cb.value);
            }

            // 월
            if (!document.getElementById('goalMonthAll').checked) {
                filters.months = [...document.querySelectorAll('.goalMonthFilter:checked')].map(cb => parseInt(cb.value));
            }

            // 검사목적
            if (!document.getElementById('goalPurposeAll').checked) {
                filters.purposes = [...document.querySelectorAll('.goalPurposeFilter:checked')].map(cb => cb.value);
            }

            // 지역
            if (!document.getElementById('goalRegionAll').checked) {
                filters.regions = [...document.querySelectorAll('.goalRegionFilter:checked')].map(cb => cb.value);
            }

            // 검체유형
            if (!document.getElementById('goalSampleTypeAll').checked) {
                filters.sample_types = [...document.querySelectorAll('.goalSampleTypeFilter:checked')].map(cb => cb.value);
            }

            // 분석자
            if (!document.getElementById('goalAnalyzerAll').checked) {
                filters.analyzers = [...document.querySelectorAll('.goalAnalyzerFilter:checked')].map(cb => cb.value);
            }

            return filters;
        }

        function populateGoalFilters(options) {
            goalFilterOptions = options;

            // 영업담당
            const managerDiv = document.getElementById('goalManagerFilters');
            managerDiv.innerHTML = options.managers.map(m =>
                `<label style="display: block;"><input type="checkbox" class="goalManagerFilter" value="${m}"> ${m}</label>`
            ).join('');

            // 팀
            const teamDiv = document.getElementById('goalTeamFilters');
            teamDiv.innerHTML = options.teams.map(t =>
                `<label style="display: block;"><input type="checkbox" class="goalTeamFilter" value="${t}"> ${t}</label>`
            ).join('');

            // 검사목적
            const purposeDiv = document.getElementById('goalPurposeFilters');
            purposeDiv.innerHTML = options.purposes.map(p =>
                `<label style="display: block;"><input type="checkbox" class="goalPurposeFilter" value="${p}"> ${p}</label>`
            ).join('');

            // 지역
            const regionDiv = document.getElementById('goalRegionFilters');
            regionDiv.innerHTML = options.regions.map(r =>
                `<label style="display: block;"><input type="checkbox" class="goalRegionFilter" value="${r}"> ${r}</label>`
            ).join('');

            // 검체유형
            const sampleTypeDiv = document.getElementById('goalSampleTypeFilters');
            sampleTypeDiv.innerHTML = options.sample_types.map(st =>
                `<label style="display: block;"><input type="checkbox" class="goalSampleTypeFilter" value="${st}"> ${st}</label>`
            ).join('');

            // 분석자
            const analyzerDiv = document.getElementById('goalAnalyzerFilters');
            analyzerDiv.innerHTML = options.analyzers.map(a =>
                `<label style="display: block;"><input type="checkbox" class="goalAnalyzerFilter" value="${a}"> ${a}</label>`
            ).join('');
        }

        async function runGoalAnalysis() {
            const targetYear = document.getElementById('goalYear').value;
            const targetAmount = document.getElementById('goalTarget').value * 100000000; // 억 -> 원
            const filters = getSelectedGoalFilters();

            document.getElementById('goalLoading').style.display = 'block';
            document.getElementById('goalResult').style.display = 'none';

            try {
                const response = await fetch('/api/ai/goal-analysis', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({target: targetAmount, year: parseInt(targetYear), filters: filters})
                });

                const data = await response.json();
                document.getElementById('goalLoading').style.display = 'none';

                if (data.error) {
                    alert('오류: ' + data.error);
                    return;
                }

                // 필터 옵션 업데이트
                if (data.filter_options) {
                    populateGoalFilters(data.filter_options);
                }

                displayGoalResult(data);
            } catch (error) {
                document.getElementById('goalLoading').style.display = 'none';
                alert('분석 실패: ' + error.message);
            }
        }

        function displayGoalResult(data) {
            document.getElementById('goalResult').style.display = 'block';

            // 현황 요약
            const status = data.current_status;
            const summaryHtml = `
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px;">
                    <div style="background: white; padding: 15px; border-radius: 8px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
                        <div style="color: #888; font-size: 13px;">2024년 매출</div>
                        <div style="font-size: 24px; font-weight: bold; color: #667eea;">${formatCurrency(status.revenue_2024)}</div>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
                        <div style="color: #888; font-size: 13px;">2025년 매출</div>
                        <div style="font-size: 24px; font-weight: bold; color: #11998e;">${formatCurrency(status.revenue_2025)}</div>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
                        <div style="color: #888; font-size: 13px;">현재 성장률</div>
                        <div style="font-size: 24px; font-weight: bold; color: ${status.growth_rate >= 0 ? '#4caf50' : '#f44336'};">${status.growth_rate >= 0 ? '+' : ''}${status.growth_rate}%</div>
                    </div>
                    <div style="background: white; padding: 15px; border-radius: 8px; text-align: center; box-shadow: 0 2px 5px rgba(0,0,0,0.1);">
                        <div style="color: #888; font-size: 13px;">목표까지</div>
                        <div style="font-size: 24px; font-weight: bold; color: #ff9800;">${formatCurrency(status.gap_to_target)}</div>
                        <div style="color: #888; font-size: 12px;">(+${status.required_growth}% 필요)</div>
                    </div>
                </div>
            `;
            document.getElementById('goalSummary').innerHTML = summaryHtml;

            // 추천사항
            let recsHtml = '<h3 style="margin-bottom: 15px;">📋 개선 추천사항</h3>';
            data.recommendations.forEach(rec => {
                const priorityColor = rec.priority === 'high' ? '#f44336' : '#ff9800';
                const priorityBg = rec.priority === 'high' ? '#ffebee' : '#fff8e1';
                recsHtml += `
                    <div style="background: ${priorityBg}; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 4px solid ${priorityColor};">
                        <div style="display: flex; justify-content: space-between; align-items: center;">
                            <span style="font-weight: bold;">${rec.category} ${rec.title}</span>
                            <span style="background: ${priorityColor}; color: white; padding: 2px 8px; border-radius: 10px; font-size: 11px;">${rec.priority === 'high' ? '중요' : '참고'}</span>
                        </div>
                        <div style="margin-top: 8px; color: #555;">${rec.content}</div>
                        <div style="margin-top: 5px; color: #11998e; font-weight: bold;">→ ${rec.action}</div>
                    </div>
                `;
            });
            document.getElementById('goalRecommendations').innerHTML = recsHtml;

            // 영업담당별 테이블
            const managerTbody = document.querySelector('#goalManagerTable tbody');
            managerTbody.innerHTML = data.analysis.by_manager.map(m => `
                <tr>
                    <td>${m.name}</td>
                    <td>${formatCurrency(m.revenue_2024)}</td>
                    <td>${formatCurrency(m.revenue_2025)}</td>
                    <td class="${m.growth >= 0 ? 'positive' : 'negative'}">${m.growth >= 0 ? '+' : ''}${m.growth}%</td>
                </tr>
            `).join('');

            // 검사목적별 테이블
            const purposeTbody = document.querySelector('#goalPurposeTable tbody');
            purposeTbody.innerHTML = data.analysis.by_purpose.map(p => `
                <tr>
                    <td>${p.name}</td>
                    <td>${formatCurrency(p.revenue_2024)}</td>
                    <td>${formatCurrency(p.revenue_2025)}</td>
                    <td class="${p.growth >= 0 ? 'positive' : 'negative'}">${p.growth >= 0 ? '+' : ''}${p.growth}%</td>
                    <td>${p.share}%</td>
                </tr>
            `).join('');

            // 지역별 테이블
            const regionTbody = document.querySelector('#goalRegionTable tbody');
            regionTbody.innerHTML = data.analysis.by_region.map(r => `
                <tr>
                    <td>${r.name}</td>
                    <td>${formatCurrency(r.revenue_2024)}</td>
                    <td>${formatCurrency(r.revenue_2025)}</td>
                    <td class="${r.growth >= 0 ? 'positive' : 'negative'}">${r.growth >= 0 ? '+' : ''}${r.growth}%</td>
                </tr>
            `).join('');

            // 항목별 테이블
            const itemTbody = document.querySelector('#goalItemTable tbody');
            itemTbody.innerHTML = data.analysis.by_item.map(i => `
                <tr>
                    <td title="${i.name}">${i.name.length > 20 ? i.name.substring(0,20)+'...' : i.name}</td>
                    <td>${formatCurrency(i.fee_2024)}</td>
                    <td>${formatCurrency(i.fee_2025)}</td>
                    <td class="${i.growth >= 0 ? 'positive' : 'negative'}">${i.growth >= 0 ? '+' : ''}${i.growth}%</td>
                </tr>
            `).join('');
        }

        // ========== 기업 정보 관리 함수 ==========
        function updateTotalEmployees() {
            const depts = ['executive', 'admin', 'finance', 'qa', 'support', 'lab', 'sales', 'branch', 'marketing'];
            let total = 0;
            depts.forEach(dept => {
                const count = parseInt(document.getElementById(`dept_${dept}_count`).value) || 0;
                total += count;
            });
            document.getElementById('totalEmployees').textContent = total;
        }

        // 부서 인원수 변경 시 자동 계산
        document.querySelectorAll('[id^="dept_"][id$="_count"]').forEach(input => {
            input.addEventListener('change', updateTotalEmployees);
            input.addEventListener('input', updateTotalEmployees);
        });

        function getCompanyInfo() {
            const depts = ['executive', 'admin', 'finance', 'qa', 'support', 'lab', 'sales', 'branch', 'marketing'];
            const deptNames = ['임원', '총무', '재무', '품질보증', '고객지원', '분석실', '직영 영업부', '지사', '마케팅'];

            const departments = {};
            depts.forEach((dept, idx) => {
                departments[deptNames[idx]] = {
                    count: parseInt(document.getElementById(`dept_${dept}_count`).value) || 0,
                    head: document.getElementById(`dept_${dept}_head`).value || '',
                    role: document.getElementById(`dept_${dept}_role`).value || ''
                };
            });

            return {
                companyName: document.getElementById('companyName').value || '',
                foundedYear: document.getElementById('foundedYear').value || '',
                businessField: document.getElementById('businessField').value || '',
                mainServices: document.getElementById('mainServices').value || '',
                revenueTarget: document.getElementById('revenueTarget').value || '',
                inspectionTarget: document.getElementById('inspectionTarget').value || '',
                kpiDescription: document.getElementById('kpiDescription').value || '',
                businessStrategy: document.getElementById('businessStrategy').value || '',
                departments: departments
            };
        }

        function setCompanyInfo(data) {
            document.getElementById('companyName').value = data.companyName || '';
            document.getElementById('foundedYear').value = data.foundedYear || '';
            document.getElementById('businessField').value = data.businessField || '';
            document.getElementById('mainServices').value = data.mainServices || '';
            document.getElementById('revenueTarget').value = data.revenueTarget || '';
            document.getElementById('inspectionTarget').value = data.inspectionTarget || '';
            document.getElementById('kpiDescription').value = data.kpiDescription || '';
            document.getElementById('businessStrategy').value = data.businessStrategy || '';

            const depts = ['executive', 'admin', 'finance', 'qa', 'support', 'lab', 'sales', 'branch', 'marketing'];
            const deptNames = ['임원', '총무', '재무', '품질보증', '고객지원', '분석실', '직영 영업부', '지사', '마케팅'];

            if (data.departments) {
                depts.forEach((dept, idx) => {
                    const deptData = data.departments[deptNames[idx]] || {};
                    document.getElementById(`dept_${dept}_count`).value = deptData.count || 0;
                    document.getElementById(`dept_${dept}_head`).value = deptData.head || '';
                    document.getElementById(`dept_${dept}_role`).value = deptData.role || '';
                });
            }
            updateTotalEmployees();
        }

        async function saveCompanyInfo() {
            const data = getCompanyInfo();
            try {
                const response = await fetch('/api/company-info', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(data)
                });
                const result = await response.json();
                if (result.success) {
                    alert('기업 정보가 저장되었습니다.');
                } else {
                    alert('저장 실패: ' + (result.error || '알 수 없는 오류'));
                }
            } catch (error) {
                alert('저장 중 오류 발생: ' + error.message);
            }
        }

        async function loadCompanyInfo() {
            try {
                const response = await fetch('/api/company-info');
                const result = await response.json();
                if (result.success && result.data) {
                    setCompanyInfo(result.data);
                    alert('기업 정보를 불러왔습니다.');
                } else if (!result.data) {
                    alert('저장된 기업 정보가 없습니다. 새로 입력해주세요.');
                } else {
                    alert('불러오기 실패: ' + (result.error || '알 수 없는 오류'));
                }
            } catch (error) {
                alert('불러오기 중 오류 발생: ' + error.message);
            }
        }

        // ========== 영업부/지사 인력 관리 ==========
        let salesPersonCounter = 0;
        let branchPersonCounter = 0;

        function addSalesPerson(data = null) {
            salesPersonCounter++;
            const id = salesPersonCounter;
            const container = document.getElementById('salesPersonList');
            document.getElementById('salesPersonEmpty').style.display = 'none';

            const div = document.createElement('div');
            div.id = `salesPerson_${id}`;
            div.style.cssText = 'display: grid; grid-template-columns: 1fr 1fr 2fr auto; gap: 10px; padding: 10px; background: #f8f9fa; border-radius: 5px; margin-bottom: 10px; align-items: center;';
            div.innerHTML = `
                <input type="text" placeholder="이름" class="sales-name" value="${data?.name || ''}" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                <input type="text" placeholder="담당 지역" class="sales-region" value="${data?.region || ''}" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                <input type="text" placeholder="담당 업무 (예: 신규 개척, 기존 고객 관리)" class="sales-role" value="${data?.role || ''}" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                <button onclick="removeSalesPerson(${id})" style="padding: 8px 12px; background: #e74c3c; color: white; border: none; border-radius: 4px; cursor: pointer;">삭제</button>
            `;
            container.appendChild(div);
            updateSalesCount();
        }

        function removeSalesPerson(id) {
            const element = document.getElementById(`salesPerson_${id}`);
            if (element) {
                element.remove();
                updateSalesCount();
                if (document.getElementById('salesPersonList').children.length === 0) {
                    document.getElementById('salesPersonEmpty').style.display = 'block';
                }
            }
        }

        function updateSalesCount() {
            const count = document.getElementById('salesPersonList').children.length;
            document.getElementById('dept_sales_count').value = count;
            updateTotalEmployees();
        }

        function addBranchPerson(data = null) {
            branchPersonCounter++;
            const id = branchPersonCounter;
            const container = document.getElementById('branchPersonList');
            document.getElementById('branchPersonEmpty').style.display = 'none';

            const div = document.createElement('div');
            div.id = `branchPerson_${id}`;
            div.style.cssText = 'display: grid; grid-template-columns: 1fr 1fr 2fr auto; gap: 10px; padding: 10px; background: #fdf2e9; border-radius: 5px; margin-bottom: 10px; align-items: center;';
            div.innerHTML = `
                <input type="text" placeholder="이름" class="branch-name" value="${data?.name || ''}" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                <input type="text" placeholder="담당 지역" class="branch-region" value="${data?.region || ''}" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                <input type="text" placeholder="담당 업무 (예: 시료 수거, 현장 영업)" class="branch-role" value="${data?.role || ''}" style="padding: 8px; border: 1px solid #ddd; border-radius: 4px;">
                <button onclick="removeBranchPerson(${id})" style="padding: 8px 12px; background: #e74c3c; color: white; border: none; border-radius: 4px; cursor: pointer;">삭제</button>
            `;
            container.appendChild(div);
            updateBranchCount();
        }

        function removeBranchPerson(id) {
            const element = document.getElementById(`branchPerson_${id}`);
            if (element) {
                element.remove();
                updateBranchCount();
                if (document.getElementById('branchPersonList').children.length === 0) {
                    document.getElementById('branchPersonEmpty').style.display = 'block';
                }
            }
        }

        function updateBranchCount() {
            const count = document.getElementById('branchPersonList').children.length;
            document.getElementById('dept_branch_count').value = count;
            updateTotalEmployees();
        }

        function getSalesPersonnel() {
            const personnel = [];
            document.querySelectorAll('#salesPersonList > div').forEach(div => {
                personnel.push({
                    name: div.querySelector('.sales-name').value || '',
                    region: div.querySelector('.sales-region').value || '',
                    role: div.querySelector('.sales-role').value || ''
                });
            });
            return personnel;
        }

        function getBranchPersonnel() {
            const personnel = [];
            document.querySelectorAll('#branchPersonList > div').forEach(div => {
                personnel.push({
                    name: div.querySelector('.branch-name').value || '',
                    region: div.querySelector('.branch-region').value || '',
                    role: div.querySelector('.branch-role').value || ''
                });
            });
            return personnel;
        }

        function setSalesPersonnel(personnel) {
            document.getElementById('salesPersonList').innerHTML = '';
            salesPersonCounter = 0;
            if (personnel && personnel.length > 0) {
                document.getElementById('salesPersonEmpty').style.display = 'none';
                personnel.forEach(p => addSalesPerson(p));
            } else {
                document.getElementById('salesPersonEmpty').style.display = 'block';
            }
        }

        function setBranchPersonnel(personnel) {
            document.getElementById('branchPersonList').innerHTML = '';
            branchPersonCounter = 0;
            if (personnel && personnel.length > 0) {
                document.getElementById('branchPersonEmpty').style.display = 'none';
                personnel.forEach(p => addBranchPerson(p));
            } else {
                document.getElementById('branchPersonEmpty').style.display = 'block';
            }
        }

        // getCompanyInfo와 setCompanyInfo 함수 업데이트 (원본 함수 재정의)
        const originalGetCompanyInfo = getCompanyInfo;
        getCompanyInfo = function() {
            const base = originalGetCompanyInfo();
            base.salesPersonnel = getSalesPersonnel();
            base.branchPersonnel = getBranchPersonnel();
            return base;
        };

        const originalSetCompanyInfo = setCompanyInfo;
        setCompanyInfo = function(data) {
            originalSetCompanyInfo(data);
            setSalesPersonnel(data.salesPersonnel || []);
            setBranchPersonnel(data.branchPersonnel || []);
        };

        // 페이지 로드 시 기업 정보 자동 로드
        window.addEventListener('load', async () => {
            try {
                const response = await fetch('/api/company-info');
                const result = await response.json();
                if (result.success && result.data) {
                    setCompanyInfo(result.data);
                    console.log('[CompanyInfo] 기업 정보 자동 로드 완료');
                }
            } catch (error) {
                console.log('[CompanyInfo] 저장된 기업 정보 없음');
            }
        });
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

def filter_data_by_date(data, year, month=None, day=None, end_year=None, end_month=None, end_day=None):
    """날짜 조건으로 데이터 필터링"""
    from datetime import datetime, date

    filtered = []
    year = int(year)
    month = int(month) if month else None
    day = int(day) if day else None
    end_year = int(end_year) if end_year else None
    end_month = int(end_month) if end_month else None
    end_day = int(end_day) if end_day else None

    # 범위 모드인 경우
    if end_year:
        # 시작 날짜 결정
        if month and day:
            start_date = date(year, month, day)
        elif month:
            start_date = date(year, month, 1)
        else:
            start_date = date(year, 1, 1)

        # 종료 날짜 결정
        if end_month and end_day:
            end_date = date(end_year, end_month, end_day)
        elif end_month:
            # 해당 월의 마지막 날
            import calendar
            last_day = calendar.monthrange(end_year, end_month)[1]
            end_date = date(end_year, end_month, last_day)
        else:
            end_date = date(end_year, 12, 31)

        for row in data:
            row_date = row.get('접수일자')
            if not row_date:
                continue

            # datetime 또는 date 객체로 변환
            if hasattr(row_date, 'date'):
                row_date = row_date.date()
            elif hasattr(row_date, 'year'):
                row_date = date(row_date.year, row_date.month, row_date.day)
            else:
                try:
                    parts = str(row_date).split('-')
                    row_date = date(int(parts[0]), int(parts[1]), int(parts[2][:2]))
                except:
                    continue

            if start_date <= row_date <= end_date:
                filtered.append(row)
    else:
        # 단일 날짜 모드
        for row in data:
            row_date = row.get('접수일자')
            if not row_date:
                continue

            # 연도 확인
            if hasattr(row_date, 'year'):
                row_year = row_date.year
                row_month = row_date.month
                row_day = row_date.day
            else:
                try:
                    parts = str(row_date).split('-')
                    row_year = int(parts[0])
                    row_month = int(parts[1])
                    row_day = int(parts[2][:2])
                except:
                    continue

            if row_year != year:
                continue

            if month and row_month != month:
                continue

            if day and row_day != day:
                continue

            filtered.append(row)

    return filtered

@app.route('/api/data')
def get_data():
    year = request.args.get('year', '2025')
    month = request.args.get('month', '')
    day = request.args.get('day', '')
    end_year = request.args.get('end_year', '')
    end_month = request.args.get('end_month', '')
    end_day = request.args.get('end_day', '')
    purpose = request.args.get('purpose', '전체')

    # 로그 출력
    date_info = f"year={year}"
    if month: date_info += f", month={month}"
    if day: date_info += f", day={day}"
    if end_year: date_info += f" ~ end_year={end_year}"
    if end_month: date_info += f", end_month={end_month}"
    if end_day: date_info += f", end_day={end_day}"
    print(f"[API] 요청: {date_info}, purpose={purpose}")

    # 기본 데이터 로드 (연도별)
    years_to_load = {year}
    if end_year and end_year != year:
        years_to_load.add(end_year)

    all_data = []
    for y in years_to_load:
        all_data.extend(load_excel_data(y))

    print(f"[API] 로드된 원본 데이터: {len(all_data)}건")

    # 날짜 필터링 적용
    filtered_data = filter_data_by_date(all_data, year, month, day, end_year, end_month, end_day)
    print(f"[API] 날짜 필터링 후 데이터: {len(filtered_data)}건")

    processed = process_data(filtered_data, purpose)
    print(f"[API] 처리 완료: total_count={processed['total_count']}")
    return jsonify(processed)

@app.route('/api/food_item')
def get_food_item_data():
    """검사항목 데이터 API"""
    year = request.args.get('year', '2025')
    purpose = request.args.get('purpose', '전체')
    sample_type = request.args.get('sample_type', '전체')
    item = request.args.get('item', '전체')
    manager = request.args.get('manager', '전체')

    print(f"[API] food_item 요청: year={year}, purpose={purpose}, sample_type={sample_type}, item={item}, manager={manager}")

    # 데이터 로드
    data = load_food_item_data(year)
    print(f"[API] food_item 로드: {len(data)}건")

    # 데이터 처리
    processed = process_food_item_data(
        data,
        purpose_filter=purpose if purpose != '전체' else None,
        sample_type_filter=sample_type if sample_type != '전체' else None,
        item_filter=item if item != '전체' else None,
        manager_filter=manager if manager != '전체' else None
    )

    processed['year'] = int(year)
    print(f"[API] food_item 처리 완료: total_count={processed['total_count']}")
    return jsonify(processed)

@app.route('/api/columns')
def get_columns():
    """Excel 파일의 컬럼명 조회"""
    year = request.args.get('year', '2025')
    from openpyxl import load_workbook

    data_path = DATA_DIR / str(year)
    if not data_path.exists():
        return jsonify({'error': f'{year}년 데이터 폴더가 없습니다.', 'columns': []})

    files = sorted(data_path.glob("*.xlsx"))
    if not files:
        return jsonify({'error': f'{year}년 데이터 파일이 없습니다.', 'columns': []})

    try:
        wb = load_workbook(files[0], read_only=True, data_only=True)
        ws = wb.active
        headers = [cell.value for cell in ws[1] if cell.value]
        wb.close()

        # 주소 관련 컬럼 표시
        address_cols = [h for h in headers if h and any(k in str(h) for k in ['주소', '지역', '시', '도', '군', '구', '동', '장소'])]

        return jsonify({
            'year': year,
            'file': files[0].name,
            'total_columns': len(headers),
            'columns': headers,
            'address_columns': address_cols
        })
    except Exception as e:
        return jsonify({'error': str(e), 'columns': []})

@app.route('/api/cache/refresh')
def refresh_cache():
    """캐시 새로고침"""
    global DATA_CACHE, CACHE_TIME, AI_SUMMARY_CACHE, FILE_MTIME
    DATA_CACHE = {}
    CACHE_TIME = {}
    AI_SUMMARY_CACHE = {}
    FILE_MTIME = {}
    print("[CACHE] 모든 캐시 초기화됨")
    # 데이터 미리 로드
    for year in ['2024', '2025']:
        load_excel_data(year, use_cache=False)
    # AI 요약 캐시도 미리 생성
    get_ai_data_summary(force_refresh=True)
    return jsonify({'status': 'ok', 'message': '캐시가 새로고침되었습니다.'})

# 기업 정보 파일 경로
COMPANY_INFO_FILE = os.path.join(DATA_DIR, 'company_info.json')

@app.route('/api/company-info', methods=['GET'])
def get_company_info():
    """기업 정보 조회"""
    try:
        if os.path.exists(COMPANY_INFO_FILE):
            with open(COMPANY_INFO_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
            print(f"[CompanyInfo] 기업 정보 로드 성공: {data.get('companyName', 'N/A')}")
            return jsonify({'success': True, 'data': data})
        else:
            print("[CompanyInfo] 저장된 기업 정보 없음")
            return jsonify({'success': True, 'data': None})
    except Exception as e:
        print(f"[CompanyInfo] 로드 오류: {e}")
        return jsonify({'success': False, 'error': str(e)})

@app.route('/api/company-info', methods=['POST'])
def save_company_info():
    """기업 정보 저장"""
    try:
        data = request.json
        with open(COMPANY_INFO_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[CompanyInfo] 기업 정보 저장 완료: {data.get('companyName', 'N/A')}")
        return jsonify({'success': True})
    except Exception as e:
        print(f"[CompanyInfo] 저장 오류: {e}")
        return jsonify({'success': False, 'error': str(e)})

def get_company_context():
    """AI 분석용 기업 정보 컨텍스트 생성"""
    try:
        if os.path.exists(COMPANY_INFO_FILE):
            with open(COMPANY_INFO_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # 부서 정보 요약
            dept_summary = []
            total_employees = 0
            if data.get('departments'):
                for dept_name, dept_info in data['departments'].items():
                    count = dept_info.get('count', 0)
                    if count > 0:
                        total_employees += count
                        role = dept_info.get('role', '')
                        dept_summary.append(f"{dept_name}({count}명): {role}")

            # 영업부 인력 요약
            sales_summary = []
            if data.get('salesPersonnel'):
                for person in data['salesPersonnel']:
                    if person.get('name'):
                        sales_summary.append(f"{person['name']}({person.get('region', '')})")

            # 지사 인력 요약
            branch_summary = []
            if data.get('branchPersonnel'):
                for person in data['branchPersonnel']:
                    if person.get('name'):
                        branch_summary.append(f"{person['name']}({person.get('region', '')})")

            context = f"""[기업 정보]
- 기업명: {data.get('companyName', '미입력')}
- 설립연도: {data.get('foundedYear', '미입력')}
- 사업분야: {data.get('businessField', '미입력')}
- 주요서비스: {data.get('mainServices', '미입력')}
- 연간매출목표: {data.get('revenueTarget', '미입력')}억원
- 연간검사목표: {data.get('inspectionTarget', '미입력')}건
- KPI: {data.get('kpiDescription', '미입력')}
- 경영전략: {data.get('businessStrategy', '미입력')}
- 총인원: {total_employees}명
- 조직구성: {'; '.join(dept_summary[:5]) if dept_summary else '미입력'}
- 영업담당자: {', '.join(sales_summary) if sales_summary else '미입력'}
- 지사담당자: {', '.join(branch_summary) if branch_summary else '미입력'}"""
            return context
        return ""
    except Exception as e:
        print(f"[CompanyInfo] 컨텍스트 생성 오류: {e}")
        return ""

@app.route('/api/ai/analyze', methods=['POST'])
def ai_analyze():
    """AI 분석 API - Gemini로 자연어 질문 분석"""
    import urllib.request
    import urllib.error
    import time

    query = request.json.get('query', '')
    print(f"[AI] === 분석 요청 시작 ===")
    print(f"[AI] 질문: {query}")

    if not query:
        print(f"[AI] 오류: 질문 없음")
        return jsonify({'error': '질문을 입력해주세요.'})

    global current_api_key_index
    if not GEMINI_API_KEYS:
        print(f"[AI] 오류: API 키 없음")
        return jsonify({'error': 'GEMINI_API_KEY가 설정되지 않았습니다.'})

    print(f"[AI] 사용 가능한 API 키: {len(GEMINI_API_KEYS)}개")

    # 캐시된 데이터 요약 사용 (변경 감지 포함)
    data_summary = get_ai_data_summary()
    filter_values = data_summary['filter_values']
    print(f"[AI] 캐시된 요약 사용: 목적 {len(filter_values['purposes'])}개, 유형 {len(filter_values['sample_types'])}개")

    # 2025년 주요 통계 요약 (Gemini에 컨텍스트 제공)
    stats_2025 = data_summary['2025']
    top_purposes = sorted(stats_2025['by_purpose'].items(), key=lambda x: x[1]['fee'], reverse=True)[:5]
    top_managers = sorted(stats_2025['by_manager'].items(), key=lambda x: x[1]['fee'], reverse=True)[:5]

    stats_text = f"""2025년 현황:
- 총 건수: {stats_2025['total_count']:,}건
- 총 매출: {stats_2025['total_fee']/100000000:.2f}억원
- TOP 검사목적: {', '.join([f"{p[0]}({p[1]['fee']/10000:.0f}만)" for p in top_purposes])}
- TOP 영업담당: {', '.join([f"{m[0]}({m[1]['fee']/10000:.0f}만)" for m in top_managers])}"""

    # 기업 정보 컨텍스트 추가
    company_context = get_company_context()
    if company_context:
        stats_text = company_context + "\n\n" + stats_text
        print(f"[AI] 기업 정보 컨텍스트 추가됨")

    # 간소화된 Gemini 프롬프트 (토큰 절약)
    system_prompt = f"""데이터 분석 도우미입니다. 질문을 JSON으로 변환하세요.

{stats_text}

가능한 값:
- 연도: 2024, 2025
- 검사목적: {', '.join(filter_values['purposes'][:10])}
- 검체유형: {', '.join(filter_values['sample_types'][:10])}
- 항목명: {', '.join(filter_values['items'][:15])}
- 영업담당: {', '.join(filter_values['managers'][:10])}

분석유형: monthly_trend(월별추이), comparison(비교), top_items(TOP N), summary(요약), direct_answer(직접답변)

JSON 형식만 응답:
{{"analysis_type":"타입","year":"2024|2025","purpose":null,"sample_type":null,"item":null,"manager":null,"top_n":10,"description":"설명","direct_answer":"직접 답변이 가능하면 여기에 작성"}}"""

    print(f"[AI] 프롬프트 길이: {len(system_prompt)}자")

    payload = {
        "contents": [{"parts": [{"text": system_prompt + f"\n\n질문: {query}"}]}],
        "generationConfig": {"temperature": 0.1, "maxOutputTokens": 500}
    }

    # Gemini API 호출 (여러 키로 429 대응)
    total_keys = len(GEMINI_API_KEYS)
    keys_tried = 0

    while keys_tried < total_keys:
        api_key = GEMINI_API_KEYS[current_api_key_index]
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={api_key}"

        print(f"[AI] API 키 {current_api_key_index + 1}/{total_keys} 사용: {api_key[:15]}...")

        try:
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode('utf-8'),
                headers={'Content-Type': 'application/json'},
                method='POST'
            )

            with urllib.request.urlopen(req, timeout=30) as response:
                result = json.loads(response.read().decode('utf-8'))

            print(f"[AI] Gemini API 응답 수신 성공")

            # 라운드 로빈: 성공 후에도 다음 키로 전환 (부하 분산)
            current_api_key_index = (current_api_key_index + 1) % total_keys

            # Gemini 응답에서 JSON 추출
            ai_response = result['candidates'][0]['content']['parts'][0]['text']
            print(f"[AI] Gemini 원본 응답: {ai_response[:200]}...")

            # JSON 파싱 (코드블록 제거)
            json_str = ai_response.strip()
            if '```json' in json_str:
                json_str = json_str.split('```json')[1].split('```')[0]
            elif '```' in json_str:
                json_str = json_str.split('```')[1].split('```')[0]

            parsed = json.loads(json_str.strip())
            print(f"[AI] 파싱 성공: {parsed}")

            # direct_answer 타입이면 바로 응답 반환
            if parsed.get('analysis_type') == 'direct_answer' and parsed.get('direct_answer'):
                print(f"[AI] 직접 답변 반환")
                return jsonify({
                    'success': True,
                    'analysis_type': 'direct_answer',
                    'description': parsed.get('description', ''),
                    'direct_answer': parsed.get('direct_answer'),
                    'parsed_query': parsed
                })

            # 데이터 조회 및 분석 실행 (캐시된 데이터 사용)
            food_2024 = load_food_item_data('2024')
            food_2025 = load_food_item_data('2025')
            data_2024 = load_excel_data('2024')
            data_2025 = load_excel_data('2025')

            analysis_result = execute_analysis(parsed, food_2024, food_2025, data_2024, data_2025)
            analysis_result['parsed_query'] = parsed

            print(f"[AI] 분석 완료: {analysis_result.get('analysis_type')}, 건수: {analysis_result.get('total_count')}")
            return jsonify(analysis_result)

        except urllib.error.HTTPError as e:
            error_body = e.read().decode('utf-8') if e.fp else ''
            print(f"[AI] HTTP 오류 {e.code}: {e.reason}")
            print(f"[AI] 오류 상세: {error_body[:300]}")

            if e.code == 429:  # Too Many Requests - 다음 키로 전환
                keys_tried += 1
                current_api_key_index = (current_api_key_index + 1) % total_keys
                print(f"[AI] 429 오류 - 다음 API 키로 전환 (키 {current_api_key_index + 1})")
                time.sleep(1)  # 짧은 대기 후 다음 키 시도
                continue
            elif e.code == 404:
                return jsonify({'error': f'API 모델을 찾을 수 없습니다 (404). 모델명 확인 필요.'})
            else:
                return jsonify({'error': f'API 오류 {e.code}: {e.reason}'})

        except urllib.error.URLError as e:
            print(f"[AI] URL 오류: {e.reason}")
            return jsonify({'error': f'API 연결 실패: {str(e.reason)}'})

        except json.JSONDecodeError as e:
            print(f"[AI] JSON 파싱 오류: {e}")
            print(f"[AI] 파싱 시도한 문자열: {json_str[:300] if 'json_str' in locals() else 'N/A'}")
            return jsonify({
                'error': f'응답 파싱 실패. Gemini가 올바른 JSON을 반환하지 않았습니다.',
                'raw_response': ai_response[:500] if 'ai_response' in locals() else ''
            })

        except Exception as e:
            import traceback
            print(f"[AI] 예외 발생: {e}")
            print(f"[AI] 트레이스백: {traceback.format_exc()}")
            return jsonify({'error': f'분석 실패: {str(e)}'})

    return jsonify({'error': f'모든 API 키({total_keys}개)가 할당량을 초과했습니다. 잠시 후 다시 시도해주세요.'})


def execute_analysis(params, food_2024, food_2025, data_2024, data_2025):
    """파싱된 조건으로 실제 데이터 분석 실행"""
    analysis_type = params.get('analysis_type', 'summary')
    year = params.get('year', '2025')
    purpose = params.get('purpose')
    sample_type = params.get('sample_type')
    item = params.get('item')
    exclude_item = params.get('exclude_item')
    manager = params.get('manager')
    top_n = params.get('top_n', 10)
    description = params.get('description', '')

    # 연도별 데이터 선택
    food_data = food_2025 if year == '2025' else food_2024

    # 필터링
    filtered = []
    for row in food_data:
        if purpose and str(row.get('검사목적', '')).strip() != purpose:
            continue
        if sample_type and str(row.get('검체유형', '')).strip() != sample_type:
            continue
        if item and str(row.get('항목명', '')).strip() != item:
            continue
        if manager and str(row.get('영업담당', '')).strip() != manager:
            continue
        filtered.append(row)

    # 제외 항목 필터링 (비교 분석용)
    filtered_excluded = []
    if exclude_item:
        for row in filtered:
            if str(row.get('항목명', '')).strip() != exclude_item:
                filtered_excluded.append(row)

    result = {
        'success': True,
        'description': description,
        'analysis_type': analysis_type,
        'total_count': len(filtered),
        'year': year
    }

    if analysis_type == 'monthly_trend':
        # 월별 추이
        monthly = {}
        monthly_excluded = {}
        for row in filtered:
            date = row.get('접수일자')
            if date and hasattr(date, 'month'):
                m = date.month
                fee = row.get('항목수수료', 0) or 0
                if isinstance(fee, str):
                    fee = float(fee.replace(',', '').replace('원', '')) if fee else 0
                monthly[m] = monthly.get(m, 0) + fee

        if exclude_item:
            for row in filtered_excluded:
                date = row.get('접수일자')
                if date and hasattr(date, 'month'):
                    m = date.month
                    fee = row.get('항목수수료', 0) or 0
                    if isinstance(fee, str):
                        fee = float(fee.replace(',', '').replace('원', '')) if fee else 0
                    monthly_excluded[m] = monthly_excluded.get(m, 0) + fee

        result['chart_data'] = {
            'labels': [f'{m}월' for m in range(1, 13)],
            'datasets': [
                {'label': '전체 매출', 'data': [monthly.get(m, 0) for m in range(1, 13)]}
            ]
        }
        if exclude_item:
            result['chart_data']['datasets'].append({
                'label': f'{exclude_item} 제외',
                'data': [monthly_excluded.get(m, 0) for m in range(1, 13)]
            })
            # 차이 계산
            diff_data = []
            for m in range(1, 13):
                diff = monthly.get(m, 0) - monthly_excluded.get(m, 0)
                diff_data.append(diff)
            result['chart_data']['datasets'].append({
                'label': '차이 (감소액)',
                'data': diff_data
            })
            result['total_diff'] = sum(diff_data)

        result['total_fee'] = sum(monthly.values())

    elif analysis_type == 'comparison':
        # 비교 분석
        total_with = sum((row.get('항목수수료', 0) or 0) for row in filtered)
        total_without = sum((row.get('항목수수료', 0) or 0) for row in filtered_excluded) if exclude_item else 0

        result['comparison'] = {
            'with_item': {'count': len(filtered), 'fee': total_with},
            'without_item': {'count': len(filtered_excluded) if exclude_item else 0, 'fee': total_without},
            'difference': {'count': len(filtered) - len(filtered_excluded) if exclude_item else 0,
                          'fee': total_with - total_without}
        }

    elif analysis_type == 'top_items':
        # TOP N 항목
        item_stats = {}
        for row in filtered:
            item_name = str(row.get('항목명', '')).strip()
            if item_name:
                if item_name not in item_stats:
                    item_stats[item_name] = {'count': 0, 'fee': 0}
                item_stats[item_name]['count'] += 1
                fee = row.get('항목수수료', 0) or 0
                if isinstance(fee, str):
                    fee = float(fee.replace(',', '').replace('원', '')) if fee else 0
                item_stats[item_name]['fee'] += fee

        sorted_items = sorted(item_stats.items(), key=lambda x: x[1]['count'], reverse=True)[:top_n]
        result['top_items'] = [{'name': k, 'count': v['count'], 'fee': v['fee']} for k, v in sorted_items]
        result['chart_data'] = {
            'labels': [item[0][:15] for item in sorted_items],
            'datasets': [{'label': '건수', 'data': [item[1]['count'] for item in sorted_items]}]
        }

    else:  # summary
        total_fee = sum((row.get('항목수수료', 0) or 0) for row in filtered)
        result['summary'] = {
            'total_count': len(filtered),
            'total_fee': total_fee,
            'avg_fee': total_fee / len(filtered) if filtered else 0
        }

    return result


@app.route('/api/ai/goal-analysis', methods=['POST'])
def goal_analysis():
    """목표 달성 분석 API - 데이터 기반 종합 분석"""
    try:
        target_revenue = request.json.get('target', 7000000000)  # 기본 70억
        target_year = request.json.get('year', 2026)

        # 필터 옵션 (체크박스 선택)
        filters = request.json.get('filters', {})
        selected_managers = filters.get('managers', [])  # 빈 배열 = 전체
        selected_teams = filters.get('teams', [])
        selected_months = filters.get('months', [])
        selected_purposes = filters.get('purposes', [])
        selected_regions = filters.get('regions', [])
        selected_sample_types = filters.get('sample_types', [])
        selected_items = filters.get('items', [])
        selected_analyzers = filters.get('analyzers', [])

        # 데이터 로드 (메인 Excel 데이터 사용 - 공급가액 기준)
        data_2024 = load_excel_data('2024')
        data_2025 = load_excel_data('2025')

        def get_fee(row):
            """공급가액 추출"""
            fee = row.get('공급가액', 0) or 0
            if isinstance(fee, str):
                fee = float(fee.replace(',', '').replace('원', '')) if fee else 0
            return float(fee)

        def match_filter(row, managers, teams, months, purposes, regions, sample_types, items, analyzers):
            """필터 조건 매칭"""
            # 빈 배열이면 전체 선택으로 처리
            if managers and str(row.get('영업담당', '')).strip() not in managers:
                return False
            if teams:
                manager = str(row.get('영업담당', '')).strip()
                team = MANAGER_TO_BRANCH.get(manager, '기타')
                if team not in teams:
                    return False
            if months:
                date = row.get('접수일자')
                if date and hasattr(date, 'month'):
                    if date.month not in months:
                        return False
            if purposes and str(row.get('검사목적', '')).strip() not in purposes:
                return False
            if regions and str(row.get('지역', '')).strip() not in regions:
                return False
            if sample_types and str(row.get('검체유형', '')).strip() not in sample_types:
                return False
            if items and str(row.get('항목명', '')).strip() not in items:
                return False
            if analyzers and str(row.get('결과입력자', '')).strip() not in analyzers:
                return False
            return True

        # 연도별 매출 계산 (공급가액 기준)
        revenue_2024 = sum(get_fee(row) for row in data_2024 if match_filter(
            row, selected_managers, selected_teams, selected_months, selected_purposes,
            selected_regions, selected_sample_types, selected_items, selected_analyzers))
        revenue_2025 = sum(get_fee(row) for row in data_2025 if match_filter(
            row, selected_managers, selected_teams, selected_months, selected_purposes,
            selected_regions, selected_sample_types, selected_items, selected_analyzers))

        # 성장률 계산
        growth_rate = ((revenue_2025 - revenue_2024) / revenue_2024 * 100) if revenue_2024 > 0 else 0

        # 목표 달성에 필요한 추가 매출
        gap = target_revenue - revenue_2025
        required_growth = ((target_revenue - revenue_2025) / revenue_2025 * 100) if revenue_2025 > 0 else 0

        result = {
            'success': True,
            'target': target_revenue,
            'target_year': target_year,
            'current_status': {
                'revenue_2024': revenue_2024,
                'revenue_2025': revenue_2025,
                'growth_rate': round(growth_rate, 1),
                'gap_to_target': gap,
                'required_growth': round(required_growth, 1)
            },
            'analysis': {},
            'recommendations': []
        }

        # 1. 영업담당별 분석
        by_manager = {}
        for row in data_2025:
            if not match_filter(row, [], selected_teams, selected_months, selected_purposes,
                               selected_regions, selected_sample_types, selected_items, selected_analyzers):
                continue
            manager = str(row.get('영업담당', '') or '').strip() or '미지정'
            revenue = get_fee(row)
            if manager not in by_manager:
                by_manager[manager] = {'revenue_2025': 0, 'count_2025': 0, 'revenue_2024': 0, 'count_2024': 0}
            by_manager[manager]['revenue_2025'] += revenue
            by_manager[manager]['count_2025'] += 1

        for row in data_2024:
            if not match_filter(row, [], selected_teams, selected_months, selected_purposes,
                               selected_regions, selected_sample_types, selected_items, selected_analyzers):
                continue
            manager = str(row.get('영업담당', '') or '').strip() or '미지정'
            revenue = get_fee(row)
            if manager not in by_manager:
                by_manager[manager] = {'revenue_2025': 0, 'count_2025': 0, 'revenue_2024': 0, 'count_2024': 0}
            by_manager[manager]['revenue_2024'] += revenue
            by_manager[manager]['count_2024'] += 1

        # 영업담당별 성장률 계산 (ISA, IBK 등 제외)
        manager_analysis = []
        for manager, data in by_manager.items():
            # 제외 대상 확인
            if manager in EXCLUDED_MANAGERS:
                continue
            if data['revenue_2024'] > 0:
                mgr_growth = ((data['revenue_2025'] - data['revenue_2024']) / data['revenue_2024'] * 100)
            else:
                mgr_growth = 100 if data['revenue_2025'] > 0 else 0
            manager_analysis.append({
                'name': manager,
                'revenue_2024': data['revenue_2024'],
                'revenue_2025': data['revenue_2025'],
                'growth': round(mgr_growth, 1),
                'count_2025': data['count_2025'],
                'potential': data['revenue_2025'] * (required_growth / 100) if mgr_growth < required_growth else 0
            })

        manager_analysis.sort(key=lambda x: x['revenue_2025'], reverse=True)
        result['analysis']['by_manager'] = manager_analysis[:15]

        # 성장률 낮은 영업담당 (개선 필요) - 제외 대상 빼고
        underperforming_managers = [m for m in manager_analysis if m['growth'] < growth_rate and m['revenue_2024'] > 10000000 and m['name'] not in EXCLUDED_MANAGERS]
        underperforming_managers.sort(key=lambda x: x['growth'])

        # 2. 검사목적별 분석
        by_purpose = {}
        for row in data_2025:
            if not match_filter(row, selected_managers, selected_teams, selected_months, [],
                               selected_regions, selected_sample_types, selected_items, selected_analyzers):
                continue
            purpose = str(row.get('검사목적', '') or '').strip() or '미지정'
            revenue = get_fee(row)
            if purpose not in by_purpose:
                by_purpose[purpose] = {'revenue_2025': 0, 'count_2025': 0, 'revenue_2024': 0, 'count_2024': 0}
            by_purpose[purpose]['revenue_2025'] += revenue
            by_purpose[purpose]['count_2025'] += 1

        for row in data_2024:
            if not match_filter(row, selected_managers, selected_teams, selected_months, [],
                               selected_regions, selected_sample_types, selected_items, selected_analyzers):
                continue
            purpose = str(row.get('검사목적', '') or '').strip() or '미지정'
            revenue = get_fee(row)
            if purpose not in by_purpose:
                by_purpose[purpose] = {'revenue_2025': 0, 'count_2025': 0, 'revenue_2024': 0, 'count_2024': 0}
            by_purpose[purpose]['revenue_2024'] += revenue
            by_purpose[purpose]['count_2024'] += 1

        purpose_analysis = []
        for purpose, data in by_purpose.items():
            if data['revenue_2024'] > 0:
                purp_growth = ((data['revenue_2025'] - data['revenue_2024']) / data['revenue_2024'] * 100)
            else:
                purp_growth = 100 if data['revenue_2025'] > 0 else 0
            purpose_analysis.append({
                'name': purpose,
                'revenue_2024': data['revenue_2024'],
                'revenue_2025': data['revenue_2025'],
                'growth': round(purp_growth, 1),
                'count_2025': data['count_2025'],
                'share': round(data['revenue_2025'] / revenue_2025 * 100, 1) if revenue_2025 > 0 else 0
            })

        purpose_analysis.sort(key=lambda x: x['revenue_2025'], reverse=True)
        result['analysis']['by_purpose'] = purpose_analysis[:10]

        # 3. 검체유형별 분석
        by_sample_type = {}
        for row in data_2025:
            if not match_filter(row, selected_managers, selected_teams, selected_months, selected_purposes,
                               selected_regions, [], selected_items, selected_analyzers):
                continue
            sample_type = str(row.get('검체유형', '') or '').strip() or '미지정'
            revenue = get_fee(row)
            if sample_type not in by_sample_type:
                by_sample_type[sample_type] = {'revenue_2025': 0, 'revenue_2024': 0}
            by_sample_type[sample_type]['revenue_2025'] += revenue

        for row in data_2024:
            if not match_filter(row, selected_managers, selected_teams, selected_months, selected_purposes,
                               selected_regions, [], selected_items, selected_analyzers):
                continue
            sample_type = str(row.get('검체유형', '') or '').strip() or '미지정'
            revenue = get_fee(row)
            if sample_type not in by_sample_type:
                by_sample_type[sample_type] = {'revenue_2025': 0, 'revenue_2024': 0}
            by_sample_type[sample_type]['revenue_2024'] += revenue

        sample_analysis = []
        for st, data in by_sample_type.items():
            if data['revenue_2024'] > 0:
                st_growth = ((data['revenue_2025'] - data['revenue_2024']) / data['revenue_2024'] * 100)
            else:
                st_growth = 100 if data['revenue_2025'] > 0 else 0
            sample_analysis.append({
                'name': st,
                'revenue_2024': data['revenue_2024'],
                'revenue_2025': data['revenue_2025'],
                'growth': round(st_growth, 1)
            })

        sample_analysis.sort(key=lambda x: x['revenue_2025'], reverse=True)
        result['analysis']['by_sample_type'] = sample_analysis[:15]

        # 4. 지역별 분석
        by_region = {}
        for row in data_2025:
            if not match_filter(row, selected_managers, selected_teams, selected_months, selected_purposes,
                               [], selected_sample_types, selected_items, selected_analyzers):
                continue
            address = str(row.get('업체주소', '') or '').strip()
            region = extract_sido(address)
            if not region:
                region = '미지정'
            revenue = get_fee(row)
            if region not in by_region:
                by_region[region] = {'revenue_2025': 0, 'revenue_2024': 0, 'count_2025': 0}
            by_region[region]['revenue_2025'] += revenue
            by_region[region]['count_2025'] += 1

        for row in data_2024:
            if not match_filter(row, selected_managers, selected_teams, selected_months, selected_purposes,
                               [], selected_sample_types, selected_items, selected_analyzers):
                continue
            address = str(row.get('업체주소', '') or '').strip()
            region = extract_sido(address)
            if not region:
                region = '미지정'
            revenue = get_fee(row)
            if region not in by_region:
                by_region[region] = {'revenue_2025': 0, 'revenue_2024': 0, 'count_2025': 0}
            by_region[region]['revenue_2024'] += revenue

        region_analysis = []
        for region, data in by_region.items():
            if data['revenue_2024'] > 0:
                reg_growth = ((data['revenue_2025'] - data['revenue_2024']) / data['revenue_2024'] * 100)
            else:
                reg_growth = 100 if data['revenue_2025'] > 0 else 0
            region_analysis.append({
                'name': region,
                'revenue_2024': data['revenue_2024'],
                'revenue_2025': data['revenue_2025'],
                'growth': round(reg_growth, 1),
                'count_2025': data['count_2025']
            })

        region_analysis.sort(key=lambda x: x['revenue_2025'], reverse=True)
        result['analysis']['by_region'] = region_analysis

        # 5. 항목별 분석 (food_item 데이터)
        by_item = {}
        for row in data_2025:
            if not match_filter(row, selected_managers, selected_teams, selected_months, selected_purposes,
                               selected_regions, selected_sample_types, [], selected_analyzers):
                continue
            item = str(row.get('항목명', '') or '').strip()
            if not item:
                continue
            fee = get_fee(row)
            if item not in by_item:
                by_item[item] = {'fee_2025': 0, 'count_2025': 0, 'fee_2024': 0, 'count_2024': 0}
            by_item[item]['fee_2025'] += fee
            by_item[item]['count_2025'] += 1

        for row in data_2024:
            if not match_filter(row, selected_managers, selected_teams, selected_months, selected_purposes,
                               selected_regions, selected_sample_types, [], selected_analyzers):
                continue
            item = str(row.get('항목명', '') or '').strip()
            if not item:
                continue
            fee = get_fee(row)
            if item not in by_item:
                by_item[item] = {'fee_2025': 0, 'count_2025': 0, 'fee_2024': 0, 'count_2024': 0}
            by_item[item]['fee_2024'] += fee
            by_item[item]['count_2024'] += 1

        item_analysis = []
        for item, data in by_item.items():
            if data['fee_2024'] > 0:
                item_growth = ((data['fee_2025'] - data['fee_2024']) / data['fee_2024'] * 100)
            else:
                item_growth = 100 if data['fee_2025'] > 0 else 0
            item_analysis.append({
                'name': item,
                'fee_2024': data['fee_2024'],
                'fee_2025': data['fee_2025'],
                'growth': round(item_growth, 1),
                'count_2025': data['count_2025']
            })

        item_analysis.sort(key=lambda x: x['fee_2025'], reverse=True)
        result['analysis']['by_item'] = item_analysis[:20]

        # 감소 항목 (위험 요소)
        declining_items = [i for i in item_analysis if i['growth'] < 0 and i['fee_2024'] > 5000000]
        declining_items.sort(key=lambda x: x['growth'])

        # ===== 추천사항 생성 =====
        recommendations = []

        # 1. 전체 목표 분석
        recommendations.append({
            'category': '📊 목표 분석',
            'title': f'{target_year}년 {target_revenue/100000000:.0f}억 달성 가능성',
            'content': f'현재 추세(연 {growth_rate:.1f}% 성장) 유지 시 {target_year}년 예상 매출: {revenue_2025 * (1 + growth_rate/100)/100000000:.1f}억원',
            'action': f'목표 달성을 위해 추가 {gap/100000000:.1f}억원 ({required_growth:.1f}% 성장) 필요',
            'priority': 'high' if required_growth > growth_rate * 1.5 else 'medium'
        })

        # 2. 영업담당 개선
        if underperforming_managers:
            top_under = underperforming_managers[:3]
            potential_gain = sum(m['potential'] for m in top_under)
            recommendations.append({
                'category': '👤 영업담당',
                'title': '성장률 개선 필요 담당자',
                'content': ', '.join([f"{m['name']}({m['growth']:+.1f}%)" for m in top_under]),
                'action': f'이 담당자들이 평균 성장률 달성 시 약 {potential_gain/10000:.0f}만원 추가 가능',
                'evidence': [{'name': m['name'], 'current': m['revenue_2025'], 'growth': m['growth']} for m in top_under],
                'priority': 'high'
            })

        # 3. 고성장 영업담당 (롤모델)
        high_growth_managers = [m for m in manager_analysis if m['growth'] > growth_rate * 1.5 and m['revenue_2025'] > 50000000]
        if high_growth_managers:
            recommendations.append({
                'category': '⭐ 우수 사례',
                'title': '고성장 영업담당 (벤치마킹 대상)',
                'content': ', '.join([f"{m['name']}({m['growth']:+.1f}%)" for m in high_growth_managers[:3]]),
                'action': '이들의 영업 전략 분석 및 공유 권장',
                'priority': 'medium'
            })

        # 4. 검사목적별 기회
        growing_purposes = [p for p in purpose_analysis if p['growth'] > 10 and p['revenue_2025'] > 100000000]
        if growing_purposes:
            recommendations.append({
                'category': '🎯 검사목적',
                'title': '성장 중인 검사목적 (집중 공략)',
                'content': ', '.join([f"{p['name']}({p['growth']:+.1f}%)" for p in growing_purposes[:3]]),
                'action': '이 분야 마케팅 강화 및 전문성 확보',
                'evidence': growing_purposes[:3],
                'priority': 'high'
            })

        # 5. 감소 항목 경고
        if declining_items:
            total_decline = sum(abs(i['fee_2025'] - i['fee_2024']) for i in declining_items[:5])
            recommendations.append({
                'category': '⚠️ 위험 요소',
                'title': '매출 감소 항목',
                'content': ', '.join([f"{i['name']}({i['growth']:.1f}%)" for i in declining_items[:5]]),
                'action': f'감소 원인 분석 필요 (총 감소액: {total_decline/10000:.0f}만원)',
                'evidence': declining_items[:5],
                'priority': 'high'
            })

        # 6. 지역별 기회
        growing_regions = [r for r in region_analysis if r['growth'] > growth_rate and r['revenue_2025'] > 50000000]
        weak_regions = [r for r in region_analysis if r['growth'] < 0 and r['revenue_2024'] > 50000000]

        if growing_regions:
            recommendations.append({
                'category': '📍 지역',
                'title': '성장 지역 (확대 공략)',
                'content': ', '.join([f"{r['name']}({r['growth']:+.1f}%)" for r in growing_regions[:5]]),
                'action': '해당 지역 영업 인력/마케팅 확대 검토',
                'priority': 'medium'
            })

        if weak_regions:
            recommendations.append({
                'category': '📍 지역',
                'title': '감소 지역 (원인 분석 필요)',
                'content': ', '.join([f"{r['name']}({r['growth']:.1f}%)" for r in weak_regions[:5]]),
                'action': '경쟁사 동향 및 고객 이탈 원인 파악',
                'priority': 'medium'
            })

        # 7. 실행 계획 제안
        monthly_target = gap / 12 if gap > 0 else 0
        active_managers = len([m for m in manager_analysis if m['revenue_2025'] > 0])
        per_manager_target = (monthly_target / active_managers / 10000) if active_managers > 0 else 0
        recommendations.append({
            'category': '📋 실행 계획',
            'title': '월별 추가 목표',
            'content': f'목표 달성을 위해 월 평균 {monthly_target/10000:.0f}만원 추가 매출 필요',
            'action': f'영업담당 1인당 월 {per_manager_target:.0f}만원 추가 목표 설정 ({active_managers}명 기준)',
            'priority': 'high'
        })

        result['recommendations'] = recommendations

        # 필터 옵션 추가 (선택 가능한 값들)
        all_managers = set()
        all_purposes = set()
        all_sample_types = set()
        all_items = set()
        all_analyzers = set()
        all_regions = set()

        for row in data_2025:
            if row.get('영업담당'): all_managers.add(str(row.get('영업담당')).strip())
            if row.get('검사목적'): all_purposes.add(str(row.get('검사목적')).strip())
            if row.get('검체유형'): all_sample_types.add(str(row.get('검체유형')).strip())
            if row.get('항목명'): all_items.add(str(row.get('항목명')).strip())
            if row.get('결과입력자'): all_analyzers.add(str(row.get('결과입력자')).strip())
            address = str(row.get('업체주소', '') or '').strip()
            region = extract_sido(address)
            if region: all_regions.add(region)

        # 팀 목록 생성
        teams = set(MANAGER_TO_BRANCH.values())

        result['filter_options'] = {
            'managers': sorted([m for m in all_managers if m not in EXCLUDED_MANAGERS]),  # ISA, IBK 등 제외
            'teams': sorted(teams),
            'months': list(range(1, 13)),
            'purposes': sorted(all_purposes),
            'regions': sorted(all_regions),
            'sample_types': sorted(all_sample_types),
            'items': sorted(all_items)[:100],  # 상위 100개만
            'analyzers': sorted(all_analyzers)
        }

        # 적용된 필터 정보
        result['applied_filters'] = {
            'managers': selected_managers,
            'teams': selected_teams,
            'months': selected_months,
            'purposes': selected_purposes,
            'regions': selected_regions,
            'sample_types': selected_sample_types,
            'items': selected_items,
            'analyzers': selected_analyzers
        }

        return jsonify(result)

    except Exception as e:
        import traceback
        return jsonify({'error': str(e), 'traceback': traceback.format_exc()})


def extract_sido(address):
    """주소에서 시/도 추출"""
    if not address:
        return None
    sido_patterns = ['서울', '부산', '대구', '인천', '광주', '대전', '울산', '세종',
                    '경기', '강원', '충북', '충남', '전북', '전남', '경북', '경남', '제주']
    for pattern in sido_patterns:
        if pattern in address:
            return pattern
    return None


def preload_data():
    """서버 시작 시 데이터 미리 로드 (SQLite 우선)"""
    import time
    start_time = time.time()

    # 1. SQLite 모드인 경우
    if USE_SQLITE:
        print("[PRELOAD] SQLite 모드로 시작...")

        # SQLite DB 업데이트 필요 여부 확인
        if check_sqlite_needs_update():
            print("[PRELOAD] SQLite DB 업데이트 필요 - Excel 변환 시작...")
            convert_excel_to_sqlite()
        else:
            print("[PRELOAD] SQLite DB 최신 상태 유지")

        # SQLite에서 빠르게 로드
        for year in ['2024', '2025']:
            load_excel_data(year)
            load_food_item_data(year)

        # AI 요약 캐시 생성
        get_ai_data_summary(force_refresh=True)

        elapsed = time.time() - start_time
        print(f"[PRELOAD] SQLite 로드 완료! ({elapsed:.1f}초)")
        return

    # 2. 기존 방식: 파일 캐시에서 로드 시도
    if load_cache_from_file():
        elapsed = time.time() - start_time
        print(f"[PRELOAD] 파일 캐시에서 로드 완료! ({elapsed:.1f}초)")
        return

    # 3. 파일 캐시가 없거나 무효 -> Excel에서 로드
    print("[PRELOAD] Excel에서 데이터 로드 시작...")
    for year in ['2024', '2025']:
        load_excel_data(year)
        load_food_item_data(year)

    # 4. AI 요약 캐시도 미리 생성
    get_ai_data_summary(force_refresh=True)

    # 5. 파일로 캐시 저장
    save_cache_to_file()

    elapsed = time.time() - start_time
    print(f"[PRELOAD] 완료! ({elapsed:.1f}초)")


if __name__ == '__main__':
    # 서버 시작 시 데이터 미리 로드
    preload_data()
    app.run(host='0.0.0.0', port=6001, debug=False)
