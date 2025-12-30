"""
경영지표 대시보드 (Flask 버전)
- 오래된 CPU에서도 작동
- Chart.js 사용
- 연도 비교, 검사목적 필터, 업체별 분석, 부적합항목 분석
"""
from flask import Flask, render_template_string, jsonify, request
import os
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

# 경로 설정 - 절대 경로 사용
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = Path("/home/biofl/business_metrics/data")

# 데이터 캐시 (메모리에 저장)
DATA_CACHE = {}
CACHE_TIME = {}

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

def load_excel_data(year, use_cache=True):
    """openpyxl로 직접 엑셀 로드 (캐시 사용)"""
    import time
    from openpyxl import load_workbook

    # 캐시 확인 (1시간 유효)
    cache_key = str(year)
    if use_cache and cache_key in DATA_CACHE:
        cache_age = time.time() - CACHE_TIME.get(cache_key, 0)
        if cache_age < 3600:  # 1시간
            print(f"[CACHE] {year}년 데이터 캐시 사용 ({len(DATA_CACHE[cache_key])}건)")
            return DATA_CACHE[cache_key]

    data_path = DATA_DIR / str(year)
    if not data_path.exists():
        return []

    print(f"[LOAD] {year}년 데이터 로딩 시작...")
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

    <script>
        let charts = {};
        let currentData = null;
        let compareData = null;

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
                thead.innerHTML = `<tr><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>비중</th></tr>`;
                const compareMap = Object.fromEntries(compareData.by_manager);
                tbody.innerHTML = currentData.by_manager.map(d => {
                    const compSales = compareMap[d[0]]?.sales || 0;
                    const compCount = compareMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${(d[1].sales / currentData.total_sales * 100).toFixed(1)}%</td></tr>`;
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
                thead.innerHTML = `<tr><th>지사/센터</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th></tr>`;
                const compareMap = Object.fromEntries(compareData.by_branch);
                tbody.innerHTML = currentData.by_branch.map(d => {
                    const compSales = compareMap[d[0]]?.sales || 0;
                    const compCount = compareMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td></tr>`;
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
                topThead.innerHTML = `<tr><th>순위</th><th>거래처</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th></tr>`;
                topTbody.innerHTML = clientData.map((d, i) => {
                    const compSales = compareClientMap[d[0]]?.sales || 0;
                    const compCount = compareClientMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    return `<tr><td>${i+1}</td><td>${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td></tr>`;
                }).join('') || '<tr><td colspan="7">데이터 없음</td></tr>';
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
                effThead.innerHTML = `<tr><th>거래처</th><th>평균단가</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th></tr>`;
                effTbody.innerHTML = effData.map(d => {
                    const compSales = compareClientMap[d[0]]?.sales || 0;
                    const compCount = compareClientMap[d[0]]?.count || 0;
                    const diff = d[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${formatCurrency(d[1].avg)}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td></tr>`;
                }).join('') || '<tr><td colspan="7">데이터 없음</td></tr>';
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
                volThead.innerHTML = `<tr><th>거래처</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>증감</th><th>${currentData.year}년 매출</th><th>${compareData.year}년 매출</th></tr>`;
                volTbody.innerHTML = volData.map(d => {
                    const compCount = compareClientMap[d[0]]?.count || 0;
                    const compSales = compareClientMap[d[0]]?.sales || 0;
                    const diff = d[1].count - compCount;
                    const diffRate = compCount > 0 ? ((diff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    return `<tr><td>${d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${diff.toLocaleString()} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compSales)}</td></tr>`;
                }).join('') || '<tr><td colspan="6">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>부적합항목</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>증감</th><th>비중</th></tr>`;
                tbody.innerHTML = defectData.map((d, i) => {
                    const compCount = compareMap[d[0]]?.count || 0;
                    const diff = d[1].count - compCount;
                    const diffRate = compCount > 0 ? ((diff / compCount) * 100).toFixed(1) : (d[1].count > 0 ? 100 : 0);
                    const diffText = diff >= 0 ? `<span class="positive">+${diff.toLocaleString()} (${'+' + diffRate}%)</span>` : `<span class="negative">${diff.toLocaleString()} (${diffRate}%)</span>`;
                    return `<tr><td>${i+1}</td><td>${d[0]}</td><td>${d[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${diffText}</td><td>${(d[1].count / totalDefects * 100).toFixed(1)}%</td></tr>`;
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
                thead.innerHTML = `<tr><th>순위</th><th style="white-space:nowrap">지역</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th></tr>`;
                const compareMap = Object.fromEntries(compareRegionData);

                tbody.innerHTML = regionData.map((d, i) => {
                    const compData = compareMap[d[0]] || {sales: 0, count: 0};
                    const diff = formatDiff(d[1].sales, compData.sales);
                    const diffRate = compData.sales > 0 ? ((diff.diff / compData.sales) * 100).toFixed(1) : (d[1].sales > 0 ? 100 : 0);
                    const diffClass = diff.diff >= 0 ? 'positive' : 'negative';
                    const diffText = diff.text ? `<span class="${diffClass}">${diff.text} (${diff.diff >= 0 ? '+' : ''}${diffRate}%)</span>` : '-';
                    return `<tr><td>${i+1}</td><td style="white-space:nowrap">${d[0]}</td><td>${formatCurrency(d[1].sales)}</td><td>${formatCurrency(compData.sales)}</td><td>${diffText}</td><td>${d[1].count.toLocaleString()}</td><td>${compData.count.toLocaleString()}</td></tr>`;
                }).join('') || '<tr><td colspan="7">지역 데이터 없음</td></tr>';
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

                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th></tr>`;
                tbody.innerHTML = managers.map((m, i) => {
                    const compData = compareMap[m.name] || {sales: 0, count: 0};
                    const diff = formatDiff(m.sales, compData.sales);
                    const diffRate = compData.sales > 0 ? ((diff.diff / compData.sales) * 100).toFixed(1) : (m.sales > 0 ? 100 : 0);
                    const diffClass = diff.diff >= 0 ? 'positive' : 'negative';
                    const diffText = diff.text ? `<span class="${diffClass}">${diff.text} (${diff.diff >= 0 ? '+' : ''}${diffRate}%)</span>` : '-';
                    return `<tr><td>${i+1}</td><td>${m.name}</td><td>${formatCurrency(m.sales)}</td><td>${formatCurrency(compData.sales)}</td><td>${diffText}</td><td>${m.count.toLocaleString()}</td><td>${compData.count.toLocaleString()}</td></tr>`;
                }).join('') || '<tr><td colspan="7">데이터 없음</td></tr>';
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

                thead.innerHTML = `<tr><th>순위</th><th>지역</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th></tr>`;
                tbody.innerHTML = regions.map((r, i) => {
                    const compData = compareMap[r.region] || {sales: 0, count: 0};
                    const diff = formatDiff(r.sales, compData.sales);
                    const diffRate = compData.sales > 0 ? ((diff.diff / compData.sales) * 100).toFixed(1) : (r.sales > 0 ? 100 : 0);
                    const diffClass = diff.diff >= 0 ? 'positive' : 'negative';
                    const diffText = diff.text ? `<span class="${diffClass}">${diff.text} (${diff.diff >= 0 ? '+' : ''}${diffRate}%)</span>` : '-';
                    return `<tr><td>${i+1}</td><td>${r.region}</td><td>${formatCurrency(r.sales)}</td><td>${formatCurrency(compData.sales)}</td><td>${diffText}</td><td>${r.count.toLocaleString()}</td><td>${compData.count.toLocaleString()}</td></tr>`;
                }).join('') || '<tr><td colspan="7">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>검사목적</th><th>${currLabel}</th><th>${compareData.dateLabel || compareData.year + '년'}</th><th>증감</th><th>${currLabel} 건수</th><th>${compareData.dateLabel || compareData.year + '년'} 건수</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedPurposes.map((p, i) => {
                    const compSales = comparePurposeData[p[0]]?.sales || 0;
                    const compCount = comparePurposeData[p[0]]?.count || 0;
                    const diff = p[1].sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (p[1].sales > 0 ? 100 : 0);
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    const diffText = `<span class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</span>`;
                    const ratio = totalSales > 0 ? (p[1].sales / totalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${p[0]}</td><td>${formatCurrency(p[1].sales)}</td><td>${formatCurrency(compSales)}</td><td>${diffText}</td><td>${p[1].count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>${currLabel}</th><th>${compLabel}</th><th>증감</th><th>${currLabel} 건수</th><th>${compLabel} 건수</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedManagers.map(([name, data], i) => {
                    const compSales = compareManagerData[name]?.sales || 0;
                    const compCount = compareManagerData[name]?.count || 0;
                    const diff = data.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (data.sales > 0 ? 100 : 0);
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    const diffText = `<span class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</span>`;
                    const ratio = managerTotalSales > 0 ? (data.sales / managerTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(data.sales)}</td><td>${formatCurrency(compSales)}</td><td>${diffText}</td><td>${data.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>지역</th><th>${currLabel}</th><th>${compLabel}</th><th>증감</th><th>${currLabel} 건수</th><th>${compLabel} 건수</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedRegions.map(([region, data], i) => {
                    const compSales = compareRegionData[region]?.sales || 0;
                    const compCount = compareRegionData[region]?.count || 0;
                    const diff = data.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (data.sales > 0 ? 100 : 0);
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    const diffText = `<span class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</span>`;
                    const ratio = regionTotalSales > 0 ? (data.sales / regionTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${region}</td><td>${formatCurrency(data.sales)}</td><td>${formatCurrency(compSales)}</td><td>${diffText}</td><td>${data.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${ratio}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>검체유형</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedData.map(([st, d], i) => {
                    const compSales = compareSampleTypeData[st]?.sales || 0;
                    const compCount = compareSampleTypeData[st]?.count || 0;
                    const diff = d.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d.sales > 0 ? 100 : 0);
                    const percent = totalSales > 0 ? (d.sales / totalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${st}</td><td>${formatCurrency(d.sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedManagers.map(([name, d], i) => {
                    const compSales = compareManagerData[name]?.sales || 0;
                    const compCount = compareManagerData[name]?.count || 0;
                    const diff = d.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d.sales > 0 ? 100 : 0);
                    const percent = managerTotalSales > 0 ? (d.sales / managerTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(d.sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
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
                thead.innerHTML = `<tr><th>순위</th><th>검사목적</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>${currentData.year}년 건수</th><th>${compareData.year}년 건수</th><th>비중</th></tr>`;
                tbody.innerHTML = sortedPurposes.map(([name, d], i) => {
                    const compSales = comparePurposeData[name]?.sales || 0;
                    const compCount = comparePurposeData[name]?.count || 0;
                    const diff = d.sales - compSales;
                    const diffRate = compSales > 0 ? ((diff / compSales) * 100).toFixed(1) : (d.sales > 0 ? 100 : 0);
                    const percent = purposeTotalSales > 0 ? (d.sales / purposeTotalSales * 100).toFixed(1) : 0;
                    return `<tr><td>${i+1}</td><td>${name}</td><td>${formatCurrency(d.sales)}</td><td>${formatCurrency(compSales)}</td><td class="${diff >= 0 ? 'positive' : 'negative'}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)} (${diff >= 0 ? '+' : ''}${diffRate}%)</td><td>${d.count.toLocaleString()}</td><td>${compCount.toLocaleString()}</td><td>${percent}%</td></tr>`;
                }).join('') || '<tr><td colspan="8">데이터 없음</td></tr>';
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

        // 페이지 로드 시 초기화
        initDateSelectors();
        showToast('조회 조건을 선택하고 [조회하기] 버튼을 클릭하세요.', 'loading', 5000);
        setTimeout(() => hideToast(), 5000);
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
    global DATA_CACHE, CACHE_TIME
    DATA_CACHE = {}
    CACHE_TIME = {}
    print("[CACHE] 캐시 초기화됨")
    # 데이터 미리 로드
    for year in ['2024', '2025']:
        load_excel_data(year, use_cache=False)
    return jsonify({'status': 'ok', 'message': '캐시가 새로고침되었습니다.'})

def preload_data():
    """서버 시작 시 데이터 미리 로드"""
    print("[PRELOAD] 데이터 미리 로드 시작...")
    for year in ['2024', '2025']:
        load_excel_data(year)
    print("[PRELOAD] 완료!")

if __name__ == '__main__':
    # 서버 시작 시 데이터 미리 로드
    preload_data()
    app.run(host='0.0.0.0', port=6001, debug=False)
