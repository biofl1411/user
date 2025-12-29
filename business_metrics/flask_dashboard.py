"""
경영지표 대시보드 (Flask 버전)
- 오래된 CPU에서도 작동
- Chart.js 사용
- 연도 비교 기능
"""
from flask import Flask, render_template_string, jsonify, request
import os
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

# 경로 설정 - 절대 경로 사용
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"

# 설정
MANAGER_TO_BRANCH = {
    "장동욱": "충청지사", "지병훈": "충청지사", "박은태": "충청지사",
    "도준구": "경북지사",
    "이강현": "전북지사",
    "엄은정": "경기지사", "정유경": "경기지사",
    "이성복": "서울지사",
    "조봉현": "서울센터", "오세중": "서울센터", "장동주": "서울센터", "오석현": "서울센터",
    "엄상흠": "경북센터",
}

def load_excel_data(year):
    """openpyxl로 직접 엑셀 로드 (pandas 없이)"""
    from openpyxl import load_workbook

    data_path = DATA_DIR / str(year)
    print(f"[DEBUG] Looking for data in: {data_path}")
    print(f"[DEBUG] Path exists: {data_path.exists()}")

    if not data_path.exists():
        print(f"[DEBUG] Data path does not exist!")
        return []

    all_data = []
    files = sorted(data_path.glob("*.xlsx"))
    print(f"[DEBUG] Found {len(files)} Excel files")

    for f in files:
        try:
            print(f"[DEBUG] Loading: {f.name}")
            wb = load_workbook(f, read_only=True, data_only=True)
            ws = wb.active

            headers = [cell.value for cell in ws[1]]
            print(f"[DEBUG] Headers: {headers[:5]}...")

            row_count = 0
            for row in ws.iter_rows(min_row=2, values_only=True):
                row_dict = dict(zip(headers, row))
                all_data.append(row_dict)
                row_count += 1

            print(f"[DEBUG] Loaded {row_count} rows from {f.name}")
            wb.close()
        except Exception as e:
            print(f"[ERROR] Loading {f}: {e}")

    print(f"[DEBUG] Total loaded: {len(all_data)} records")
    return all_data

def process_data(data):
    """데이터 처리"""
    by_manager = {}
    by_branch = {}
    by_month = {}
    total_sales = 0
    total_count = 0

    for row in data:
        manager = row.get('영업담당', '미지정')
        sales = row.get('수수료', 0) or 0
        date = row.get('접수일자')

        if isinstance(sales, str):
            sales = float(sales.replace(',', '').replace('원', '')) if sales else 0

        # 매니저별
        if manager not in by_manager:
            by_manager[manager] = {'sales': 0, 'count': 0}
        by_manager[manager]['sales'] += sales
        by_manager[manager]['count'] += 1

        # 지사별
        branch = MANAGER_TO_BRANCH.get(manager, '기타')
        if branch not in by_branch:
            by_branch[branch] = {'sales': 0, 'count': 0, 'managers': set()}
        by_branch[branch]['sales'] += sales
        by_branch[branch]['count'] += 1
        by_branch[branch]['managers'].add(manager)

        # 월별
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

        total_sales += sales
        total_count += 1

    # 정렬
    sorted_managers = sorted(by_manager.items(), key=lambda x: x[1]['sales'], reverse=True)
    sorted_branches = sorted(by_branch.items(), key=lambda x: x[1]['sales'], reverse=True)

    return {
        'by_manager': sorted_managers,
        'by_branch': [(k, {'sales': v['sales'], 'count': v['count'], 'managers': len(v['managers'])})
                      for k, v in sorted_branches],
        'by_month': sorted(by_month.items()),
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
        body {
            font-family: 'Malgun Gothic', sans-serif;
            background: #f5f7fa;
            padding: 20px;
        }
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 20px;
        }
        .header h1 { font-size: 24px; }
        .controls {
            display: flex;
            gap: 10px;
            margin: 15px 0;
            flex-wrap: wrap;
            align-items: center;
        }
        .controls select, .controls label {
            padding: 8px 15px;
            border-radius: 5px;
            border: 1px solid #ddd;
            font-size: 14px;
        }
        .compare-box {
            display: flex;
            align-items: center;
            gap: 8px;
            background: rgba(255,255,255,0.2);
            padding: 8px 15px;
            border-radius: 5px;
            margin-left: 10px;
        }
        .compare-box input[type="checkbox"] {
            width: 18px;
            height: 18px;
            cursor: pointer;
        }
        .compare-box label {
            color: white;
            cursor: pointer;
            padding: 0;
            border: none;
            background: none;
        }
        .compare-box select {
            padding: 5px 10px;
        }
        .summary {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 20px;
        }
        .card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .card h3 { color: #666; font-size: 14px; margin-bottom: 10px; }
        .card .value { font-size: 28px; font-weight: bold; color: #333; }
        .card .sub { color: #888; font-size: 12px; margin-top: 5px; }
        .card .compare-value {
            font-size: 14px;
            color: #764ba2;
            margin-top: 5px;
            padding-top: 5px;
            border-top: 1px dashed #ddd;
        }
        .card .diff { font-size: 12px; margin-top: 3px; }
        .card .diff.positive { color: #2ecc71; }
        .card .diff.negative { color: #e74c3c; }
        .charts {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
            gap: 20px;
        }
        .chart-container {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }
        .chart-container h3 { margin-bottom: 15px; color: #333; }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; }
        th, td { padding: 10px; text-align: left; border-bottom: 1px solid #eee; }
        th { background: #f8f9fa; font-weight: 600; }
        tr:hover { background: #f8f9fa; }
        .tabs { display: flex; gap: 10px; margin-bottom: 20px; }
        .tab {
            padding: 10px 20px;
            background: white;
            border: none;
            border-radius: 5px;
            cursor: pointer;
            font-size: 14px;
        }
        .tab.active { background: #667eea; color: white; }
        .tab-content { display: none; }
        .tab-content.active { display: block; }
        .loading { text-align: center; padding: 50px; color: #666; }
        .btn-search {
            padding: 8px 20px;
            background: #fff;
            color: #667eea;
            border: 2px solid #fff;
            border-radius: 5px;
            font-size: 14px;
            font-weight: bold;
            cursor: pointer;
            transition: all 0.3s;
        }
        .btn-search:hover { background: rgba(255,255,255,0.9); }
        .btn-search:disabled { opacity: 0.6; cursor: not-allowed; }
        .toast {
            position: fixed;
            top: 20px;
            right: 20px;
            padding: 15px 25px;
            background: #2ecc71;
            color: white;
            border-radius: 8px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            z-index: 1000;
            display: none;
            animation: slideIn 0.3s ease;
        }
        .toast.error { background: #e74c3c; }
        .toast.loading { background: #3498db; }
        @keyframes slideIn {
            from { transform: translateX(100%); opacity: 0; }
            to { transform: translateX(0); opacity: 1; }
        }
        .legend-custom {
            display: flex;
            gap: 20px;
            margin-bottom: 10px;
            font-size: 13px;
        }
        .legend-item {
            display: flex;
            align-items: center;
            gap: 5px;
        }
        .legend-color {
            width: 12px;
            height: 12px;
            border-radius: 2px;
        }
    </style>
</head>
<body>
    <div id="toast" class="toast"></div>
    <div class="header">
        <h1>📊 경영지표 대시보드</h1>
        <div class="controls">
            <select id="yearSelect">
                <option value="2025">2025년</option>
                <option value="2024">2024년</option>
            </select>
            <div class="compare-box">
                <input type="checkbox" id="compareCheck" onchange="toggleCompare()">
                <label for="compareCheck">비교</label>
                <select id="compareYearSelect" disabled>
                    <option value="2024">2024년</option>
                    <option value="2025">2025년</option>
                </select>
            </div>
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
        <button class="tab active" onclick="showTab('personal')">👤 개인별 실적</button>
        <button class="tab" onclick="showTab('team')">🏢 팀별 실적</button>
        <button class="tab" onclick="showTab('monthly')">📅 월별 추이</button>
    </div>

    <div id="personal" class="tab-content active">
        <div class="charts">
            <div class="chart-container">
                <h3>영업담당별 매출 TOP 15</h3>
                <div id="managerLegend" class="legend-custom" style="display:none;"></div>
                <canvas id="managerChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>영업담당별 상세</h3>
                <table id="managerTable">
                    <thead id="managerTableHead"><tr><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>

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

    <div id="monthly" class="tab-content">
        <div class="charts">
            <div class="chart-container" style="grid-column: 1/-1;">
                <h3>월별 매출 추이</h3>
                <div id="monthlyLegend" class="legend-custom" style="display:none;"></div>
                <canvas id="monthlyChart"></canvas>
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
            if (type !== 'loading') {
                setTimeout(() => { toast.style.display = 'none'; }, duration);
            }
        }

        function hideToast() {
            document.getElementById('toast').style.display = 'none';
        }

        function toggleCompare() {
            const checked = document.getElementById('compareCheck').checked;
            document.getElementById('compareYearSelect').disabled = !checked;
        }

        function showTab(tabId) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelector(`[onclick="showTab('${tabId}')"]`).classList.add('active');
            document.getElementById(tabId).classList.add('active');
        }

        async function loadData() {
            const year = document.getElementById('yearSelect').value;
            const compareEnabled = document.getElementById('compareCheck').checked;
            const compareYear = document.getElementById('compareYearSelect').value;
            const btn = document.getElementById('btnSearch');

            btn.disabled = true;
            btn.textContent = '로딩중...';
            showToast('데이터를 불러오는 중입니다...', 'loading');

            try {
                const response = await fetch(`/api/data?year=${year}`);
                currentData = await response.json();
                currentData.year = year;

                if (compareEnabled && compareYear !== year) {
                    const compareResponse = await fetch(`/api/data?year=${compareYear}`);
                    compareData = await compareResponse.json();
                    compareData.year = compareYear;
                } else {
                    compareData = null;
                }

                updateSummary();
                updateManagerChart();
                updateBranchChart();
                updateMonthlyChart();
                updateManagerTable();
                updateBranchTable();

                let msg = `${year}년 데이터 로드가 완료되었습니다. (${currentData.total_count.toLocaleString()}건)`;
                if (compareData) {
                    msg = `${year}년 vs ${compareYear}년 비교 데이터 로드 완료`;
                }
                showToast(msg, 'success');

            } catch (error) {
                console.error('Error loading data:', error);
                showToast('데이터 로드 중 오류가 발생했습니다.', 'error');
            } finally {
                btn.disabled = false;
                btn.textContent = '조회하기';
            }
        }

        function updateSummary() {
            document.getElementById('totalSales').textContent = formatCurrency(currentData.total_sales);
            document.getElementById('totalCount').textContent = currentData.total_count.toLocaleString() + '건';
            const avgPrice = currentData.total_count > 0 ? currentData.total_sales / currentData.total_count : 0;
            document.getElementById('avgPrice').textContent = formatCurrency(avgPrice);

            if (compareData) {
                const compAvg = compareData.total_count > 0 ? compareData.total_sales / compareData.total_count : 0;

                document.getElementById('compareTotalSales').textContent = `${compareData.year}년: ${formatCurrency(compareData.total_sales)}`;
                document.getElementById('compareTotalSales').style.display = 'block';
                const salesDiff = formatDiff(currentData.total_sales, compareData.total_sales);
                document.getElementById('diffTotalSales').textContent = salesDiff.text;
                document.getElementById('diffTotalSales').className = 'diff ' + (salesDiff.diff >= 0 ? 'positive' : 'negative');

                document.getElementById('compareTotalCount').textContent = `${compareData.year}년: ${compareData.total_count.toLocaleString()}건`;
                document.getElementById('compareTotalCount').style.display = 'block';
                const countDiff = formatDiff(currentData.total_count, compareData.total_count);
                document.getElementById('diffTotalCount').textContent = countDiff.text;
                document.getElementById('diffTotalCount').className = 'diff ' + (countDiff.diff >= 0 ? 'positive' : 'negative');

                document.getElementById('compareAvgPrice').textContent = `${compareData.year}년: ${formatCurrency(compAvg)}`;
                document.getElementById('compareAvgPrice').style.display = 'block';
                const avgDiff = formatDiff(avgPrice, compAvg);
                document.getElementById('diffAvgPrice').textContent = avgDiff.text;
                document.getElementById('diffAvgPrice').className = 'diff ' + (avgDiff.diff >= 0 ? 'positive' : 'negative');
            } else {
                document.getElementById('compareTotalSales').style.display = 'none';
                document.getElementById('compareTotalCount').style.display = 'none';
                document.getElementById('compareAvgPrice').style.display = 'none';
                document.getElementById('diffTotalSales').textContent = '';
                document.getElementById('diffTotalCount').textContent = '';
                document.getElementById('diffAvgPrice').textContent = '';
            }
        }

        function updateManagerChart() {
            const top15 = currentData.by_manager.slice(0, 15);
            const ctx = document.getElementById('managerChart').getContext('2d');

            if (charts.manager) charts.manager.destroy();

            const datasets = [{
                label: currentData.year + '년',
                data: top15.map(d => d[1].sales),
                backgroundColor: 'rgba(102, 126, 234, 0.8)',
            }];

            if (compareData) {
                const compareMap = Object.fromEntries(compareData.by_manager);
                datasets.push({
                    label: compareData.year + '년',
                    data: top15.map(d => compareMap[d[0]]?.sales || 0),
                    backgroundColor: 'rgba(118, 75, 162, 0.6)',
                });
                document.getElementById('managerLegend').innerHTML = `
                    <div class="legend-item"><div class="legend-color" style="background:rgba(102,126,234,0.8)"></div>${currentData.year}년</div>
                    <div class="legend-item"><div class="legend-color" style="background:rgba(118,75,162,0.6)"></div>${compareData.year}년</div>
                `;
                document.getElementById('managerLegend').style.display = 'flex';
            } else {
                document.getElementById('managerLegend').style.display = 'none';
            }

            charts.manager = new Chart(ctx, {
                type: 'bar',
                data: { labels: top15.map(d => d[0]), datasets },
                options: {
                    responsive: true,
                    plugins: { legend: { display: false } },
                    scales: { y: { ticks: { callback: value => formatCurrency(value) } } }
                }
            });
        }

        function updateBranchChart() {
            const ctx = document.getElementById('branchChart').getContext('2d');

            if (charts.branch) charts.branch.destroy();

            if (compareData) {
                // 비교 모드: 막대 차트로 변경
                const labels = currentData.by_branch.map(d => d[0]);
                const compareMap = Object.fromEntries(compareData.by_branch);

                document.getElementById('branchLegend').innerHTML = `
                    <div class="legend-item"><div class="legend-color" style="background:rgba(102,126,234,0.8)"></div>${currentData.year}년</div>
                    <div class="legend-item"><div class="legend-color" style="background:rgba(118,75,162,0.6)"></div>${compareData.year}년</div>
                `;
                document.getElementById('branchLegend').style.display = 'flex';

                charts.branch = new Chart(ctx, {
                    type: 'bar',
                    data: {
                        labels: labels,
                        datasets: [
                            {
                                label: currentData.year + '년',
                                data: currentData.by_branch.map(d => d[1].sales),
                                backgroundColor: 'rgba(102, 126, 234, 0.8)',
                            },
                            {
                                label: compareData.year + '년',
                                data: labels.map(l => compareMap[l]?.sales || 0),
                                backgroundColor: 'rgba(118, 75, 162, 0.6)',
                            }
                        ]
                    },
                    options: {
                        responsive: true,
                        plugins: { legend: { display: false } },
                        scales: { y: { ticks: { callback: value => formatCurrency(value) } } }
                    }
                });
            } else {
                document.getElementById('branchLegend').style.display = 'none';
                charts.branch = new Chart(ctx, {
                    type: 'pie',
                    data: {
                        labels: currentData.by_branch.map(d => d[0]),
                        datasets: [{
                            data: currentData.by_branch.map(d => d[1].sales),
                            backgroundColor: ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe', '#43e97b', '#fa709a', '#fee140']
                        }]
                    },
                    options: { responsive: true, plugins: { legend: { position: 'right' } } }
                });
            }
        }

        function updateMonthlyChart() {
            const ctx = document.getElementById('monthlyChart').getContext('2d');

            if (charts.monthly) charts.monthly.destroy();

            const labels = [];
            for (let i = 1; i <= 12; i++) labels.push(i + '월');

            const currentMap = Object.fromEntries(currentData.by_month);
            const currentValues = labels.map((_, i) => currentMap[i+1]?.sales || 0);

            const datasets = [{
                label: currentData.year + '년',
                data: currentValues,
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                fill: true,
                tension: 0.4
            }];

            if (compareData) {
                const compareMap = Object.fromEntries(compareData.by_month);
                datasets.push({
                    label: compareData.year + '년',
                    data: labels.map((_, i) => compareMap[i+1]?.sales || 0),
                    borderColor: '#764ba2',
                    backgroundColor: 'rgba(118, 75, 162, 0.1)',
                    fill: true,
                    tension: 0.4
                });
                document.getElementById('monthlyLegend').innerHTML = `
                    <div class="legend-item"><div class="legend-color" style="background:#667eea"></div>${currentData.year}년</div>
                    <div class="legend-item"><div class="legend-color" style="background:#764ba2"></div>${compareData.year}년</div>
                `;
                document.getElementById('monthlyLegend').style.display = 'flex';
            } else {
                document.getElementById('monthlyLegend').style.display = 'none';
            }

            charts.monthly = new Chart(ctx, {
                type: 'line',
                data: { labels, datasets },
                options: {
                    responsive: true,
                    plugins: { legend: { display: false } },
                    scales: { y: { ticks: { callback: value => formatCurrency(value) } } }
                }
            });
        }

        function updateManagerTable() {
            const thead = document.getElementById('managerTableHead');
            const tbody = document.querySelector('#managerTable tbody');

            if (compareData) {
                thead.innerHTML = `<tr><th>담당자</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th><th>비중</th></tr>`;
                const compareMap = Object.fromEntries(compareData.by_manager);
                tbody.innerHTML = currentData.by_manager.map(d => {
                    const compSales = compareMap[d[0]]?.sales || 0;
                    const diff = d[1].sales - compSales;
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    return `<tr>
                        <td>${d[0]}</td>
                        <td>${formatCurrency(d[1].sales)}</td>
                        <td>${formatCurrency(compSales)}</td>
                        <td class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)}</td>
                        <td>${(d[1].sales / currentData.total_sales * 100).toFixed(1)}%</td>
                    </tr>`;
                }).join('');
            } else {
                thead.innerHTML = `<tr><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr>`;
                tbody.innerHTML = currentData.by_manager.map(d => `
                    <tr>
                        <td>${d[0]}</td>
                        <td>${formatCurrency(d[1].sales)}</td>
                        <td>${d[1].count}</td>
                        <td>${(d[1].sales / currentData.total_sales * 100).toFixed(1)}%</td>
                    </tr>
                `).join('');
            }
        }

        function updateBranchTable() {
            const thead = document.getElementById('branchTableHead');
            const tbody = document.querySelector('#branchTable tbody');

            if (compareData) {
                thead.innerHTML = `<tr><th>지사/센터</th><th>${currentData.year}년</th><th>${compareData.year}년</th><th>증감</th></tr>`;
                const compareMap = Object.fromEntries(compareData.by_branch);
                tbody.innerHTML = currentData.by_branch.map(d => {
                    const compSales = compareMap[d[0]]?.sales || 0;
                    const diff = d[1].sales - compSales;
                    const diffClass = diff >= 0 ? 'positive' : 'negative';
                    return `<tr>
                        <td>${d[0]}</td>
                        <td>${formatCurrency(d[1].sales)}</td>
                        <td>${formatCurrency(compSales)}</td>
                        <td class="${diffClass}">${diff >= 0 ? '+' : ''}${formatCurrency(diff)}</td>
                    </tr>`;
                }).join('');
            } else {
                thead.innerHTML = `<tr><th>지사/센터</th><th>매출액</th><th>건수</th><th>담당자수</th></tr>`;
                tbody.innerHTML = currentData.by_branch.map(d => `
                    <tr>
                        <td>${d[0]}</td>
                        <td>${formatCurrency(d[1].sales)}</td>
                        <td>${d[1].count}</td>
                        <td>${d[1].managers}명</td>
                    </tr>
                `).join('');
            }
        }

        // 페이지 로드 시 안내 메시지
        showToast('연도를 선택하고 [조회하기] 버튼을 클릭하세요.', 'loading', 5000);
        setTimeout(() => hideToast(), 5000);
    </script>
</body>
</html>
'''

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/data')
def get_data():
    year = request.args.get('year', '2025')
    data = load_excel_data(year)
    processed = process_data(data)
    return jsonify(processed)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6001, debug=False)
