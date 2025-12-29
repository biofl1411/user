"""
경영지표 대시보드 (Flask 버전)
- 오래된 CPU에서도 작동
- Chart.js 사용
"""
from flask import Flask, render_template_string, jsonify, request
import os
from pathlib import Path
from datetime import datetime

app = Flask(__name__)

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

    data_path = Path(f"data/{year}")
    if not data_path.exists():
        return []

    all_data = []
    for f in sorted(data_path.glob("*.xlsx")):
        try:
            wb = load_workbook(f, read_only=True, data_only=True)
            ws = wb.active

            headers = [cell.value for cell in ws[1]]

            for row in ws.iter_rows(min_row=2, values_only=True):
                row_dict = dict(zip(headers, row))
                all_data.append(row_dict)

            wb.close()
        except Exception as e:
            print(f"Error loading {f}: {e}")

    return all_data

def process_data(data):
    """데이터 처리"""
    # 영업담당별 매출 집계
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
        }
        .controls select {
            padding: 8px 15px;
            border-radius: 5px;
            border: 1px solid #ddd;
            font-size: 14px;
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
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 경영지표 대시보드</h1>
        <div class="controls">
            <select id="yearSelect" onchange="loadData()">
                <option value="2025">2025년</option>
                <option value="2024">2024년</option>
            </select>
        </div>
    </div>

    <div class="summary" id="summary">
        <div class="card">
            <h3>총 매출</h3>
            <div class="value" id="totalSales">-</div>
        </div>
        <div class="card">
            <h3>총 건수</h3>
            <div class="value" id="totalCount">-</div>
        </div>
        <div class="card">
            <h3>평균 단가</h3>
            <div class="value" id="avgPrice">-</div>
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
                <canvas id="managerChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>영업담당별 상세</h3>
                <table id="managerTable">
                    <thead><tr><th>담당자</th><th>매출액</th><th>건수</th><th>비중</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>

    <div id="team" class="tab-content">
        <div class="charts">
            <div class="chart-container">
                <h3>지사/센터별 매출</h3>
                <canvas id="branchChart"></canvas>
            </div>
            <div class="chart-container">
                <h3>지사/센터별 상세</h3>
                <table id="branchTable">
                    <thead><tr><th>지사/센터</th><th>매출액</th><th>건수</th><th>담당자수</th></tr></thead>
                    <tbody></tbody>
                </table>
            </div>
        </div>
    </div>

    <div id="monthly" class="tab-content">
        <div class="charts">
            <div class="chart-container" style="grid-column: 1/-1;">
                <h3>월별 매출 추이</h3>
                <canvas id="monthlyChart"></canvas>
            </div>
        </div>
    </div>

    <script>
        let charts = {};

        function formatCurrency(value) {
            if (value >= 100000000) return (value/100000000).toFixed(1) + '억';
            if (value >= 10000) return (value/10000).toFixed(0) + '만';
            return value.toLocaleString();
        }

        function showTab(tabId) {
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelector(`[onclick="showTab('${tabId}')"]`).classList.add('active');
            document.getElementById(tabId).classList.add('active');
        }

        async function loadData() {
            const year = document.getElementById('yearSelect').value;

            try {
                const response = await fetch(`/api/data?year=${year}`);
                const data = await response.json();

                // 요약 업데이트
                document.getElementById('totalSales').textContent = formatCurrency(data.total_sales);
                document.getElementById('totalCount').textContent = data.total_count.toLocaleString() + '건';
                document.getElementById('avgPrice').textContent = formatCurrency(data.total_count > 0 ? data.total_sales / data.total_count : 0);

                // 차트 업데이트
                updateManagerChart(data.by_manager);
                updateBranchChart(data.by_branch);
                updateMonthlyChart(data.by_month);

                // 테이블 업데이트
                updateManagerTable(data.by_manager, data.total_sales);
                updateBranchTable(data.by_branch);

            } catch (error) {
                console.error('Error loading data:', error);
            }
        }

        function updateManagerChart(data) {
            const top15 = data.slice(0, 15);
            const ctx = document.getElementById('managerChart').getContext('2d');

            if (charts.manager) charts.manager.destroy();

            charts.manager = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: top15.map(d => d[0]),
                    datasets: [{
                        label: '매출액',
                        data: top15.map(d => d[1].sales),
                        backgroundColor: 'rgba(102, 126, 234, 0.8)',
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { display: false }
                    },
                    scales: {
                        y: {
                            ticks: {
                                callback: value => formatCurrency(value)
                            }
                        }
                    }
                }
            });
        }

        function updateBranchChart(data) {
            const ctx = document.getElementById('branchChart').getContext('2d');

            if (charts.branch) charts.branch.destroy();

            charts.branch = new Chart(ctx, {
                type: 'pie',
                data: {
                    labels: data.map(d => d[0]),
                    datasets: [{
                        data: data.map(d => d[1].sales),
                        backgroundColor: [
                            '#667eea', '#764ba2', '#f093fb', '#f5576c',
                            '#4facfe', '#43e97b', '#fa709a', '#fee140'
                        ]
                    }]
                },
                options: {
                    responsive: true,
                    plugins: {
                        legend: { position: 'right' }
                    }
                }
            });
        }

        function updateMonthlyChart(data) {
            const ctx = document.getElementById('monthlyChart').getContext('2d');

            if (charts.monthly) charts.monthly.destroy();

            charts.monthly = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: data.map(d => d[0] + '월'),
                    datasets: [{
                        label: '매출액',
                        data: data.map(d => d[1].sales),
                        borderColor: '#667eea',
                        backgroundColor: 'rgba(102, 126, 234, 0.1)',
                        fill: true,
                        tension: 0.4
                    }]
                },
                options: {
                    responsive: true,
                    scales: {
                        y: {
                            ticks: {
                                callback: value => formatCurrency(value)
                            }
                        }
                    }
                }
            });
        }

        function updateManagerTable(data, total) {
            const tbody = document.querySelector('#managerTable tbody');
            tbody.innerHTML = data.map(d => `
                <tr>
                    <td>${d[0]}</td>
                    <td>${formatCurrency(d[1].sales)}</td>
                    <td>${d[1].count}</td>
                    <td>${(d[1].sales / total * 100).toFixed(1)}%</td>
                </tr>
            `).join('');
        }

        function updateBranchTable(data) {
            const tbody = document.querySelector('#branchTable tbody');
            tbody.innerHTML = data.map(d => `
                <tr>
                    <td>${d[0]}</td>
                    <td>${formatCurrency(d[1].sales)}</td>
                    <td>${d[1].count}</td>
                    <td>${d[1].managers}명</td>
                </tr>
            `).join('');
        }

        // 초기 로드
        loadData();
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
