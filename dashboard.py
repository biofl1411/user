"""
경영지표 대시보드 (Streamlit)
- 개인별/팀별 실적
- 월별 추이
- 목표 대비 달성률
- 전년대비 분석
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
from datetime import datetime
import sys

# 경로 설정
sys.path.insert(0, str(Path(__file__).parent))
from config.settings import MANAGER_TO_BRANCH, BRANCHES

# 페이지 설정
st.set_page_config(
    page_title="경영지표 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 스타일
st.markdown("""
<style>
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .stMetric > div {
        background-color: #f8f9fa;
        padding: 15px;
        border-radius: 10px;
        border-left: 4px solid #3498db;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_data
def load_data(year: int) -> pd.DataFrame:
    """연도별 데이터 로드"""
    data_path = Path(f"data/{year}")
    if not data_path.exists():
        return pd.DataFrame()

    files = sorted(data_path.glob("*.xlsx"))
    if not files:
        return pd.DataFrame()

    dfs = []
    for f in files:
        try:
            df = pd.read_excel(f)
            dfs.append(df)
        except Exception as e:
            st.warning(f"파일 로드 오류: {f.name} - {e}")

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    # 지사/센터 컬럼 추가
    if '영업담당' in df.columns:
        df['지사센터'] = df['영업담당'].map(MANAGER_TO_BRANCH).fillna('기타')

    # 날짜 처리
    if '접수일자' in df.columns:
        df['접수일자'] = pd.to_datetime(df['접수일자'], errors='coerce')
        df['월'] = df['접수일자'].dt.month
        df['년월'] = df['접수일자'].dt.to_period('M').astype(str)

    return df


@st.cache_data
def load_targets(year: int) -> pd.DataFrame:
    """목표 데이터 로드"""
    csv_path = Path(f"data/targets/{year}_목표.csv")
    if csv_path.exists():
        return pd.read_csv(csv_path)
    return pd.DataFrame()


def format_currency(value):
    """통화 형식"""
    if value >= 100000000:
        return f"{value/100000000:.1f}억"
    elif value >= 10000:
        return f"{value/10000:,.0f}만"
    else:
        return f"{value:,.0f}"


def main():
    st.title("📊 경영지표 대시보드")

    # 사이드바
    with st.sidebar:
        st.header("설정")

        # 연도 선택
        current_year = datetime.now().year
        selected_year = st.selectbox("연도 선택", [2025, 2024], index=0)

        # 데이터 로드
        df = load_data(selected_year)
        df_prev = load_data(selected_year - 1)
        targets = load_targets(selected_year)

        if df.empty:
            st.error(f"{selected_year}년 데이터가 없습니다.")
            return

        st.success(f"✅ {len(df):,}건 로드됨")

        # 월 필터
        if '월' in df.columns:
            months = sorted(df['월'].dropna().unique())
            selected_months = st.multiselect(
                "월 선택",
                months,
                default=months,
                format_func=lambda x: f"{int(x)}월"
            )
            df = df[df['월'].isin(selected_months)]

        # 지사/센터 필터
        if '지사센터' in df.columns:
            branches = ['전체'] + sorted(df['지사센터'].unique())
            selected_branch = st.selectbox("지사/센터", branches)
            if selected_branch != '전체':
                df = df[df['지사센터'] == selected_branch]

    # 메인 컨텐츠
    if df.empty:
        st.warning("선택한 조건에 해당하는 데이터가 없습니다.")
        return

    # ===== 상단 요약 카드 =====
    st.subheader("📈 주요 지표")

    col1, col2, col3, col4 = st.columns(4)

    total_sales = df['수수료'].sum() if '수수료' in df.columns else 0
    total_count = len(df)
    avg_sales = total_sales / total_count if total_count > 0 else 0

    # 전년 대비
    yoy_growth = 0
    if not df_prev.empty and '수수료' in df_prev.columns:
        prev_sales = df_prev['수수료'].sum()
        if prev_sales > 0:
            yoy_growth = ((total_sales / prev_sales) - 1) * 100

    with col1:
        st.metric("총 매출", format_currency(total_sales), f"{yoy_growth:+.1f}% 전년대비")

    with col2:
        # 목표 달성률
        target_sales = 0
        if not targets.empty and selected_months:
            for m in selected_months:
                col_name = f"{int(m)}월"
                if col_name in targets.columns:
                    total_row = targets[targets['구분'] == '총계']
                    if not total_row.empty:
                        target_sales += total_row[col_name].values[0] * 1000  # 천원 → 원

        achievement = (total_sales / target_sales * 100) if target_sales > 0 else 0
        st.metric("목표 달성률", f"{achievement:.1f}%", f"목표: {format_currency(target_sales)}")

    with col3:
        st.metric("총 건수", f"{total_count:,}건")

    with col4:
        st.metric("평균 단가", format_currency(avg_sales))

    st.divider()

    # ===== 탭 구성 =====
    tab1, tab2, tab3, tab4 = st.tabs(["👤 개인별 실적", "🏢 팀별 실적", "📅 월별 추이", "📊 상세 분석"])

    # ----- 개인별 실적 -----
    with tab1:
        st.subheader("👤 영업담당별 실적")

        if '영업담당' in df.columns and '수수료' in df.columns:
            # 개인별 집계
            personal = df.groupby('영업담당').agg(
                매출액=('수수료', 'sum'),
                건수=('수수료', 'count'),
                평균단가=('수수료', 'mean')
            ).reset_index()
            personal = personal.sort_values('매출액', ascending=False)
            personal['비중'] = (personal['매출액'] / personal['매출액'].sum() * 100).round(1)

            col1, col2 = st.columns([2, 1])

            with col1:
                # 막대 차트
                fig = px.bar(
                    personal.head(15),
                    x='영업담당',
                    y='매출액',
                    color='매출액',
                    color_continuous_scale='Blues',
                    title='영업담당별 매출 TOP 15'
                )
                fig.update_layout(showlegend=False)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # 테이블
                display_df = personal.copy()
                display_df['매출액'] = display_df['매출액'].apply(format_currency)
                display_df['평균단가'] = display_df['평균단가'].apply(lambda x: format_currency(x))
                display_df['비중'] = display_df['비중'].apply(lambda x: f"{x}%")
                st.dataframe(display_df, use_container_width=True, height=500)

    # ----- 팀별 실적 -----
    with tab2:
        st.subheader("🏢 지사/센터별 실적")

        if '지사센터' in df.columns and '수수료' in df.columns:
            # 팀별 집계
            team = df.groupby('지사센터').agg(
                매출액=('수수료', 'sum'),
                건수=('수수료', 'count'),
                평균단가=('수수료', 'mean'),
                담당자수=('영업담당', 'nunique')
            ).reset_index()
            team = team.sort_values('매출액', ascending=False)
            team['비중'] = (team['매출액'] / team['매출액'].sum() * 100).round(1)
            team['인당매출'] = team['매출액'] / team['담당자수']

            col1, col2 = st.columns(2)

            with col1:
                # 파이 차트
                fig = px.pie(
                    team,
                    values='매출액',
                    names='지사센터',
                    title='지사/센터별 매출 비중',
                    color_discrete_sequence=px.colors.qualitative.Set3
                )
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                # 막대 차트 (인당 매출)
                fig = px.bar(
                    team,
                    x='지사센터',
                    y='인당매출',
                    color='인당매출',
                    color_continuous_scale='Greens',
                    title='지사/센터별 인당 매출'
                )
                st.plotly_chart(fig, use_container_width=True)

            # 상세 테이블
            st.subheader("📋 지사/센터별 상세")
            display_team = team.copy()
            display_team['매출액'] = display_team['매출액'].apply(format_currency)
            display_team['평균단가'] = display_team['평균단가'].apply(format_currency)
            display_team['인당매출'] = display_team['인당매출'].apply(format_currency)
            display_team['비중'] = display_team['비중'].apply(lambda x: f"{x}%")
            st.dataframe(display_team, use_container_width=True)

            # 팀별 개인 상세
            st.subheader("📋 팀별 개인 실적")
            selected_team = st.selectbox("팀 선택", team['지사센터'].tolist())

            team_members = df[df['지사센터'] == selected_team].groupby('영업담당').agg(
                매출액=('수수료', 'sum'),
                건수=('수수료', 'count')
            ).reset_index().sort_values('매출액', ascending=False)

            team_members['매출액_표시'] = team_members['매출액'].apply(format_currency)
            st.dataframe(team_members[['영업담당', '매출액_표시', '건수']], use_container_width=True)

    # ----- 월별 추이 -----
    with tab3:
        st.subheader("📅 월별 매출 추이")

        if '년월' in df.columns and '수수료' in df.columns:
            monthly = df.groupby('년월')['수수료'].sum().reset_index()
            monthly.columns = ['년월', '매출액']

            # 전년도 데이터
            if not df_prev.empty and '접수일자' in df_prev.columns:
                df_prev['년월'] = pd.to_datetime(df_prev['접수일자'], errors='coerce').dt.to_period('M').astype(str)
                monthly_prev = df_prev.groupby('년월')['수수료'].sum().reset_index()
                monthly_prev.columns = ['년월', '전년매출']
                monthly_prev['년월'] = monthly_prev['년월'].str.replace(str(selected_year-1), str(selected_year))
                monthly = monthly.merge(monthly_prev, on='년월', how='left')

            # 라인 차트
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=monthly['년월'],
                y=monthly['매출액'],
                mode='lines+markers',
                name=f'{selected_year}년',
                line=dict(color='#3498db', width=3)
            ))

            if '전년매출' in monthly.columns:
                fig.add_trace(go.Scatter(
                    x=monthly['년월'],
                    y=monthly['전년매출'],
                    mode='lines+markers',
                    name=f'{selected_year-1}년',
                    line=dict(color='#95a5a6', width=2, dash='dash')
                ))

            fig.update_layout(title='월별 매출 추이', xaxis_title='월', yaxis_title='매출액')
            st.plotly_chart(fig, use_container_width=True)

            # 월별 테이블
            display_monthly = monthly.copy()
            display_monthly['매출액'] = display_monthly['매출액'].apply(format_currency)
            if '전년매출' in display_monthly.columns:
                display_monthly['전년매출'] = display_monthly['전년매출'].apply(lambda x: format_currency(x) if pd.notna(x) else '-')
            st.dataframe(display_monthly, use_container_width=True)

    # ----- 상세 분석 -----
    with tab4:
        st.subheader("📊 상세 분석")

        col1, col2 = st.columns(2)

        with col1:
            # 검사목적별
            if '검사목적' in df.columns:
                purpose = df.groupby('검사목적')['수수료'].sum().reset_index()
                purpose.columns = ['검사목적', '매출액']
                purpose = purpose.sort_values('매출액', ascending=False).head(10)

                fig = px.bar(
                    purpose,
                    x='매출액',
                    y='검사목적',
                    orientation='h',
                    title='검사목적별 매출 TOP 10',
                    color='매출액',
                    color_continuous_scale='Oranges'
                )
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            # 시험분야별
            if '시험분야' in df.columns:
                field = df.groupby('시험분야')['수수료'].sum().reset_index()
                field.columns = ['시험분야', '매출액']

                fig = px.pie(
                    field,
                    values='매출액',
                    names='시험분야',
                    title='시험분야별 매출 비중'
                )
                st.plotly_chart(fig, use_container_width=True)

        # 거래처 TOP
        if '거래처' in df.columns:
            st.subheader("🏆 거래처 TOP 20")
            client = df.groupby('거래처')['수수료'].agg(['sum', 'count']).reset_index()
            client.columns = ['거래처', '매출액', '건수']
            client = client.sort_values('매출액', ascending=False).head(20)
            client['매출액'] = client['매출액'].apply(format_currency)
            st.dataframe(client, use_container_width=True)

    # 푸터
    st.divider()
    st.caption(f"마지막 업데이트: {datetime.now().strftime('%Y-%m-%d %H:%M')}")


if __name__ == "__main__":
    main()
