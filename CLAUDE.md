# Business Metrics Analyzer

음식 검사 기관의 경영 데이터 분석 및 시각화 시스템

## 기술 스택
- Backend: Flask, SQLite, Pandas
- Frontend: JavaScript, Chart.js
- AI: Google Gemini API, Claude API
- GUI: PyQt5

---

## 서버 구성

| 구분 | 경로 | 포트 | IP |
|------|------|------|-----|
| 클로드 서버 (개발) | `/home/user/business_metrics/` | - | - |
| 운영 서버 (본 사이트) | `/home/biofl/business_metrics/` | 6001 | 14.7.14.31 |
| 운영 서버 (데모) | `/home/biofl/business_metrics_demo/` | 6005 | 14.7.14.31 |

- 운영 서버 호스트: `bioflsever`, 사용자: `biofl`
- 내부 IP: `192.168.0.96`

---

## 개발 워크플로우

```
클로드 서버에서 수정 → git push → PR/병합 → 데모 테스트 → 본 사이트 적용
```

**중요**: 클로드 서버는 운영 서버에 직접 접근 불가. git push 후 사용자가 pull 필요.

---

## 디렉토리 구조

```
business_metrics/
├── flask_dashboard.py      # 메인 웹앱 (26,500줄+)
├── config/settings.py      # 컬럼매핑, 담당자매핑, KPI임계값
├── modules/                # data_loader, metrics_calculator 등
├── colab/                  # Colab 노트북
│   └── excel_to_sqlite_uploader.ipynb  # 드라이브→SQLite 변환
└── data/
    ├── users.db            # 사용자/권한 DB
    ├── business_data.db    # 데이터 캐시 DB (~1GB)
    ├── 2024/, 2025/        # 메인 엑셀 데이터
    └── food_item/          # 음식항목 엑셀 데이터
```

---

## 데이터베이스

- **users.db**: 사용자, 팀, 권한, 목표
- **business_data.db**: excel_data, food_item_data, file_metadata

### SQLite 테이블
- `excel_data`: 메인 매출 데이터 (`data/2024/*.xlsx`)
- `food_item_data`: 음식항목 데이터 (`data/food_item/2024/*.xlsx`)

---

## 데이터 변환 (Colab → 서버)

### Colab 노트북 사용

`colab/excel_to_sqlite_uploader.ipynb` 노트북을 Google Colab에서 실행

**구글 드라이브 폴더 구조**:
```
MyDrive/business_metrics_data/
├── 2024/*.xlsx          # 메인 매출 데이터
├── 2025/*.xlsx
└── food_item/
    ├── 2024/*.xlsx      # 음식항목 데이터
    └── 2025/*.xlsx
```

**실행 순서**:
1. 드라이브에 엑셀 파일 업로드
2. Colab에서 노트북 실행 (드라이브 마운트 → 변환 → 업로드)
3. 데모 서버에서 먼저 테스트 후 본 사이트 적용

**업로드 API**: `/api/upload-db`
- 헤더: `X-API-Key: biofl1411-upload-key`
- 자동으로 기존 DB 백업 및 캐시 초기화

---

## 운영 명령어

```bash
# 업데이트
cd ~/business_metrics && git pull origin main
cd ~/business_metrics_demo && git pull origin main

# 본 사이트 재시작
cd ~/business_metrics
pkill -f "business_metrics/flask_dashboard.py"
nohup python flask_dashboard.py > nohup.out 2>&1 &

# 데모 재시작
cd ~/business_metrics_demo
pkill -f "business_metrics_demo/flask_dashboard.py"
nohup python flask_dashboard.py > nohup_demo.out 2>&1 &

# 확인
ps aux | grep flask_dashboard
netstat -tlnp | grep -E "6001|6005"
```

---

## 주의사항

1. `flask_dashboard.py`는 26,500줄+ 단일 파일
2. 컬럼명은 `config/settings.py`의 COLUMN_MAPPING만 인식
3. **항상 데모에서 먼저 테스트** 후 본 사이트 적용
4. 이전 버전으로 롤백 금지
