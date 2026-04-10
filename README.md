# KR Quant Studio

한국 주식 재무 분석 플랫폼. DART 공시 기반의 영업레버리지 시뮬레이터, GPM 회귀분석, 섹터 스크리너, 리서치 리포트 발행 도구.

## 모듈
1. **Operating Leverage Simulator** — 매출 성장률 시나리오별 영업이익 시뮬레이션
2. **GPM-Revenue Regression** — 과거 분기 데이터 회귀로 GPM 밴드 자동 추정
3. **Sector Screener** — 레버리지형 / CAPA 증설형 종목 발굴
4. **DC Revenue Calculator** — 사업부문별 매출 기여도 계산

## 개발 환경 셋업

```bash
python -m uv sync
python -m uv sync --group dev --group data --group ui --group reports --group stats
```

## 실행

```bash
python -m uv run pytest tests/unit/ -v
python -m uv run streamlit run src/krqs/ui/app.py
```

자세한 플랜은 `C:\Users\admin\.claude\plans\binary-enchanting-seahorse.md` 참조.
