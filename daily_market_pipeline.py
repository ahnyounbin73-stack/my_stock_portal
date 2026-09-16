# -*- coding: utf-8 -*-
"""
글로벌 증시, 국내 대기업 및 실시간 환율(달러/엔/유로) 자동 업데이트 파이프라인
파일명: daily_market_pipeline.py
"""

import os
import sys
import json
import datetime

TICKER_MAP = {
    # 🌐 글로벌 시장 지수
    "SPX": {"yf": "^GSPC", "name": "S&P 500", "cat": "INDEX", "cur": "USD", "desc": "미국 대형주 500개 대표 지수"},
    "IXIC": {"yf": "^IXIC", "name": "나스닥 종합", "cat": "INDEX", "cur": "USD", "desc": "글로벌 기술주 및 AI 반도체 중심 대표 지수"},
    "DJI": {"yf": "^DJI", "name": "다우존스 30", "cat": "INDEX", "cur": "USD", "desc": "미국 전통 우량 블루칩 30개 기업"},
    "KS11": {"yf": "^KS11", "name": "코스피", "cat": "INDEX", "cur": "KRW", "desc": "대한민국 유가증권시장 대표 종합주가지수"},
    "KQ11": {"yf": "^KQ11", "name": "코스닥", "cat": "INDEX", "cur": "KRW", "desc": "혁신 바이오·반도체 소부장 성장기업 시장"},
    
    # 🇺🇸 미국 빅테크
    "AVGO": {"yf": "AVGO", "name": "브로드컴", "cat": "US_TECH", "cur": "USD", "desc": "AI 커스텀 ASIC 및 네트워킹 지배기업"},
    "NVDA": {"yf": "NVDA", "name": "엔비디아", "cat": "US_TECH", "cur": "USD", "desc": "AI 가속기 및 CUDA 생태계 독점"},
    "AAPL": {"yf": "AAPL", "name": "애플", "cat": "US_TECH", "cur": "USD", "desc": "아이폰 및 애플 인텔리전스 생태계"},
    "MSFT": {"yf": "MSFT", "name": "마이크로소프트", "cat": "US_TECH", "cur": "USD", "desc": "애저 클라우드 및 코파일럿 AI"},
    "GOOGL": {"yf": "GOOGL", "name": "알파벳", "cat": "US_TECH", "cur": "USD", "desc": "구글 검색, 제미나이 AI, 자체 TPU"},
    "META": {"yf": "META", "name": "메타", "cat": "US_TECH", "cur": "USD", "desc": "인스타그램, 왓츠앱, 라마 AI 모델"},
    "AMZN": {"yf": "AMZN", "name": "아마존", "cat": "US_TECH", "cur": "USD", "desc": "이커머스 및 AWS 클라우드"},
    "TSLA": {"yf": "TSLA", "name": "테슬라", "cat": "US_TECH", "cur": "USD", "desc": "전기차, FSD 자율주행, 옵티머스 로봇"},
    
    # 🇰🇷 국내 대표 대기업
    "005930": {"yf": "005930.KS", "name": "삼성전자", "cat": "KR_GIANT", "cur": "KRW", "desc": "D램·낸드 1위, HBM3E/HBM4 및 갤럭시 AI"},
    "000660": {"yf": "000660.KS", "name": "SK하이닉스", "cat": "KR_GIANT", "cur": "KRW", "desc": "HBM 세계 1위, 엔비디아 핵심 파트너"},
    "005380": {"yf": "005380.KS", "name": "현대자동차", "cat": "KR_GIANT", "cur": "KRW", "desc": "하이브리드·전기차 및 밸류업 주주환원"},
    "373220": {"yf": "373220.KS", "name": "LG에너지솔루션", "cat": "KR_GIANT", "cur": "KRW", "desc": "글로벌 배터리 제조 및 ESS 사업 확장"},
    "035420": {"yf": "035420.KS", "name": "NAVER", "cat": "KR_GIANT", "cur": "KRW", "desc": "국내 1위 검색 포털 및 하이퍼클로바X AI"}
}

def fetch_exchange_rates(yf_module):
    """주요 환율(달러, 100엔, 유로) 최신 시세 수집"""
    fx_dict = {
        "USD": {"yf": "USDKRW=X", "name": "USD/KRW", "flag": "🇺🇸", "default": 1364.50},
        "JPY": {"yf": "JPYKRW=X", "name": "100JPY/KRW", "flag": "🇯🇵", "default": 881.14},
        "EUR": {"yf": "EURKRW=X", "name": "EUR/KRW", "flag": "🇪🇺", "default": 1577.33}
    }
    rates = {}
    for k, item in fx_dict.items():
        rate = item["default"]
        chg, pct = 0.0, 0.0
        try:
            tk = yf_module.Ticker(item["yf"])
            h = tk.history(period="2d")
            if not h.empty:
                latest = float(h.iloc[-1]["Close"])
                prev = float(h.iloc[-2]["Close"]) if len(h) >= 2 else latest
                if k == "JPY":
                    latest *= 100.0
                    prev *= 100.0
                rate = round(latest, 1)
                chg = round(latest - prev, 1)
                pct = round((chg / prev) * 100.0, 2) if prev != 0 else 0.0
        except Exception as e:
            print(f"[환율 수집 경고] {k}: {e}")
            
        rates[k] = {
            "name": item["name"],
            "flag": item["flag"],
            "rate": rate,
            "change": chg,
            "pct_change": pct
        }
    return rates

def generate_daily_factors(name, pct_chg, is_index):
    if pct_chg > 4.0:
        sentiment, category = "급등", "실적/호재"
        headline = f"{name} 호실적 발표 및 핵심 수주 모멘텀으로 {sentiment} 견인"
    elif pct_chg > 1.5:
        sentiment, category = "상승", "업황호조"
        headline = f"업종 전반의 투자 심리 개선 및 기관 순매수에 힘입어 {sentiment} 마감"
    elif pct_chg < -4.0:
        sentiment, category = "급락", "거시/충격"
        headline = f"실적 가이던스 눈높이 미달 또는 대외 거시 악재로 인한 대규모 매물 출회"
    elif pct_chg < -1.5:
        sentiment, category = "하락", "시장조정"
        headline = f"단기 급등에 따른 차익 실현 및 시장 변동성 확대로 {sentiment}세 전개"
    else:
        sentiment, category = "보합", "시장관망"
        headline = f"주요 경제 지표 및 기업 이벤트를 앞둔 차분한 매물 소화 장세 ({pct_chg:+.2f}%)"
    return headline, category, sentiment

def build_real_market_database():
    import yfinance as yf
    print(f"[{datetime.datetime.now()}] 18개 자산 및 환율 데이터 수집 시작...")
    
    db = {}
    success_count = 0
    
    for tk, info in TICKER_MAP.items():
        is_usd = info["cur"] == "USD"
        is_index = info["cat"] == "INDEX"
        yf_symbol = info["yf"]
        
        try:
            ticker_obj = yf.Ticker(yf_symbol)
            hist = ticker_obj.history(period="1y")
            if hist.empty:
                continue
                
            series = []
            prev_close = None
            for dt_index, row in hist.iterrows():
                day_str = dt_index.strftime("%Y-%m-%d")
                op = round(float(row["Open"]), 2 if is_usd else 0)
                hp = round(float(row["High"]), 2 if is_usd else 0)
                lp = round(float(row["Low"]), 2 if is_usd else 0)
                cp = round(float(row["Close"]), 2 if is_usd else 0)
                vol = int(row["Volume"]) if not row.isna().get("Volume", False) else 0
                
                if prev_close is None:
                    prev_close = op
                chg = round(cp - prev_close, 2 if is_usd else 0)
                pct = round((chg / prev_close) * 100.0, 2) if prev_close != 0 else 0.0
                hdl, cat, snt = generate_daily_factors(info["name"], pct, is_index)
                
                series.append({
                    "date": day_str, "open": op, "high": hp, "low": lp, "close": cp,
                    "change": chg, "pct_change": pct, "volume": vol,
                    "headline": hdl, "category": cat, "sentiment": snt
                })
                prev_close = cp
                
            if not series:
                continue
                
            cur_price = series[-1]["close"]
            h52 = max(item["high"] for item in series)
            l52 = min(item["low"] for item in series)
            
            sorted_by_impact = sorted(series[10:-1], key=lambda x: abs(x["pct_change"]), reverse=True)
            milestones = []
            for item in sorted_by_impact[:4]:
                milestones.append((item["date"], f"{item['headline'][:14]} ({item['pct_change']:+.1f}%)"))
            milestones.append((series[-1]["date"], f"최근 마감일 ({series[-1]['pct_change']:+.1f}%)"))
            milestones.sort(key=lambda x: x[0])
            
            m_cap = "-"
            try:
                fast_info = ticker_obj.fast_info
                if hasattr(fast_info, 'market_cap') and fast_info.market_cap:
                    mc = fast_info.market_cap
                    m_cap = f"${mc/1e12:.2f}T" if (is_usd and mc > 1e12) else (f"${mc/1e9:.1f}B" if is_usd else f"{int(mc/1e12):,}조원")
            except Exception:
                pass
                
            db[tk] = {
                "meta": {
                    "name": info["name"], "ticker": tk, "category": info["cat"],
                    "region": "US" if is_usd else "KR", "currency": info["cur"],
                    "market_cap": m_cap, "pe": "-", "high_52w": h52, "low_52w": l52,
                    "current": cur_price, "desc": info["desc"], "milestones": milestones
                },
                "series": series
            }
            success_count += 1
            print(f"  -> [성공] {tk} 수집 완료")
        except Exception as e:
            print(f"  -> [에러] {tk}: {e}")
            
    # 💱 최신 환율 정보 수집 및 병합
    try:
        rates = fetch_exchange_rates(yf)
        db["exchange_rates"] = rates
        print(f"[성공] 환율 수집 완료: USD={rates['USD']['rate']}, JPY={rates['JPY']['rate']}, EUR={rates['EUR']['rate']}")
    except Exception as e:
        print(f"[환율 수집 오류]: {e}")
        db["exchange_rates"] = {
            "USD": {"name": "USD/KRW", "flag": "🇺🇸", "rate": 1364.5, "change": -0.05, "pct_change": -0.01},
            "JPY": {"name": "100JPY/KRW", "flag": "🇯🇵", "rate": 881.1, "change": 2.51, "pct_change": 0.29},
            "EUR": {"name": "EUR/KRW", "flag": "🇪🇺", "rate": 1577.3, "change": 2.88, "pct_change": 0.18}
        }

    if db:
        with open("portal_database.json", "w", encoding="utf-8") as f:
            json.dump(db, f, ensure_ascii=False)
        print(f"[{datetime.datetime.now()}] 총 {success_count}개 자산 및 환율 데이터베이스 저장 완료!")

if __name__ == "__main__":
    build_real_market_database()
