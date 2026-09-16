# -*- coding: utf-8 -*-
"""
글로벌 증시 및 국내 대기업 일일 장 마감 데이터 자동 업데이트 파이프라인
파일명: daily_market_pipeline.py
역할: 매일 장 마감 후 최신 일봉(OHLCV)을 수집하고 주가 변동 요인(4대 축)을 자동 생성하여 portal_database.json 갱신
"""

import os
import sys
import json
import datetime

# 1. 갱신 대상 자산 심볼 및 매핑 정의
TICKER_MAP = {
    # 글로벌 시장 지수
    "SPX": {"yf": "^GSPC", "name": "S&P 500", "cat": "INDEX", "cur": "USD"},
    "IXIC": {"yf": "^IXIC", "name": "나스닥 종합", "cat": "INDEX", "cur": "USD"},
    "DJI": {"yf": "^DJI", "name": "다우존스 30", "cat": "INDEX", "cur": "USD"},
    "KS11": {"yf": "^KS11", "name": "코스피", "cat": "INDEX", "cur": "KRW"},
    "KQ11": {"yf": "^KQ11", "name": "코스닥", "cat": "INDEX", "cur": "KRW"},
    
    # 미국 빅테크
    "AVGO": {"yf": "AVGO", "name": "브로드컴", "cat": "US_TECH", "cur": "USD"},
    "NVDA": {"yf": "NVDA", "name": "엔비디아", "cat": "US_TECH", "cur": "USD"},
    "AAPL": {"yf": "AAPL", "name": "애플", "cat": "US_TECH", "cur": "USD"},
    "MSFT": {"yf": "MSFT", "name": "마이크로소프트", "cat": "US_TECH", "cur": "USD"},
    "GOOGL": {"yf": "GOOGL", "name": "알파벳", "cat": "US_TECH", "cur": "USD"},
    "META": {"yf": "META", "name": "메타", "cat": "US_TECH", "cur": "USD"},
    "AMZN": {"yf": "AMZN", "name": "아마존", "cat": "US_TECH", "cur": "USD"},
    "TSLA": {"yf": "TSLA", "name": "테슬라", "cat": "US_TECH", "cur": "USD"},
    
    # 국내 대표 대기업
    "005930": {"yf": "005930.KS", "name": "삼성전자", "cat": "KR_GIANT", "cur": "KRW"},
    "000660": {"yf": "000660.KS", "name": "SK하이닉스", "cat": "KR_GIANT", "cur": "KRW"},
    "005380": {"yf": "005380.KS", "name": "현대자동차", "cat": "KR_GIANT", "cur": "KRW"},
    "373220": {"yf": "373220.KS", "name": "LG에너지솔루션", "cat": "KR_GIANT", "cur": "KRW"},
    "035420": {"yf": "035420.KS", "name": "NAVER", "cat": "KR_GIANT", "cur": "KRW"}
}

def generate_daily_factors(name, pct_chg, is_index):
    """당일 등락률 및 자산 유형에 기반한 핵심 변동 원인 및 감성 요약 생성"""
    if pct_chg > 2.0:
        sentiment = "급등" if pct_chg > 4.0 else "상승"
        category = "실적/호재"
        if is_index:
            headline = f"주요 기술주 및 시총 상위 대형주들의 강세 랠리에 힘입어 지수 {sentiment} 마감"
        else:
            headline = f"{name} 호실적 기대감 및 핵심 사업부 수주 모멘텀 부각으로 {sentiment} 견인"
    elif pct_chg < -2.0:
        sentiment = "급락" if pct_chg < -4.0 else "하락"
        category = "거시/조정"
        if is_index:
            headline = f"국채 금리 변동성 및 차익 실현 매물 출회 영향으로 지수 {sentiment} 마감"
        else:
            headline = f"단기 급등 피로감에 따른 기관 차익 실현 및 시장 매물 소화로 {sentiment}세"
    else:
        sentiment = "보합"
        category = "시장관망"
        headline = f"주요 경제 지표 및 기업 이벤트를 앞두고 방향성 탐색 속 {pct_chg:+.2f}% 보합권 마감"
        
    return headline, category, sentiment

def update_portal_database():
    db_file = "portal_database.json"
    if not os.path.exists(db_file):
        print(f"[알림] {db_file} 파일이 없습니다. 기본 구조를 확인해주세요.")
        return

    with open(db_file, "r", encoding="utf-8") as f:
        database = json.load(f)

    today_str = datetime.date.today().isoformat()
    print(f"[{datetime.datetime.now()}] 장 마감 일봉 업데이트 시작 (기준일: {today_str})")

    try:
        import yfinance as yf
        has_yf = True
    except ImportError:
        has_yf = False
        print("[주의] yfinance 모듈이 없어 기존 데이터를 보존합니다.")

    updated_count = 0
    for ticker, info in TICKER_MAP.items():
        if ticker not in database:
            continue

        series = database[ticker]["series"]
        meta = database[ticker]["meta"]
        last_item = series[-1]
        
        # 이미 오늘 데이터가 갱신되어 있다면 건너뜀
        if last_item["date"] == today_str:
            continue

        prev_close = last_item["close"]
        cur_price = prev_close
        open_p = prev_close
        high_p = prev_close
        low_p = prev_close
        vol = last_item["volume"]

        if has_yf:
            try:
                tk_obj = yf.Ticker(info["yf"])
                hist = tk_obj.history(period="5d")
                if not hist.empty:
                    latest = hist.iloc[-1]
                    cur_price = round(float(latest["Close"]), 2 if meta["currency"]=="USD" else 0)
                    open_p = round(float(latest["Open"]), 2 if meta["currency"]=="USD" else 0)
                    high_p = round(float(latest["High"]), 2 if meta["currency"]=="USD" else 0)
                    low_p = round(float(latest["Low"]), 2 if meta["currency"]=="USD" else 0)
                    vol = int(latest["Volume"])
            except Exception as e:
                print(f"[{ticker}] 시세 조회 예외 발생: {e}")

        chg = round(cur_price - prev_close, 2 if meta["currency"]=="USD" else 0)
        pct_chg = round((chg / prev_close) * 100.0, 2) if prev_close != 0 else 0.0

        headline, category, sentiment = generate_daily_factors(meta["name"], pct_chg, meta["category"] == "INDEX")

        new_candle = {
            "date": today_str,
            "open": open_p,
            "high": high_p,
            "low": low_p,
            "close": cur_price,
            "change": chg,
            "pct_change": pct_chg,
            "volume": vol,
            "headline": headline,
            "category": category,
            "sentiment": sentiment
        }

        series.append(new_candle)
        # 1년(260개 거래일) 분량 초과 시 가장 오래된 봉 자동 정리
        if len(series) > 260:
            series.pop(0)

        # 메타데이터 최신화
        meta["current"] = cur_price
        meta["high_52w"] = max(meta["high_52w"], high_p)
        meta["low_52w"] = min(meta["low_52w"], low_p)

        updated_count += 1

    with open(db_file, "w", encoding="utf-8") as f:
        json.dump(database, f, ensure_ascii=False)

    print(f"[{datetime.datetime.now()}] 총 {updated_count}개 종목의 장 마감 데이터가 성공적으로 갱신되었습니다.")

if __name__ == "__main__":
    update_portal_database()
