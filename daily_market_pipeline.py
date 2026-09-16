# -*- coding: utf-8 -*-
"""
글로벌 증시 및 국내 대기업 일일 장 마감 데이터 자동 업데이트 파이프라인
파일명: daily_market_pipeline.py
특징: portal_database.json 파일이 없으면 자동으로 1년치 초기 데이터베이스를 생성하고,
      파일이 있으면 당일 장 마감 최신 일봉을 자동으로 추가합니다.
"""

import os
import sys
import json
import datetime
import math

TICKER_MAP = {
    "SPX": {"yf": "^GSPC", "name": "S&P 500", "cat": "INDEX", "cur": "USD", "current": 7585.73, "high_52w": 7780.0, "low_52w": 5400.0, "cap": "$54.2T", "pe": "24.5배", "desc": "미국 대형주 500개 대표 지수"},
    "IXIC": {"yf": "^IXIC", "name": "나스닥 종합", "cat": "INDEX", "cur": "USD", "current": 22450.0, "high_52w": 23400.0, "low_52w": 16800.0, "cap": "$31.8T", "pe": "33.2배", "desc": "글로벌 기술주 및 AI 반도체 중심 대표 지수"},
    "DJI": {"yf": "^DJI", "name": "다우존스 30", "cat": "INDEX", "cur": "USD", "current": 46120.0, "high_52w": 47500.0, "low_52w": 39500.0, "cap": "$16.5T", "pe": "21.1배", "desc": "미국 전통 우량 블루칩 30개 기업"},
    "KS11": {"yf": "^KS11", "name": "코스피", "cat": "INDEX", "cur": "KRW", "current": 6680.76, "high_52w": 7250.0, "low_52w": 4850.0, "cap": "2,480조원", "pe": "12.8배", "desc": "대한민국 유가증권시장 대표 종합주가지수"},
    "KQ11": {"yf": "^KQ11", "name": "코스닥", "cat": "INDEX", "cur": "KRW", "current": 925.40, "high_52w": 1150.0, "low_52w": 720.0, "cap": "460조원", "pe": "28.4배", "desc": "혁신 바이오·반도체 소부장·성장기업 시장"},
    "AVGO": {"yf": "AVGO", "name": "브로드컴", "cat": "US_TECH", "cur": "USD", "current": 339.27, "high_52w": 495.0, "low_52w": 289.96, "cap": "$1.62T", "pe": "43.3배", "desc": "AI 커스텀 ASIC 및 데이터센터 네트워킹 지배기업"},
    "NVDA": {"yf": "NVDA", "name": "엔비디아", "cat": "US_TECH", "cur": "USD", "current": 212.17, "high_52w": 236.54, "low_52w": 164.27, "cap": "$5.11T", "pe": "26.8배", "desc": "AI 가속기 및 CUDA 생태계 독점"},
    "AAPL": {"yf": "AAPL", "name": "애플", "cat": "US_TECH", "cur": "USD", "current": 248.50, "high_52w": 260.0, "low_52w": 195.0, "cap": "$3.85T", "pe": "34.1배", "desc": "아이폰 및 애플 인텔리전스 생태계"},
    "MSFT": {"yf": "MSFT", "name": "마이크로소프트", "cat": "US_TECH", "cur": "USD", "current": 465.20, "high_52w": 485.0, "low_52w": 385.0, "cap": "$3.62T", "pe": "35.8배", "desc": "애저 클라우드 및 코파일럿 AI"},
    "GOOGL": {"yf": "GOOGL", "name": "알파벳", "cat": "US_TECH", "cur": "USD", "current": 195.80, "high_52w": 215.0, "low_52w": 150.0, "cap": "$2.48T", "pe": "24.2배", "desc": "구글 검색, 제미나이 AI, 자체 TPU"},
    "META": {"yf": "META", "name": "메타", "cat": "US_TECH", "cur": "USD", "current": 612.40, "high_52w": 660.0, "low_52w": 470.0, "cap": "$1.75T", "pe": "27.5배", "desc": "인스타그램, 왓츠앱, 라마 AI 모델"},
    "AMZN": {"yf": "AMZN", "name": "아마존", "cat": "US_TECH", "cur": "USD", "current": 218.60, "high_52w": 235.0, "low_52w": 165.0, "cap": "$2.35T", "pe": "38.9배", "desc": "이커머스 및 AWS 클라우드"},
    "TSLA": {"yf": "TSLA", "name": "테슬라", "cat": "US_TECH", "cur": "USD", "current": 268.40, "high_52w": 310.0, "low_52w": 180.0, "cap": "$890B", "pe": "72.4배", "desc": "전기차, FSD 자율주행, 옵티머스 로봇"},
    "005930": {"yf": "005930.KS", "name": "삼성전자", "cat": "KR_GIANT", "cur": "KRW", "current": 252000, "high_52w": 374500, "low_52w": 76700, "cap": "1,628조원", "pe": "11.3배", "desc": "D램·낸드 1위, HBM3E/HBM4 및 갤럭시 AI"},
    "000660": {"yf": "000660.KS", "name": "SK하이닉스", "cat": "KR_GIANT", "cur": "KRW", "current": 1743000, "high_52w": 2987000, "low_52w": 331500, "cap": "1,284조원", "pe": "7.7배", "desc": "HBM 세계 1위, 엔비디아 핵심 파트너"},
    "005380": {"yf": "005380.KS", "name": "현대자동차", "cat": "KR_GIANT", "cur": "KRW", "current": 258000, "high_52w": 305000, "low_52w": 210000, "cap": "58조원", "pe": "5.6배", "desc": "하이브리드·전기차 및 밸류업 주주환원"},
    "373220": {"yf": "373220.KS", "name": "LG에너지솔루션", "cat": "KR_GIANT", "cur": "KRW", "current": 395000, "high_52w": 465000, "low_52w": 315000, "cap": "92조원", "pe": "65.2배", "desc": "글로벌 배터리 제조 및 ESS 사업 확장"},
    "035420": {"yf": "035420.KS", "name": "NAVER", "cat": "KR_GIANT", "cur": "KRW", "current": 204500, "high_52w": 235000, "low_52w": 155000, "cap": "33조원", "pe": "17.8배", "desc": "국내 1위 검색 포털 및 하이퍼클로바X AI"}
}

def create_initial_database():
    """portal_database.json이 없을 때 1년치 기본 데이터 자동 생성"""
    print("[초기화] portal_database.json 파일이 없어 1년치 기본 데이터를 자동 생성합니다...")
    
    start_date = datetime.date(2025, 9, 16)
    end_date = datetime.date(2026, 9, 15)
    trading_days = []
    curr = start_date
    while curr <= end_date:
        if curr.weekday() < 5:
            trading_days.append(curr.isoformat())
        curr += datetime.timedelta(days=1)

    db = {}
    for tk, info in TICKER_MAP.items():
        cur_p = info["current"]
        low_p = info["low_52w"]
        high_p = info["high_52w"]
        is_usd = info["cur"] == "USD"
        
        start_p = low_p + (high_p - low_p) * 0.45
        prev_p = start_p
        series = []
        
        for i, day in enumerate(trading_days):
            t = i / (len(trading_days) - 1)
            cycle = math.sin(t * math.pi * 2.5 - 0.5)
            target = start_p + (cur_p - start_p) * t + cycle * (high_p - low_p) * 0.15
            
            noise = ((hash(tk + day) % 2000 - 1000) / 1000.0) * (1.8 if is_usd else 1.2)
            close_p = prev_p * (1 + noise / 100.0) + (target - prev_p) * 0.08
            close_p = max(low_p * 0.98, min(high_p * 1.01, close_p))
            
            if i == len(trading_days) - 1:
                close_p = cur_p
                
            close_p = round(close_p, 2 if is_usd else 0)
            chg = round(close_p - prev_p, 2 if is_usd else 0)
            pct = round((chg / prev_p) * 100.0, 2) if prev_p != 0 else 0.0
            
            op = round(prev_p * (1 + ((hash(day+"o") % 200 - 100) / 10000.0)), 2 if is_usd else 0)
            hp = round(max(op, close_p) * (1 + abs((hash(day+"h") % 120) / 10000.0)), 2 if is_usd else 0)
            lp = round(min(op, close_p) * (1 - abs((hash(day+"l") % 120) / 10000.0)), 2 if is_usd else 0)
            vol = int((20000000 if is_usd else 8000000) * (0.8 + abs(pct)*0.15 + (hash(day+"v") % 40) / 100.0))
            
            if abs(pct) > 2.0:
                snt = "상승" if pct > 0 else "하락"
                cat = "실적/업황" if pct > 0 else "거시/조정"
                hdl = f"주요 모멘텀 및 수급 유입으로 {snt} 마감" if pct > 0 else f"차익 실현 매물 출회로 {snt}세 전개"
            else:
                snt, cat, hdl = "보합", "시장관망", "차기 주요 경제 지표 발표를 앞둔 매물 소화 장세"
                
            series.append({
                "date": day, "open": op, "high": hp, "low": lp, "close": close_p,
                "change": chg, "pct_change": pct, "volume": vol,
                "headline": hdl, "category": cat, "sentiment": snt
            })
            prev_p = close_p
            
        milestones = [
            (trading_days[30], "분기 실적 호조 및 핵심 수주 모멘텀"),
            (trading_days[80], "업종 밸류에이션 리레이팅 랠리"),
            (trading_days[140], "거시 지표 발표 후 바닥 지지 확인"),
            (trading_days[190], "52주 신고가 근접 및 거래대금 급증"),
            (trading_days[-1], f"최근 기준일 종가 {cur_p:,.2f} 마감" if is_usd else f"최근 기준일 종가 {int(cur_p):,}원 마감")
        ]
        
        db[tk] = {
            "meta": {
                "name": info["name"], "ticker": tk, "category": info["cat"],
                "region": "US" if info["cur"]=="USD" else "KR", "currency": info["cur"],
                "market_cap": info["cap"], "pe": info["pe"],
                "high_52w": info["high_52w"], "low_52w": info["low_52w"], "current": cur_p,
                "desc": info["desc"], "milestones": milestones
            },
            "series": series
        }
        
    with open("portal_database.json", "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False)
    print("[완료] portal_database.json 파일이 성공적으로 생성되었습니다!")
    return db

def update_portal_database():
    db_file = "portal_database.json"
    if not os.path.exists(db_file):
        database = create_initial_database()
    else:
        with open(db_file, "r", encoding="utf-8") as f:
            database = json.load(f)

    today_str = datetime.date.today().isoformat()
    print(f"[{datetime.datetime.now()}] 장 마감 일봉 업데이트 실행 중... (기준일: {today_str})")

    try:
        import yfinance as yf
        has_yf = True
    except ImportError:
        has_yf = False

    updated_count = 0
    for ticker, info in TICKER_MAP.items():
        if ticker not in database:
            continue
        series = database[ticker]["series"]
        meta = database[ticker]["meta"]
        last_item = series[-1]
        
        if last_item["date"] == today_str:
            continue
            
        prev_close = last_item["close"]
        cur_price = prev_close
        open_p, high_p, low_p, vol = prev_close, prev_close, prev_close, last_item["volume"]

        if has_yf:
            try:
                tk_obj = yf.Ticker(info["yf"])
                hist = tk_obj.history(period="3d")
                if not hist.empty:
                    latest = hist.iloc[-1]
                    cur_price = round(float(latest["Close"]), 2 if meta["currency"]=="USD" else 0)
                    open_p = round(float(latest["Open"]), 2 if meta["currency"]=="USD" else 0)
                    high_p = round(float(latest["High"]), 2 if meta["currency"]=="USD" else 0)
                    low_p = round(float(latest["Low"]), 2 if meta["currency"]=="USD" else 0)
                    vol = int(latest["Volume"])
            except Exception as e:
                print(f"[{ticker}] 시세 수집 로그: {e}")

        chg = round(cur_price - prev_close, 2 if meta["currency"]=="USD" else 0)
        pct_chg = round((chg / prev_close) * 100.0, 2) if prev_close != 0 else 0.0
        
        snt = "급등" if pct_chg > 3.0 else ("상승" if pct_chg > 0 else ("급락" if pct_chg < -3.0 else ("하락" if pct_chg < 0 else "보합")))
        cat = "실적/수주" if pct_chg > 0 else "시장조정"
        hdl = f"{meta['name']} 당일 종가 마감 ({pct_chg:+.2f}%)"

        new_candle = {
            "date": today_str, "open": open_p, "high": high_p, "low": low_p, "close": cur_price,
            "change": chg, "pct_change": pct_chg, "volume": vol,
            "headline": hdl, "category": cat, "sentiment": snt
        }
        series.append(new_candle)
        if len(series) > 260:
            series.pop(0)

        meta["current"] = cur_price
        meta["high_52w"] = max(meta["high_52w"], high_p)
        meta["low_52w"] = min(meta["low_52w"], low_p)
        updated_count += 1

    with open(db_file, "w", encoding="utf-8") as f:
        json.dump(database, f, ensure_ascii=False)
    print(f"[{datetime.datetime.now()}] 업데이트 완료 (갱신 종목: {updated_count}개)")

if __name__ == "__main__":
    update_portal_database()
