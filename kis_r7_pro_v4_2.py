# -*- coding: utf-8 -*-
"""
Report7 완성본 v4.2
- out/ 자동 초기화
- 유니버스 수집(KRX) + KIS API(토큰/일봉/현재가)
- 4개 세부필터 각각 Top50 CSV + union 원시 + 스코어 + 트래커(xlsx)
- 종가기준: MA5/고점-3% 조건 계산 (일봉 데이터 기준)
"""

import os
import sys
import io
import re
import json
import time
import math
import shutil
import errno
import random
import string
import requests
import pandas as pd
from datetime import datetime, timedelta

# -------------------- 공통 설정 --------------------
OUT_DIR = "out"
KRX_CACHE = "master/krx_universe.csv"
DEFAULT_DOMAIN = "https://openapi.koreainvestment.com:9443"

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
DOMAIN = os.getenv("KIS_DOMAIN", DEFAULT_DOMAIN)

# -------------------- out/ 초기화 --------------------
def init_out_dir():
    if os.path.exists(OUT_DIR):
        try:
            shutil.rmtree(OUT_DIR)
            print("[INFO] 기존 out 폴더 삭제 완료")
        except PermissionError:
            print("[경고] out 폴더 삭제 실패 - 파일 잠김. 대체 저장 로직으로 진행")
    os.makedirs(OUT_DIR, exist_ok=True)
    print("[INFO] 새 out 폴더 생성 완료")

# -------------------- 안전 저장 도우미 --------------------
def safe_to_csv(df: pd.DataFrame, path: str, **kw):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        df.to_csv(path, **kw)
    except PermissionError:
        base, ext = os.path.splitext(path)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt = f"{base}_{ts}{ext}"
        df.to_csv(alt, **kw)
        print(f"[경고] {path} 잠금/권한 문제 → {alt} 로 대체 저장")

def safe_to_xlsx(writer_path: str, sheet_name: str, df: pd.DataFrame):
    try:
        with pd.ExcelWriter(writer_path, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name=sheet_name)
    except PermissionError:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt = writer_path.replace(".xlsx", f"_{ts}.xlsx")
        with pd.ExcelWriter(alt, engine="xlsxwriter") as w:
            df.to_excel(w, index=False, sheet_name=sheet_name)
        print(f"[경고] {writer_path} 잠금 → {alt} 로 대체 저장")

# -------------------- KRX 유니버스 --------------------
def fetch_krx_universe(cache_path=KRX_CACHE):
    """
    KRX 전체 종목 코드(6자리) 수집
    - 캐시 우선, 실패 시 웹 파싱(lxml → bs4 순차 시도)
    """
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)
    # 캐시
    if os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path, dtype=str)
            if "ticker" in df.columns and len(df) > 0:
                print(f"[KRX] 캐시 사용: {len(df)} 종목")
                return df["ticker"].astype(str).str.zfill(6).tolist()
        except Exception:
            pass

    url = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
        html = resp.text
        # lxml → 실패 시 bs4
        try:
            tables = pd.read_html(html, header=0)
        except Exception:
            tables = pd.read_html(html, header=0, flavor="bs4")
        df = tables[0]
        # 종목코드 컬럼명 호환
        if "종목코드" in df.columns:
            codes = df["종목코드"].astype(str)
        else:
            first = df.columns[0]
            codes = df[first].astype(str).str.replace(r"[^0-9]", "", regex=True)
        out = pd.DataFrame({"ticker": codes.str.zfill(6)}).drop_duplicates()
        out.to_csv(cache_path, index=False, encoding="utf-8-sig")
        print(f"[KRX] 웹 수집 성공: {len(out)} 종목 (캐시 저장)")
        return out["ticker"].tolist()
    except Exception as e:
        print("[KRX 경고] 유니버스 파싱 실패:", e)

    # 최후의 보루(샘플)
    sample = ["005930","000660","035420","068270","051910","207940",
              "005380","006400","000270","105560"]
    print("[KRX 대체] 샘플 유니버스 사용:", len(sample))
    return sample

# -------------------- KIS 토큰 --------------------
def get_access_token():
    if not APP_KEY or not APP_SECRET:
        print("[에러] KIS_APP_KEY/KIS_APP_SECRET 환경변수 필요")
        sys.exit(1)
    url = f"{DOMAIN}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    data = {"grant_type":"client_credentials","appkey":APP_KEY,"appsecret":APP_SECRET}
    r = requests.post(url, headers=headers, json=data, timeout=15)
    r.raise_for_status()
    j = r.json()
    token = j.get("access_token")
    if not token:
        print("[에러] 토큰 응답:", j)
        sys.exit(1)
    return token

# -------------------- KIS 일봉/현재가 --------------------
def kis_headers(token:str, tr_id:str):
    return {
        "content-type":"application/json",
        "authorization":f"Bearer {token}",
        "appkey":APP_KEY,
        "appsecret":APP_SECRET,
        "tr_id":tr_id
    }

def get_daily_ohlcv(token:str, ticker:str, start:str, end:str):
    """
    일봉(종가/고가/저가/거래량) 조회
    - 종가기준 지표(MA5, 고점-3%) 계산에 사용
    - KIS 표준 일봉 TR (조정가 포함)
    """
    url = f"{DOMAIN}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    headers = kis_headers(token, "FHKST03010100")
    params = {
        "fid_cond_mrkt_div_code":"J",
        "fid_input_iscd": ticker,
        "fid_input_date_1": start,   # YYYYMMDD
        "fid_input_date_2": end,     # YYYYMMDD
        "fid_period_div_code":"D",
        "fid_org_adj_prc":"1"
    }
    r = requests.get(url, headers=headers, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    rows = j.get("output1") or j.get("output")
    if not rows:
        return pd.DataFrame()
    # 표준화: 날짜/종가/고가/거래량
    recs = []
    for o in rows:
        d = {
            "date": o.get("stck_bsop_date"),       # YYYYMMDD
            "close": float(o.get("stck_clpr", "0")),
            "high":  float(o.get("stck_hgpr", "0")),
            "low":   float(o.get("stck_lwpr", "0")),
            "open":  float(o.get("stck_oprc", "0")),
            "volume": float(o.get("acml_vol", "0"))
        }
        recs.append(d)
    df = pd.DataFrame(recs)
    if len(df)==0: return df
    df = df.sort_values("date")
    return df

def get_price_now(token:str, ticker:str):
    """
    현재가/누적 거래량/거래대금/종목명 등
    - 장중 집계가 필요할 때 사용 (여기서는 검증/이름 추출용)
    """
    url = f"{DOMAIN}/uapi/domestic-stock/v1/quotations/inquire-price"
    headers = kis_headers(token, "FHKST01010100")
    params = {"fid_cond_mrkt_div_code":"J", "fid_input_iscd": ticker}
    r = requests.get(url, headers=headers, params=params, timeout=10)
    r.raise_for_status()
    j = r.json()
    o = j.get("output") or {}
    name = o.get("hts_kor_isnm")
    # 누적 거래량/대금 필드명은 증권사 응답 버전에 따라 다를 수 있어 넓게 처리
    vol = float(o.get("acc_trdvol", o.get("trqu", "0")) or 0)
    val = float(o.get("acc_trdval", "0") or 0)
    close = float(o.get("stck_prpr", o.get("bstp_nmix_prpr", "0")) or 0)
    return {"name": name, "now_volume": vol, "now_value": val, "now_price": close}

# -------------------- 필터 계산 --------------------
def compute_indicators(df_daily: pd.DataFrame):
    """
    df_daily: 날짜 오름차순, 열: date, close, high, volume
    반환: ma5, above_ma5, within_3pct(고점-종가), vol_ratio(today/prev)
    기준은 '종가기준' (요청사항)
    """
    if len(df_daily) < 6:
        return None
    d = df_daily.copy()
    d["ma5"] = d["close"].rolling(5).mean()
    last = d.iloc[-1]
    prev = d.iloc[-2]
    ma5 = float(last["ma5"]) if not math.isnan(last["ma5"]) else None
    if ma5 is None: 
        return None
    above_ma5 = (last["close"] >= ma5)
    # 고점 대비 -3% 이내 (종가기준)
    within_3pct = (last["high"] - last["close"]) / max(last["high"], 1) <= 0.03
    # 전일 대비 거래량 비율 (당일/전일)
    vol_ratio = (last["volume"] / max(prev["volume"], 1))
    return dict(
        close=float(last["close"]),
        high=float(last["high"]),
        volume=float(last["volume"]),
        prev_volume=float(prev["volume"]),
        ma5=float(ma5),
        above_ma5=bool(above_ma5),
        within_3pct=bool(within_3pct),
        vol_ratio=float(vol_ratio)
    )

# -------------------- 전체 파이프라인 --------------------
def run_pipeline(limit:int=1200, top_k:int=60):
    init_out_dir()

    universe = fetch_krx_universe()
    if limit and limit < len(universe):
        universe = universe[:limit]
    print(f"[유니버스] {len(universe)} 종목")

    token = get_access_token()
    print("[INFO] 토큰 발급 완료")

    start = (datetime.today() - timedelta(days=60)).strftime("%Y%m%d")
    end   = datetime.today().strftime("%Y%m%d")

    rows = []
    for i, t in enumerate(universe, 1):
        t6 = str(t).zfill(6)
        try:
            d = get_daily_ohlcv(token, t6, start, end)
            if d is None or len(d) < 6:
                continue
            ind = compute_indicators(d)
            if not ind: 
                continue
            # 이름/체크 (네임 못받아오면 빈칸 허용)
            try:
                meta = get_price_now(token, t6)
            except Exception:
                meta = {"name": None, "now_volume": None, "now_value": None, "now_price": None}
            rows.append({
                "ticker": t6,
                "name": meta.get("name"),
                "close": ind["close"],
                "high": ind["high"],
                "volume": ind["volume"],
                "prev_volume": ind["prev_volume"],
                "vol_ratio": ind["vol_ratio"],
                "ma5": ind["ma5"],
                "above_ma5": ind["above_ma5"],
                "within_3pct": ind["within_3pct"],
                # 거래대금(종가기준 근사): close * volume
                "today_value": ind["close"] * ind["volume"]
            })
        except Exception as e:
            # 네트워크/제한/일시오류 등은 스킵
            # print(f"[스킵] {t6}: {e}")
            continue

        # 진행률 가벼운 출력
        if i % 100 == 0:
            print(f"  … 진행 {i}/{len(universe)}")

    base = pd.DataFrame(rows)
    if len(base) == 0:
        print("[에러] 수집된 데이터가 없습니다.")
        sys.exit(1)

    # 정규화
    base["ticker"] = base["ticker"].astype(str).str.zfill(6)
    # 필터 마스크
    m_vol200 = base["vol_ratio"] >= 2.0
    # 거래대금 Top N은 뒤에서 sort 후 head
    m_ma5   = base["above_ma5"] == True
    m_gap3  = base["within_3pct"] == True

    # ===== 4개 세부필터 Top50 =====
    vol200_df = base[m_vol200].sort_values("vol_ratio", ascending=False).head(50)
    value_df  = base.sort_values("today_value", ascending=False).head(50)
    ma5_df    = base[m_ma5].sort_values("close", ascending=False).head(50)
    gap3_df   = base[m_gap3].sort_values("close", ascending=False).head(50)

    safe_to_csv(vol200_df, os.path.join(OUT_DIR, "vol200_top50.csv"), index=False, encoding="utf-8-sig")
    safe_to_csv(value_df,  os.path.join(OUT_DIR, "value_top50.csv"),  index=False, encoding="utf-8-sig")
    safe_to_csv(ma5_df,    os.path.join(OUT_DIR, "ma5_top50.csv"),    index=False, encoding="utf-8-sig")
    safe_to_csv(gap3_df,   os.path.join(OUT_DIR, "gap3_top50.csv"),   index=False, encoding="utf-8-sig")

    # ===== union 원시 + 스코어 =====
    base["flag_vol200"] = m_vol200
    base["flag_value_top50"] = base["ticker"].isin(value_df["ticker"])
    base["flag_ma5"] = m_ma5
    base["flag_gap3"] = m_gap3

    # 스코어: 4개 조건 True 합
    base["meets_count"] = (
        base["flag_vol200"].astype(int) +
        base["flag_value_top50"].astype(int) +
        base["flag_ma5"].astype(int) +
        base["flag_gap3"].astype(int)
    )
    base["meets_all"] = base["meets_count"] >= 4  # 전부 충족
    union_cols = ["ticker","name","close","high","volume","prev_volume","vol_ratio","ma5",
                  "above_ma5","within_3pct","today_value",
                  "flag_vol200","flag_value_top50","flag_ma5","flag_gap3","meets_count","meets_all"]
    safe_to_csv(base[union_cols], os.path.join(OUT_DIR, "union_raw_4filters.csv"),
                index=False, encoding="utf-8-sig")

    # 후보/스코어 파일
    candidates = base.sort_values(["meets_count","today_value"], ascending=[False,False])
    safe_to_csv(candidates, os.path.join(OUT_DIR,"r7_candidates.csv"), index=False, encoding="utf-8-sig")

    scored = candidates.copy()
    # 간단 가중 스코어(원하시면 조정 가능)
    scored["score"] = (
        scored["flag_vol200"].astype(int)*1.5 +
        scored["flag_value_top50"].astype(int)*1.2 +
        scored["flag_ma5"].astype(int)*1.0 +
        scored["flag_gap3"].astype(int)*1.0 +
        (scored["today_value"]/scored["today_value"].max())*1.0
    )
    scored = scored.sort_values("score", ascending=False)
    safe_to_csv(scored, os.path.join(OUT_DIR,"r7_scored.csv"), index=False, encoding="utf-8-sig")

    # ===== 트래커(xlsx) 생성 (13개 컬럼) =====
    # 그룹 분류는 기본은 "상승예측"으로 두고, 상위 1~3개는 1/2/3로 배치
    top_list = scored.head(top_k).copy()
    top_list["group"] = "상승예측"
    if len(top_list) >= 1: top_list.iloc[0, top_list.columns.get_loc("group")] = "1"
    if len(top_list) >= 2: top_list.iloc[1, top_list.columns.get_loc("group")] = "2"
    if len(top_list) >= 3: top_list.iloc[2, top_list.columns.get_loc("group")] = "3"

    today_str = datetime.today().strftime("%Y-%m-%d")
    tracker = pd.DataFrame({
        "name": top_list["name"],
        "ticker": top_list["ticker"].apply(lambda x: f"KRX:{x}"),
        "group": top_list["group"],
        "d0": today_str,
        "vol_ok": top_list["flag_vol200"],
        "val_ok": top_list["flag_value_top50"],
        "close3_ok": top_list["flag_gap3"],
        "ma5_ok": top_list["flag_ma5"],
        "memo": "",         # 섹터/테마 메모(추후 자동 분류기로 채움)
        "remark": "",
        "select_reason": "R7 세부필터 통과",
        "success_reason": "",
        "fail_reason": ""
    })
    safe_to_xlsx(os.path.join(OUT_DIR, "r7_tracker.xlsx"), "Report7", tracker)

    print("\n[완료] 저장:")
    for f in ["vol200_top50.csv","value_top50.csv","ma5_top50.csv","gap3_top50.csv",
              "union_raw_4filters.csv","r7_candidates.csv","r7_scored.csv","r7_tracker.xlsx"]:
        print(" -", os.path.join(OUT_DIR, f))

# -------------------- CLI --------------------
def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=1200, help="유니버스 상한(테스트시 200 추천)")
    ap.add_argument("--top_k", type=int, default=60, help="스코어 상위 N (트래커 출력)")
    args = ap.parse_args()
    run_pipeline(limit=args.limit, top_k=args.top_k)

if __name__ == "__main__":
    main()
