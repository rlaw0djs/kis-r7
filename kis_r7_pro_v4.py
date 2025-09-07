# kis_r7_pro_v4.py
"""
Report7 PRO v4
- 전 종목(코스피+코스닥) 자동 유니버스 (KRX 다운로드 → 로컬 캐시)
- 마스터 파일 불필요: 종목명/상장주식수 KIS API로 자동 수집, 섹터는 공란
- 4필터별 원시 리스트를 '항상 50종목'으로 보장 (부족시 관련 지표로 보충)
- 기존 산출물: r7_candidates.csv, r7_scored.csv, r7_tracker.xlsx
- 추가 산출물: vol200_top50.csv, value_top50.csv, ma5_top50.csv, gap3_top50.csv, union_raw_4filters.csv

실행 예:
  python kis_r7_pro_v4.py --top_k 60
옵션:
  --limit 1500        # 과도한 API 호출 방지용 상한 (기본: 전체)
  --krx_cache krx.csv # KRX 종목 캐시 경로 (기본: master/krx_universe.csv)
  --days 90           # 일봉 기간
"""

import os, sys, time, json, argparse, requests
import pandas as pd
from datetime import datetime, timedelta
from urllib.parse import urljoin

BASE_URL   = os.getenv("KIS_DOMAIN", "https://openapi.koreainvestment.com:9443")
APP_KEY    = os.getenv("KIS_APP_KEY", "")
APP_SECRET = os.getenv("KIS_APP_SECRET", "")

KRX_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

def must(v, name):
    if not v:
        print(f"[오류] {name} 없음. 환경변수 또는 .env를 설정하세요.")
        sys.exit(1)

def get_access_token():
    url = urljoin(BASE_URL, "/oauth2/tokenP")
    headers = {"content-type": "application/json; charset=UTF-8"}
    payload = {"grant_type": "client_credentials", "appkey": APP_KEY, "appsecret": APP_SECRET}
    r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=20)
    if r.status_code >= 400:
        print("[토큰요청 실패]", r.status_code, url, "\n[본문]", r.text)
        r.raise_for_status()
    return r.json()["access_token"]

def fetch_daily(token, ticker, start, end):
    """일봉"""
    path = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    url  = urljoin(BASE_URL, path)
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST03010100",
    }
    params = {
        "FID_COND_MRKT_DIV_CODE":"J",
        "FID_INPUT_ISCD": ticker,
        "FID_INPUT_DATE_1": start,
        "FID_INPUT_DATE_2": end,
        "FID_PERIOD_DIV_CODE":"D",
        "FID_ORG_ADJ_PRC":"1",
    }
    r = requests.get(url, headers=headers, params=params, timeout=20)
    r.raise_for_status()
    js = r.json()
    items = js.get("output2") or js.get("output") or []
    rows = []
    for it in items:
        try:
            rows.append({
                "date":   it.get("stck_bsop_date"),
                "open":   float(it.get("stck_oprc", "0")),
                "high":   float(it.get("stck_hgpr", "0")),
                "low":    float(it.get("stck_lwpr", "0")),
                "close":  float(it.get("stck_clpr", "0")),
                "volume": float(it.get("acml_vol", "0")),
                "value":  float(it.get("acml_tr_pbmn", "0")),
            })
        except:
            pass
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("date").reset_index(drop=True)
    return df

def fetch_stock_info(token, ticker):
    """현재가/기본정보: 종목명(hts_kor_isnm), 상장주식수(lstn_stcn)"""
    path = "/uapi/domestic-stock/v1/quotations/inquire-price"
    url  = urljoin(BASE_URL, path)
    headers = {
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "FHKST01010100",
    }
    params = {"FID_COND_MRKT_DIV_CODE":"J", "FID_INPUT_ISCD": ticker}
    r = requests.get(url, headers=headers, params=params, timeout=15)
    if r.status_code != 200:
        return {"name":"", "shares_outstanding": None}
    js = r.json()
    out = js.get("output", {})
    return {
        "name": out.get("hts_kor_isnm",""),
        "shares_outstanding": float(out.get("lstn_stcn","0"))
    }

def compute_flags(df_daily):
    if df_daily is None or df_daily.empty or len(df_daily) < 6:
        return None
    v_prev = df_daily.iloc[-2]["volume"]
    v_curr = df_daily.iloc[-1]["volume"]
    vol_ratio = None if v_prev==0 else v_curr/max(1.0, v_prev)
    vol_200 = (v_prev > 0) and (v_curr >= 2*v_prev)
    df_daily = df_daily.copy()
    df_daily["ma5"] = df_daily["close"].rolling(5).mean()
    price = df_daily.iloc[-1]["close"]
    ma5   = df_daily.iloc[-1]["ma5"]
    above_ma5 = (ma5==ma5) and (price >= ma5)
    day_high = df_daily.iloc[-1]["high"]
    last_px  = df_daily.iloc[-1]["close"]
    within_3pct = (day_high > 0) and ((day_high - last_px)/day_high <= 0.03)
    gap_pct = None if day_high==0 else ((day_high - last_px)/day_high*100)
    value = df_daily.iloc[-1]["value"]
    return dict(
        vol_ratio = vol_ratio,
        vol_200pct = vol_200,
        above_ma5 = above_ma5,
        within_3pct_of_day_high = within_3pct,
        gap_from_high_pct = None if gap_pct is None else round(gap_pct, 2),
        last_price = last_px,
        day_high = day_high,
        today_value = value,
    )

def minmax(s):
    s = pd.to_numeric(s, errors="coerce").fillna(0.0)
    lo, hi = s.min(), s.max()
    if hi - lo == 0: return s*0
    return (s - lo) / (hi - lo)

def score_rows(df, w_value, w_vol, w_gap, w_ma):
    if df.empty: return df.copy()
    cap = 10.0
    df = df.copy()
    df["value_norm"] = minmax(df["today_value"])
    df["vol_norm"]   = minmax(df["vol_ratio"].clip(upper=cap))
    df["gap_safe"]   = df["gap_from_high_pct"].abs().fillna(100.0)
    df["gap_norm"]   = 1.0 - minmax(df["gap_safe"])
    df["ma_norm"]    = df["above_ma5"].astype(int)
    df["score"] = (w_value*df["value_norm"] + w_vol*df["vol_norm"] +
                   w_gap*df["gap_norm"] + w_ma*df["ma_norm"])
    return df.sort_values(["meets_all","score","today_value"], ascending=[False,False,False])

def fetch_krx_universe(cache_path="master/krx_universe.csv"):
    """
    KRX 기업목록 다운로드 → ticker 6자리 추출 → 캐시 저장/로드
    - 우선 캐시가 있으면 그것부터 사용
    - 웹 파싱은 lxml → bs4(html5lib) 순으로 시도
    """
    import pandas as pd, os, requests, io, re
    os.makedirs(os.path.dirname(cache_path), exist_ok=True)

    # 1) 캐시 우선
    if os.path.exists(cache_path):
        try:
            df = pd.read_csv(cache_path, dtype=str)
            if "ticker" in df.columns and len(df) > 0:
                return df["ticker"].astype(str).str.zfill(6).tolist()
        except Exception:
            pass

    KRX_URL = "https://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13"

    # 2) 웹에서 테이블 파싱
    try:
        # 원문 HTML 받아오기 (일부 환경에서 read_html(url) 직접 호출이 막힐 수 있어 선요청)
        resp = requests.get(KRX_URL, timeout=20)
        resp.raise_for_status()
        html = resp.text

        # (a) lxml 있으면 그대로
        try:
            tables = pd.read_html(html, header=0)  # lxml 우선
        except Exception:
            # (b) bs4 + html5lib 시도
            tables = pd.read_html(html, header=0, flavor="bs4")

        df = tables[0]
        # 보통 '종목코드' 컬럼 존재. 없으면 숫자 6자리 추출
        if "종목코드" in df.columns:
            code = df["종목코드"].astype(str)
        else:
            # 첫 컬럼에서 숫자만 추출
            first = df.columns[0]
            code = df[first].astype(str).str.replace(r"[^0-9]", "", regex=True)
        df["ticker"] = code.str.zfill(6)
        df = df.drop_duplicates(subset=["ticker"])
        df[["ticker"]].to_csv(cache_path, index=False, encoding="utf-8-sig")
        return df["ticker"].tolist()

    except Exception as e:
        print("[경고] KRX 파싱 실패:", e)
        # 3) 마지막 방어: 캐시가 없으면 샘플 유니버스라도 반환
        SAMPLE = ["005930","000660","035420","068270","051910","207940",
                  "005380","006400","000270","105560"]
        print("[대체] 캐시/파싱 둘 다 실패 → 샘플 유니버스 사용:", len(SAMPLE))
        return SAMPLE

    # 다운로드
    df = pd.read_html(KRX_URL, header=0)[0]   # HTML 테이블 읽기
    # 보통 '종목코드', '회사명', '업종' 등이 포함됨
    code_col = "종목코드" if "종목코드" in df.columns else df.columns[0]
    df["ticker"] = df[code_col].astype(str).str.replace(r"[^0-9]", "", regex=True).str.zfill(6)
    df = df.drop_duplicates(subset=["ticker"])
    df[["ticker"]].to_csv(cache_path, index=False, encoding="utf-8-sig")
    return df["ticker"].tolist()

def ensure_topk(df, cond_mask, sort_cols, ascending, k=50):
    """조건 충족 종목이 k개 미만이면 관련 지표 정렬로 보충해서 k개 채움"""
    hit = df[cond_mask].copy()
    if len(hit) >= k:
        return hit.sort_values(sort_cols, ascending=ascending).head(k)
    # 부족분 보충: 조건 미충족 종목에서 정렬 기준으로 상위 선택
    need = k - len(hit)
    rest = df[~cond_mask].copy().sort_values(sort_cols, ascending=ascending).head(need)
    out = pd.concat([hit, rest], ignore_index=True)
    return out.sort_values(sort_cols, ascending=ascending).head(k)

def to_tracker(df, today_str):
    def map_row(r):
        return {
            "name": r.get("name",""),
            "ticker": f"KRX:{r['ticker']}",
            "group": "1" if r["meets_all"] else "상승예측",
            "d0": today_str,
            "vol_ok": bool(r["vol_200pct"]),
            "val_ok": bool(r["top50_value"]),
            "close3_ok": bool(r["within_3pct_of_day_high"]),
            "ma5_ok": bool(r["above_ma5"]),
            "memo": "",  # 섹터/테마는 ChatGPT가 채움
            "remark": f"점수:{round(r.get('score',0),3)} / 괴리:{round(r['gap_from_high_pct'],2)}% / 시총:{round((r.get('marketcap',0))/1e8,2)}억",
            "select_reason": "거래대금/거래량/5MA/고점근접(종가기준) 충족" if r["meets_all"] else "",
            "success_reason": "",
            "fail_reason": "",
        }
    cols = ["name","ticker","group","d0","vol_ok","val_ok","close3_ok","ma5_ok",
            "memo","remark","select_reason","success_reason","fail_reason"]
    return pd.DataFrame([map_row(r) for _, r in df.iterrows()], columns=cols)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--w_value", type=float, default=0.40)
    ap.add_argument("--w_vol",   type=float, default=0.25)
    ap.add_argument("--w_gap",   type=float, default=0.20)
    ap.add_argument("--w_ma",    type=float, default=0.15)
    ap.add_argument("--top_k",   type=int, default=100)
    ap.add_argument("--limit",   type=int, default=0, help="유니버스 상한(0=전체)")
    ap.add_argument("--krx_cache", default="master/krx_universe.csv")
    args = ap.parse_args()

    must(APP_KEY, "KIS_APP_KEY"); must(APP_SECRET, "KIS_APP_SECRET")

    out_dir = "out"; os.makedirs(out_dir, exist_ok=True)
    token = get_access_token()

    # 1) 유니버스: KRX 전체 (캐시)
    tickers = fetch_krx_universe(args.krx_cache)
    if args.limit and args.limit > 0:
        tickers = tickers[:args.limit]
    print(f"[유니버스] {len(tickers)} 종목")

    # 2) 수집 기준일
    today = datetime.now().strftime("%Y%m%d")
    start = (datetime.now() - timedelta(days=args.days)).strftime("%Y%m%d")

    # 3) 전 종목 스캔 (주의: 호출량 큼 → limit로 조절 가능)
    rows = []
    for i, t in enumerate(tickers, 1):
        try:
            df = fetch_daily(token, t, start, today)
            if df.empty: 
                continue
            flags = compute_flags(df)
            if not flags: 
                continue
            info = fetch_stock_info(token, t)
            marketcap = (flags["last_price"] * (info.get("shares_outstanding") or 0))
            rows.append({
                "ticker": t,
                "name": info.get("name",""),
                "shares_outstanding": info.get("shares_outstanding"),
                "marketcap": marketcap,
                "close": df.iloc[-1]["close"],
                "today_value": flags["today_value"],
                "volume": df.iloc[-1]["volume"],
                "vol_ratio": flags["vol_ratio"],
                "vol_200pct": flags["vol_200pct"],
                "above_ma5": flags["above_ma5"],
                "within_3pct_of_day_high": flags["within_3pct_of_day_high"],
                "gap_from_high_pct": flags["gap_from_high_pct"],
            })
        except Exception as e:
            # 네트워크/일시 오류는 스킵
            pass
        if i % 50 == 0:
            time.sleep(0.2)  # 과호출 방지
    base = pd.DataFrame(rows)
    if base.empty:
        print("데이터 없음"); sys.exit(0)

    # 거래대금 TOP50 플래그
    val_rank = base.sort_values("today_value", ascending=False).reset_index(drop=True)
    base["top50_value"] = base["ticker"].isin(val_rank.head(50)["ticker"])
    base["meets_all"] = base[["vol_200pct","top50_value","above_ma5","within_3pct_of_day_high"]].all(axis=1)

    # 4) 스코어링/저장
    scored = score_rows(base, args.w_value, args.w_vol, args.w_gap, args.w_ma)
    base.sort_values(["meets_all","today_value"], ascending=[False,False]).to_csv(os.path.join(out_dir,"r7_candidates.csv"), index=False, encoding="utf-8-sig")
    scored.to_csv(os.path.join(out_dir,"r7_scored.csv"), index=False, encoding="utf-8-sig")

    # 5) 트래커
    tracker = to_tracker(scored.head(args.top_k).reset_index(drop=True), datetime.now().strftime("%Y-%m-%d"))
    with pd.ExcelWriter(os.path.join(out_dir,"r7_tracker.xlsx"), engine="xlsxwriter") as w:
        tracker.to_excel(w, index=False, sheet_name="Report7")

    # 6) 원시 4세트: 항상 50종목 보장
    vol200_top50 = ensure_topk(
        scored,
        cond_mask = scored["vol_200pct"],
        sort_cols = ["vol_ratio","today_value"],
        ascending = [False, False],
        k=50
    )
    vol200_top50.to_csv(os.path.join(out_dir,"vol200_top50.csv"), index=False, encoding="utf-8-sig")

    value_top50 = val_rank.head(50).merge(scored, on="ticker", how="left")
    value_top50.to_csv(os.path.join(out_dir,"value_top50.csv"), index=False, encoding="utf-8-sig")

    ma5_top50 = ensure_topk(
        scored,
        cond_mask = scored["above_ma5"],
        sort_cols = ["today_value"],
        ascending = [False],
        k=50
    )
    ma5_top50.to_csv(os.path.join(out_dir,"ma5_top50.csv"), index=False, encoding="utf-8-sig")

    gap3_top50 = ensure_topk(
        scored,
        cond_mask = scored["within_3pct_of_day_high"],
        sort_cols = ["gap_from_high_pct","today_value"],
        ascending = [True, False],
        k=50
    )
    gap3_top50.to_csv(os.path.join(out_dir,"gap3_top50.csv"), index=False, encoding="utf-8-sig")

    union = pd.concat([vol200_top50, value_top50, ma5_top50, gap3_top50], ignore_index=True, sort=False)
    union.to_csv(os.path.join(out_dir,"union_raw_4filters.csv"), index=False, encoding="utf-8-sig")

    print("저장 완료: out/*_top50.csv + union_raw_4filters.csv + r7_*")

if __name__ == "__main__":
    main()
