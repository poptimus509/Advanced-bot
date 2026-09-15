"""Conservative 1M strategy for 60-second signal generation."""
import math
import logging
from typing import Tuple, Dict, Any, Optional, List
import numpy as np
import pandas as pd

logger = logging.getLogger("QuotexSignalBoard.Strategy")

def prepare_history(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty: return pd.DataFrame() if df is None else df.copy()
    out = df.copy()
    for c in ["open", "high", "low", "close"]:
        if c in out: out[c] = pd.to_numeric(out[c], errors="coerce")
    if "time" in out:
        out["time"] = pd.to_numeric(out["time"], errors="coerce")
        out = out.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
        if len(out) > 1 and out.time.diff().dropna().max() > 60:
            out = out.iloc[out.time.diff().idxmax():].reset_index(drop=True)
    if not {"open", "high", "low", "close"}.issubset(out.columns): return out
    out = out.dropna(subset=["open", "high", "low", "close"])
    valid = (out.high >= out[["open", "close"]].max(axis=1)) & (out.low <= out[["open", "close"]].min(axis=1))
    out.loc[~valid, ["open", "high", "low", "close"]] = np.nan
    out = out.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    out["ema_9"] = out.close.ewm(span=9, adjust=False).mean(); out["ema_21"] = out.close.ewm(span=21, adjust=False).mean(); out["ema_50"] = out.close.ewm(span=50, adjust=False).mean()
    d = out.close.diff(); gain = d.clip(lower=0).rolling(14, min_periods=14).mean(); loss = (-d.clip(upper=0)).rolling(14, min_periods=14).mean()
    out["rsi"] = (100 - 100 / (1 + gain / loss.replace(0, np.nan))).fillna(50.0)
    tr = pd.concat([out.high-out.low, (out.high-out.close.shift()).abs(), (out.low-out.close.shift()).abs()], axis=1).max(axis=1)
    out["atr"] = tr.rolling(14, min_periods=1).mean()
    return out

def confirmed_swings(df: pd.DataFrame, left=2, right=2) -> List[Dict[str, Any]]:
    if df is None or len(df) < left+right+1: return []
    result=[]
    for i in range(left, len(df)-right):
        window=df.iloc[i-left:i+right+1]
        if df.iloc[i].high == window.high.max(): result.append({"kind":"HIGH","price":float(df.iloc[i].high),"confirmed_index":i+right})
        if df.iloc[i].low == window.low.min(): result.append({"kind":"LOW","price":float(df.iloc[i].low),"confirmed_index":i+right})
    return result

def detect_market_structure(df):
    s=confirmed_swings(df.tail(100).reset_index(drop=True)); hi=[x["price"] for x in s if x["kind"]=="HIGH"]; lo=[x["price"] for x in s if x["kind"]=="LOW"]
    if len(hi)>=2 and len(lo)>=2 and hi[-1]>hi[-2] and lo[-1]>lo[-2]: return "HH_HL",hi[-1],lo[-1]
    if len(hi)>=2 and len(lo)>=2 and hi[-1]<hi[-2] and lo[-1]<lo[-2]: return "LH_LL",hi[-1],lo[-1]
    return "MIXED",max(hi,default=float(df.close.max())),min(lo,default=float(df.close.min()))

def detect_m5_trend(df):
    if len(df)<25:return "NEUTRAL"
    r=df.iloc[-1]
    if r.close>r.ema_21>=r.ema_50:return "BULLISH"
    if r.close<r.ema_21<=r.ema_50:return "BEARISH"
    return "NEUTRAL"

def activity_pressure(df):
    if "verified_ticks" not in df.columns:return {"activity_status":"UNAVAILABLE","pressure":None}
    cur=float(df.iloc[-1].verified_ticks); base=float(df.iloc[:-1].verified_ticks.tail(20).mean() or 0)
    return {"activity_status":"LIVE_TICK_PROXY","pressure":cur-base,"relative_activity":round(cur/base,2) if base else 1.0}

def candle_reaction(current, previous, bullish=True):
    if bullish and current.close<=current.open:return None
    if not bullish and current.close>=current.open:return None
    return "BULLISH" if bullish else "BEARISH"

def evaluate_strategy(df, df_5m=None, df_15m=None) -> Tuple[str,int,str,Dict[str,Any]]:
    if df is None or len(df)<30:return "NO_TRADE",0,"WAIT",{"score_reason":"INSUFFICIENT_DATA","structure":"MIXED","trend_5m":"UNAVAILABLE","call_score":0,"put_score":0,"atr":0.0001,"setup_id":"NONE","rank_strength":0,"pa":"NEUTRAL"}
    w=prepare_history(df)
    if len(w)<30:return "NO_TRADE",0,"WAIT",{"score_reason":"INVALID_OR_GAPPED_DATA","signal_candle_epoch":int(df.iloc[-1].time) if "time" in df else 0}
    r=w.iloc[-1]; structure,_,_=detect_market_structure(w); trend=detect_m5_trend(w); atr=max(float(r.atr),1e-9); body=abs(r.close-r.open); rng=max(r.high-r.low,1e-9); bull=r.close>r.open and body/rng>=.45; bear=r.close<r.open and body/rng>=.45
    recent=w.tail(50); resistance=float(recent.high.quantile(.90)); support=float(recent.low.quantile(.10)); near_res=r.close>=resistance-.35*atr; near_sup=r.close<=support+.35*atr
    call=put=0; reasons=[]
    if structure=="HH_HL":call+=3;reasons.append("BULLISH_STRUCTURE")
    elif structure=="LH_LL":put+=3;reasons.append("BEARISH_STRUCTURE")
    if bull:call+=2;reasons.append("BULLISH_CLOSE")
    if bear:put+=2;reasons.append("BEARISH_CLOSE")
    if r.close>r.ema_9>r.ema_21:call+=1
    elif r.close<r.ema_9<r.ema_21:put+=1
    if 52<=r.rsi<=70:call+=1
    elif 30<=r.rsi<=48:put+=1
    if trend=="BULLISH":call+=1
    elif trend=="BEARISH":put+=1
    if near_res:call=0;reasons.append("CALL_BLOCKED_NEAR_RESISTANCE")
    if near_sup:put=0;reasons.append("PUT_BLOCKED_NEAR_SUPPORT")
    score=max(call,put); direction="CALL" if call>=7 and call>put and bull and not near_res else "PUT" if put>=7 and put>call and bear and not near_sup else "NO_TRADE"
    quality="A_PLUS" if score>=8 else "STANDARD" if score>=7 else "WAIT"
    details={"score_reason":"+".join(reasons[-4:]) or "WAITING_SETUP","structure":structure,"trend_5m":trend,"call_score":min(call,10),"put_score":min(put,10),"atr":atr,"setup_id":f"{direction}_{int(r.time)}_{score}","rank_strength":score*10,"pa":"BULLISH" if call>put else "BEARISH" if put>call else "NEUTRAL","activity_status":"ACTIVE","relative_activity":1.0,"signal_candle_epoch":int(r.time)}
    return direction,score,quality,details
