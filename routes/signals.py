from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional, List
from database import engine

router = APIRouter(prefix="/signals", tags=["Technical Signals"])

# 1. 거래량 급증 종목 조회
@router.get("/volume_surge", response_model=None)
def get_volume_surge(
    date: str, 
    threshold: float = Query(2.0, description="거래량 비율 임계값 (기본 2.0)")
    ):

    with engine.connect() as conn:
        query = text("""
            SELECT s.name AS stock_name, ti.volume_ratio, 
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date = :date AND ti.volume_ratio >= :threshold
            ORDER BY ti.volume_ratio DESC
        """)
        results = conn.execute(query, {"date": date, "threshold": threshold}).mappings().fetchall()
        return results if results else {"value": None}


# 2. 볼린저 밴드 터치 종목 조회
@router.get("/bollinger_touch", response_model=None)
def get_bollinger_touch(
    date: str, 
    band: str = Query("upper", enum=["upper", "lower"])
    ):
    column = "bb_upper_touch" if band == "upper" else "bb_lower_touch"
    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name, ti.{column}
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date = :date AND ti.{column} = 1
        """)
        results = conn.execute(query, {"date": date}).mappings().fetchall()
        return results if results else {"value": None}


# 3. MA(이동평균) 돌파 종목 조회
@router.get("/ma_breakout", response_model=None)
def get_ma_breakout(date: str, short_ma: int = Query(5), long_ma: int = Query(20)):
    col_short = f"ma_{short_ma}"
    col_long = f"ma_{long_ma}"
    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name, ti.{col_short}, ti.{col_long}
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date = :date AND ti.{col_short} > ti.{col_long}
        """)
        results = conn.execute(query, {"date": date}).mappings().fetchall()
        return results if results else {"value": None}


# 4. RSI 과매도/과매수 종목 조회
@router.get("/rsi_signal", response_model=None)
def get_rsi_signal(date: str, type: str = Query("overbought", enum=["overbought", "oversold"])):
    condition = "ti.rsi >= 70" if type == "overbought" else "ti.rsi <= 30"
    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name, ti.rsi
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date = :date AND {condition}
            ORDER BY ti.rsi DESC
        """)
        results = conn.execute(query, {"date": date}).mappings().fetchall()
        return results if results else {"value": None}


# 5. 크로스(골든/데드) 기간 내 발생 종목 조회
@router.get("/cross_in_period", response_model=None)
def get_cross_in_period(start_date: str, end_date: str, cross_type: str = Query("golden", enum=["golden", "death"])):
    column = "golden_cross" if cross_type == "golden" else "death_cross"
    with engine.connect() as conn:
        query = text(f"""
            SELECT DISTINCT s.name AS stock_name
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date BETWEEN :start_date AND :end_date AND ti.{column} = 1
        """)
        results = conn.execute(query, {"start_date": start_date, "end_date": end_date}).mappings().fetchall()
        return results if results else {"value": None}


# 6. 특정 종목의 크로스 발생 횟수 조회
@router.get("/cross_count", response_model=None)
def get_cross_count(stock_name: str, cross_type: str = Query("golden", enum=["golden", "death"])):
    column = "golden_cross" if cross_type == "golden" else "death_cross"
    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name, COUNT(*) AS cross_count
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE s.name = :stock_name AND ti.{column} = 1
            GROUP BY s.name
        """)
        result = conn.execute(query, {"stock_name": stock_name}).mappings().fetchone()
        return result if result else {"value": None}
