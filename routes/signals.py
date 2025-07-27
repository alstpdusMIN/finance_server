from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional, List
from database import engine

router = APIRouter(prefix="/signals", tags=["Technical Signals"])

# 1. 거래량 급증 종목 조회
@router.get("/volume_surge", response_model=list)
def get_volume_surge(
    date: str, 
    threshold: float = Query(2.0, description="거래량 비율 임계값 (기본 2.0)")
    ):

    with engine.connect() as conn:
        query = text("""
            SELECT s.name AS stock_name, ti.volume_ratio
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date = :date AND ti.volume_ratio >= :threshold
            ORDER BY ti.volume_ratio DESC
        """)
        results = conn.execute(query, {"date": date, "threshold": threshold}).mappings().fetchall()
        formatted = [f"{row['stock_name']} ({round(row['volume_ratio'], 2)}%)" for row in results]
        return formatted if formatted else {"value": None}


# 2. 볼린저 밴드 터치 종목 조회
@router.get("/bb_band", response_model=None)
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
def get_ma_breakout(
    date: str,
    ma_type: int = Query(20, description="이동평균 기준 기간 (예: 20일)"),
    percent: float = Query(10.0, description="이동평균 대비 초과 비율 (%)")
):
    col_ma = f"ma{ma_type}"
    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name,
                   dp.close_price,
                   ti.{col_ma},
                   ((dp.close_price / ti.{col_ma}) * 100 - 100) AS percent_above
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            JOIN daily_prices dp ON dp.stock_id = s.stock_id AND dp.date = ti.date
            WHERE ti.date = :date
              AND ti.{col_ma} > 0
              AND ((dp.close_price / ti.{col_ma}) * 100 - 100) >= :percent
            ORDER BY percent_above DESC
        """)
        results = conn.execute(query, {"date": date, "percent": percent}).mappings().fetchall()
        return results if results else {"value": None}



# 4. RSI 과매도/과매수 종목 조회
@router.get("/rsi", response_model=None)
def get_rsi_signal(
    date: str,
    threshold: float = Query(..., description="RSI 임계값 (예: 70, 80 등)"),
    operator: str = Query(">=", description="비교 연산자: >= 또는 <=")
):
    # ✅ 허용된 연산자만 사용
    if operator not in [">=", "<="]:
        raise HTTPException(status_code=400, detail="Invalid operator. Use '>=' or '<='.")
    
    # ✅ SQL 조건식 동적으로 생성
    condition = f"ti.rsi {operator} :threshold"

    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name, ti.rsi
            FROM technical_indicators ti
            JOIN stocks s ON ti.stock_id = s.stock_id
            WHERE ti.date = :date AND {condition}
            ORDER BY ti.rsi DESC
        """)
        results = conn.execute(query, {"date": date, "threshold": threshold}).mappings().fetchall()
        return results if results else {"value": None}



# 5. 크로스(골든/데드) 조회
@router.get("/cross", response_model=None)
def get_cross(
    start_date: str,
    end_date: str,
    cross_type: Optional[str] = Query(None, enum=["golden", "death"], description="골든/데드 중 하나, 없으면 둘 다"),
    stock_name: str = Query(None, description="특정 종목 지정 시 횟수 반환")
):
    with engine.connect() as conn:
        params = {"start_date": start_date, "end_date": end_date}

        # ✅ 조회할 cross 종류 설정
        cross_types = [cross_type] if cross_type else ["golden", "death"]

        results = {}

        for ctype in cross_types:
            column = "golden_cross" if ctype == "golden" else "death_cross"
            base_query = f"""
                FROM technical_indicators ti
                JOIN stocks s ON ti.stock_id = s.stock_id
                WHERE ti.date BETWEEN :start_date AND :end_date
                  AND ti.{column} = 1
            """

            if stock_name:
                params["stock_name"] = stock_name
                query = text(f"""
                    SELECT s.name AS stock_name, COUNT(*) AS cross_count
                    {base_query} AND s.name = :stock_name
                    GROUP BY s.name
                """)
                row = conn.execute(query, params).mappings().fetchone()
                results[ctype] = row["cross_count"] if row else 0
            else:
                query = text(f"""
                    SELECT DISTINCT s.name AS stock_name
                    {base_query}
                """)
                rows = conn.execute(query, params).mappings().fetchall()
                results[ctype] = [r["stock_name"] for r in rows]

        return results if results else {"value": None}

