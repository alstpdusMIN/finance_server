
from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional, Union, List
from main import engine


router = APIRouter(prefix="/simple", tags=["Simple Query"])


#1. 특정 종목의 일별 주가 정보 조회
@router.get("/stocks/info")
def get_stock_info(stock_name: str, date: str, metric: str):
    with engine.connect() as conn:
        query = text(f"""
            SELECT dp.{metric}
            FROM daily_prices dp
            JOIN stocks s ON dp.stock_id = s.stock_id
            WHERE s.name = :name AND dp.date = :date
        """)
        result = conn.execute(query, {"name": stock_name, "date": date}).fetchone()

        if not result:
            return {
                "stock_name": stock_name,
                "date": date,
                "metric": metric,
                "value": None,
                "message": f"{date}의 {metric} 데이터가 없습니다.(영업일 아님)"
            }
        
        return {
            "stock_name": stock_name,
            "date": date,
            "metric": metric,
            "value": result[0]
        }

    

#2. 특정 시장 지수 조회
@router.get("/markets/index")
def get_market_index(market: str, date: str):
    with engine.connect() as conn:
        query = text("""
            SELECT close_index FROM market_indices
            WHERE market = :market AND date = :date
        """)
        result = conn.execute(query, {"market": market, "date": date}).fetchone()
        
        if not result:
            return {
                "value": None,
                "message": f"{date}의 {market} 시장 지수 데이터가 없습니다.(영업일 아님)"
            }
        
        return {
            "market": market,
            "date": date,
            "index_value": result[0]
        }
    

#3. 시장 통계 조회
@router.get("/markets/stats")
def get_market_stats(market: str, date: str, metric: str):
    with engine.connect() as conn:
        query = text(f"""
            SELECT {metric} FROM market_stats
            WHERE market = :market AND date = :date
        """)
        result = conn.execute(query, {"market": market, "date": date}).fetchone()
        
        if not result:
            return {
                "value": None,
                "message": f"{date}의 {metric} 데이터가 없습니다.(영업일 아님)"
            }

        return {
            "market": market,
            "date": date,
            "metric": metric,
            "value": f"{int(result[0]):,}"
        }


# 4. TopN 종목 리스트 조회
ALLOWED_METRICS = ["close_price", "volume", "change", "change_rate"]

@router.get("/stocks/topn", response_model=None)
def get_topn_stocks(
    market: Optional[str] = Query(None),
    metric: str = Query(...),
    date: str = Query(...),
    order: str = Query("desc", regex="^(asc|desc)$"),
    topn: int = Query(5, ge=1)
):
    if metric not in ALLOWED_METRICS:
        raise HTTPException(status_code=400, detail="Invalid metric name")

    base_query = f"""
        SELECT s.name AS stock_name, dp.{metric}
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.stock_id
        WHERE dp.date = :date
    """
    params = {"date": date, "topn": topn}
    if market:
        base_query += " AND s.market = :market"
        params["market"] = market

    order_by = "ASC" if order == "asc" else "DESC"
    base_query += f" ORDER BY dp.{metric} {order_by} LIMIT :topn"

    with engine.connect() as conn:
        results = conn.execute(text(base_query), params).mappings().fetchall()
        
        if not results:
            return {
                "value": None,
                "message": f"{date}의 {metric} 데이터가 없습니다.(영업일 아님)"
            }

        response = []
        for row in results:
            formatted_amount = (
                f"{int(row[metric]):,}"
                if metric in ["volume", "close_price"]
                else f"{round(row[metric], 2):,}"
            )
            response.append({
                "stock_name": row["stock_name"],
                "metric": metric,
                "formatted_value": formatted_amount
            })
        return response


#5. 단일 최대값 종목 조회
@router.get("/stocks/max", response_model=None)
def get_max_stock(
    market: Optional[str] = Query(None),
    metric: str = Query(...),
    date: str = Query(...)
):
    try:
        result = get_topn_stocks(market=market, metric=metric, date=date, topn=1)
        if isinstance(result, dict):
            return result
        if not result:
            return {
                "value": None,
                "message": f"{date}의 {metric} 데이터가 없습니다.(영업일 아님)"
            }

        return result[0]

    except HTTPException as e:
        if e.status_code == 404:
            return {
                "value": None,
                "message": f"{date}의 {metric} 데이터가 없습니다.(영업일 아님)"
            }
        raise