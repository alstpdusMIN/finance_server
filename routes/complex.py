from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional, Union, List
from database import engine


router = APIRouter(prefix="/conditional", tags=["Complex Query"])

@router.get("/condition", response_model=None)
def get_conditional_stocks(
    date: str,
    market: Optional[str] = Query(None, description="시장명(KOSPI, KOSDAQ)"),
    metric: str = Query(..., description="조건 메트릭 (close_price, volume, change_rate, volume_ratio)"),
    operator: str = Query("gte", description="비교 연산자: gte(>=), lte(<=), between(BETWEEN)"),
    value: float = Query(..., description="비교 값1 (BETWEEN일 경우 하한)"),
    value2: Optional[float] = Query(None, description="BETWEEN 상한 (BETWEEN일 때만 사용)"),
    compare_prev: bool = Query(False, description="전날 대비 비율 계산 여부"),
    extra_metric: Optional[str] = Query(None, description="두번째 조건 메트릭"),
    extra_operator: Optional[str] = Query(None),
    extra_value: Optional[float] = Query(None)
):
    #operator mapping
    op_map = {
        "gte": ">=",
        "lte": "<=",
        "between": "BETWEEN"
    }

    sql_operator = op_map.get(operator.lower())

    #==================조건별 파라미터 정의=====================
    conditions = ["dp.date = :date"]
    params = {"date": date}

    # 시장 조건
    if market:
        conditions.append("s.market = :market")
        params["market"] = market


    #=======================메트릭 조건==========================
    # 1) 거래량 변화율(전날 대비 %) 계산
    if compare_prev and metric == "volume":
        metric_expr = "(dp.volume / prev.volume) * 100"  # 전날 대비 %
        join_prev = "JOIN daily_prices prev ON prev.stock_id = dp.stock_id AND prev.date = DATE_SUB(dp.date, INTERVAL 1 DAY)"
    else:
        metric_expr = f"dp.{metric}"
        join_prev = ""

    # 2) 기본 조건
    if operator.upper() == "BETWEEN" and value2 is not None:
        conditions.append(f"{metric_expr} BETWEEN :v1 AND :v2")
        params["v1"] = value
        params["v2"] = value2
    else:
        conditions.append(f"{metric_expr} {sql_operator} :v1")
        params["v1"] = value

    # 3) 추가 조건 (AND)
    if extra_metric and extra_operator and extra_value is not None:
        extra_op = op_map.get(extra_operator.lower())
        if not extra_op:
            raise HTTPException(status_code=400, detail="Invalid extra_operator")
        conditions.append(f"dp.{extra_metric} {extra_op} :v_extra")
        params["v_extra"] = extra_value

    # 4) SQL 조합
    where_clause = " AND ".join(conditions)
    query = text(f"""
        SELECT s.name AS stock_name, s.market, dp.date, dp.close_price, dp.volume, dp.change_rate
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.stock_id
        {join_prev}
        WHERE {where_clause}
        ORDER BY dp.{metric} DESC
    """)

    # 실행
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().fetchall()

    return [dict(row) for row in rows] if rows else {"value": None}
