from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional
from database import engine

router = APIRouter(prefix="/conditional", tags=["Complex Query"])

# ✅ 1) 기존 단일 조건 엔드포인트 유지
@router.get("/simple", response_model=None)
def get_conditional_stocks(
    date: str,
    market: Optional[str] = Query(None, description="시장명(KOSPI, KOSDAQ)"),
    metric: str = Query(..., description="조건 메트릭 (close_price, volume, change_rate, volume_ratio)"),
    operator: str = Query("gte", description="비교 연산자: gte(>=), lte(<=), between(BETWEEN)"),
    value: float = Query(..., description="비교 값1 (BETWEEN일 경우 하한)"),
    value2: Optional[float] = Query(None, description="BETWEEN 상한"),
    compare_prev: bool = Query(False, description="거래량(volume)일 때 전날 대비 비율 여부")
):
    # 연산자 매핑
    op_map = {"gte": ">=", "lte": "<=", "between": "BETWEEN"}
    sql_operator = op_map.get(operator.lower())
    if not sql_operator:
        raise HTTPException(status_code=400, detail="Invalid operator")

    # 조건 및 파라미터
    conditions = ["dp.date = :date"]
    params = {"date": date}

    # 시장 조건
    if market:
        conditions.append("s.market = :market")
        params["market"] = market

    # 메트릭 표현식
    if compare_prev and metric == "volume":
        metric_expr = "(dp.volume / prev.volume) * 100"
        join_prev = "JOIN daily_prices prev ON prev.stock_id = dp.stock_id AND prev.date = DATE_SUB(dp.date, INTERVAL 1 DAY)"
    elif metric == "volume_ratio":
        metric_expr = "ti.volume_ratio"
        join_prev = "JOIN technical_indicators ti ON ti.stock_id = s.stock_id AND ti.date = dp.date"
    else:
        metric_expr = f"dp.{metric}"
        join_prev = ""

    # 조건 추가
    if sql_operator == "BETWEEN" and value2 is not None:
        conditions.append(f"{metric_expr} BETWEEN :v1 AND :v2")
        params.update({"v1": value, "v2": value2})
    else:
        conditions.append(f"{metric_expr} {sql_operator} :v1")
        params["v1"] = value

    # SQL
    where_clause = " AND ".join(conditions)
    query = text(f"""
        SELECT s.name AS stock_name, {metric_expr}
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.stock_id
        {join_prev}
        WHERE {where_clause}
        ORDER BY {metric_expr} DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().fetchall()

    return [dict(row) for row in rows] if rows else {"value": None}


# ✅ 2) 새로운 복합 조건 엔드포인트 추가
@router.get("/complex", response_model=None)
def get_complex_conditional_stocks(
    date: str,
    market: Optional[str] = Query(None, description="시장명(KOSPI, KOSDAQ)"),
    metric1: str = Query(..., description="첫 번째 조건 메트릭"),
    operator1: str = Query("gte", description="첫 번째 조건 연산자"),
    value1: float = Query(..., description="첫 번째 조건 값1"),
    value1_2: Optional[float] = Query(None, description="첫 번째 BETWEEN 상한"),
    compare_prev1: bool = Query(False, description="첫 번째 메트릭 전날 대비 비율 여부"),
    metric2: str = Query(..., description="두 번째 조건 메트릭"),
    operator2: str = Query("gte", description="두 번째 조건 연산자"),
    value2: float = Query(..., description="두 번째 조건 값1"),
    value2_2: Optional[float] = Query(None, description="두 번째 BETWEEN 상한"),
    compare_prev2: bool = Query(False, description="두 번째 메트릭 전날 대비 비율 여부")
):
    # 연산자 매핑
    op_map = {"gte": ">=", "lte": "<=", "between": "BETWEEN"}

    def metric_expr_builder(metric, compare_prev):
        if compare_prev and metric == "volume":
            return "(dp.volume / prev.volume) * 100", "JOIN daily_prices prev ON prev.stock_id = dp.stock_id AND prev.date = DATE_SUB(dp.date, INTERVAL 1 DAY)"
        elif metric == "volume_ratio":
            return "ti.volume_ratio", "JOIN technical_indicators ti ON ti.stock_id = s.stock_id AND ti.date = dp.date"
        else:
            return f"dp.{metric}", ""

    expr1, join1 = metric_expr_builder(metric1, compare_prev1)
    expr2, join2 = metric_expr_builder(metric2, compare_prev2)

    sql_op1 = op_map.get(operator1.lower())
    sql_op2 = op_map.get(operator2.lower())
    if not sql_op1 or not sql_op2:
        raise HTTPException(status_code=400, detail="Invalid operator")

    # 조건 및 파라미터
    conditions = ["dp.date = :date"]
    params = {"date": date}

    if market:
        conditions.append("s.market = :market")
        params["market"] = market

    # 첫 번째 조건
    if sql_op1 == "BETWEEN" and value1_2 is not None:
        conditions.append(f"{expr1} BETWEEN :v1 AND :v2")
        params.update({"v1": value1, "v2": value1_2})
    else:
        conditions.append(f"{expr1} {sql_op1} :v1")
        params["v1"] = value1

    # 두 번째 조건
    if sql_op2 == "BETWEEN" and value2_2 is not None:
        conditions.append(f"{expr2} BETWEEN :v3 AND :v4")
        params.update({"v3": value2, "v4": value2_2})
    else:
        conditions.append(f"{expr2} {sql_op2} :v3")
        params["v3"] = value2

    joins = f"{join1} {join2}".strip()
    where_clause = " AND ".join(conditions)

    query = text(f"""
        SELECT s.name AS stock_name, {expr1} AS condition1, {expr2} AS condition2
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.stock_id
        {joins}
        WHERE {where_clause}
        ORDER BY {expr1} DESC
    """)

    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().fetchall()

    return [dict(row) for row in rows] if rows else {"value": None}
