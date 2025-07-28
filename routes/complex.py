from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional
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
    #operatror 매핑
    op_map = {"gte": ">=", "lte": "<=", "between": "BETWEEN"}
    sql_operator = op_map.get(operator.lower())
    if not sql_operator:
        raise HTTPException(status_code=400, detail="Invalid operator")

    #==================조건 파라미터 정의==================
    conditions = ["dp.date = :date"]
    params = {"date": date}

    # 시장 조건 추가될 경우
    if market:
        conditions.append("s.market = :market")
        params["market"] = market

    #metric 표현식
    if compare_prev and metric == "volume":
        metric_expr = "(dp.volume / prev.volume) * 100"
        join_prev = "JOIN daily_prices prev ON prev.stock_id = dp.stock_id AND prev.date = DATE_SUB(dp.date, INTERVAL 1 DAY)"
    else:
        metric_expr = f"dp.{metric}"
        join_prev = ""

    #메트릭 조건 추가
    if sql_operator == "BETWEEN" and value2 is not None:
        conditions.append(f"{metric_expr} BETWEEN :v1 AND :v2")
        params.update({"v1": value, "v2": value2})
    else:
        conditions.append(f"{metric_expr} {sql_operator} :v1")
        params["v1"] = value

    #추가 조건 (AND)
    if extra_metric and extra_operator and extra_value is not None:
        extra_op = op_map.get(extra_operator.lower())
        if not extra_op:
            raise HTTPException(status_code=400, detail="Invalid extra_operator")
        conditions.append(f"dp.{extra_metric} {extra_op} :v_extra")
        params["v_extra"] = extra_value

    #SELECT 컬럼 동적 구성
    allowed_metrics = ["close_price", "volume", "change_rate", "volume_ratio"]
    metric_column = f"dp.{metric}" if metric in allowed_metrics else "dp.close_price"

    select_columns = ["s.name AS stock_name", "dp.date", metric_column]
    if include_market:
        select_columns.insert(1, "s.market")
    if extra_metric and extra_metric in allowed_metrics:
        select_columns.append(f"dp.{extra_metric}")

    select_clause = ", ".join(select_columns)

    #최종 SQL 조합
    where_clause = " AND ".join(conditions)
    query = text(f"""
        SELECT {select_clause}
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.stock_id
        {join_prev}
        WHERE {where_clause}
        ORDER BY {metric_column} DESC
    """)

    #실행
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().fetchall()

    return [dict(row) for row in rows] if rows else {"value": None}
