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
    #연산자 매핑
    op_map = {"gte": ">=", "lte": "<=", "between": "BETWEEN"}
    sql_operator = op_map.get(operator.lower())
    if not sql_operator:
        raise HTTPException(status_code=400, detail="Invalid operator")

    #===================조건 파라미터===================
    conditions = ["dp.date = :date"]
    params = {"date": date}

    #시장 조건 추가
    if market:
        conditions.append("s.market = :market")
        params["market"] = market

    #==================메트릭별 테이블 정의==================
    daily_metrics = ["close_price", "volume", "change_rate"]
    technical_metrics = ["volume_ratio"]

    #metric의 테이블 및 컬럼 선택
    if metric in technical_metrics:
        metric_table = "ti"
        join_tech = "JOIN technical_indicators ti ON ti.stock_id = s.stock_id AND ti.date = dp.date"
        metric_expr = f"{metric_table}.{metric}"
    else:
        metric_table = "dp"
        join_tech = ""
        metric_expr = f"{metric_table}.{metric}" if metric in daily_metrics else "dp.close_price"

    #==================메트릭별 조회 조건 정의==================
    #거래량 > 전날 대비 비율 계산
    join_prev = ""
    if compare_prev and metric == "volume":
        metric_expr = "(dp.volume / prev.volume) * 100"
        join_prev = "JOIN daily_prices prev ON prev.stock_id = dp.stock_id AND prev.date = DATE_SUB(dp.date, INTERVAL 1 DAY)"

    #메트릭 조건 추가
    if sql_operator == "BETWEEN" and value2 is not None:
        conditions.append(f"{metric_expr} BETWEEN :v1 AND :v2")
        params.update({"v1": value, "v2": value2})
    else:
        conditions.append(f"{metric_expr} {sql_operator} :v1")
        params["v1"] = value

    #추가 조건 (extra_metric)
    extra_select = ""
    if extra_metric and extra_operator and extra_value is not None:
        extra_op = op_map.get(extra_operator.lower())
        if not extra_op:
            raise HTTPException(status_code=400, detail="Invalid extra_operator")

        # extra_metric의 테이블 결정
        if extra_metric in technical_metrics:
            extra_expr = f"ti.{extra_metric}"
            if "technical_indicators" not in join_tech:
                join_tech = "JOIN technical_indicators ti ON ti.stock_id = s.stock_id AND ti.date = dp.date"
        else:
            extra_expr = f"dp.{extra_metric}"

        conditions.append(f"{extra_expr} {extra_op} :v_extra")
        params["v_extra"] = extra_value
        extra_select = f", {extra_expr}"

    #==================SQL 쿼리 작성==================
    #SELECT, WHERE문 구성 
    select_clause = f"s.name AS stock_name, {metric_expr}{extra_select}"
    where_clause = " AND ".join(conditions)

    #SQL 조합
    query = text(f"""
        SELECT {select_clause}
        FROM daily_prices dp
        JOIN stocks s ON dp.stock_id = s.stock_id
        {join_prev}
        {join_tech}
        WHERE {where_clause}
        ORDER BY {metric_expr} DESC
    """)

    #실행
    with engine.connect() as conn:
        rows = conn.execute(query, params).mappings().fetchall()

    return [dict(row) for row in rows] if rows else {"value": None}
