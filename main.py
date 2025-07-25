from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from typing import Union, List, Optional


import pymysql
from sqlalchemy import create_engine, text

from dotenv import load_dotenv
import os

#환경변수 로드
load_dotenv()


# ==============================================================================
# 1. FastAPI 애플리케이션 설정
# ==============================================================================

app = FastAPI(
    title="Finance DB API Server",
    description="Finance DB 연결을 위한 FastAPI 서버",
    version="1.0.0",
)

# CORS 설정
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# OpenAPI 3.0.3 스키마 설정 (ClovaStudio 호환)
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        #routes=app.routes,
    )
    
    openapi_schema["openapi"] = "3.0.3"
    
    # 서버 정보 - 환경변수로 설정
    openapi_schema["servers"] = [
        {
            "url": os.getenv("API_SERVER_URL","https://port-0-finance-server-md8xojr9fed0268a.sel5.cloudtype.app"),
            "description": "Finance DB API Server"
        }
    ]
    
    # ClovaStudio 호환성을 위해 examples 속성 제거
    def remove_examples(schema_dict):
        if isinstance(schema_dict, dict):
            # examples 속성 제거
            if 'examples' in schema_dict:
                del schema_dict['examples']
            # 중첩된 객체에서도 재귀적으로 제거
            for key, value in schema_dict.items():
                remove_examples(value)
        elif isinstance(schema_dict, list):
            for item in schema_dict:
                remove_examples(item)
    
    # 전체 스키마에서 examples 제거
    remove_examples(openapi_schema)
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# ==============================================================================
# 2. AWS RDS 연결 설정
# ==============================================================================

'''USER = os.getenv("USER_NAME")
PASSWORD = os.getenv("USER_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
ENDPOINT = os.getenv("DB_ENDPOINT")
PORT = os.getenv("DB_PORT", 3306)  # 기본 포트 3306'''

DB_URL = "mysql+pymysql://alstpdusMin:Alstpdus!!@finance-db.c36egosuec87.ap-northeast-2.rds.amazonaws.com:3306/stock_data"
engine = create_engine(DB_URL, echo=True)


# ==============================================================================
# 3. 기능별 OpenAPI JSON 엔드포인트
# ==============================================================================

#1. 특정 종목의 일별 주가 정보 조회
@app.get("/stocks/info")
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
                #"message": f"{date}의 {metric} 데이터가 없습니다."
            }
        
        return {
            "stock_name": stock_name,
            "date": date,
            "metric": metric,
            "value": result[0]
        }

    

#2. 특정 시장 지수 조회
@app.get("/markets/index")
def get_market_index(market: str, date: str):
    with engine.connect() as conn:
        query = text("""
            SELECT close_index FROM market_indices
            WHERE market = :market AND date = :date
        """)
        result = conn.execute(query, {"market": market, "date": date}).fetchone()
        
        if not result:
            return {
                "value": None
            }
        
        return {
            "market": market,
            "date": date,
            "index_value": result[0]
        }
    

#3. 시장 통계 조회
@app.get("/markets/stats")
def get_market_stats(market: str, date: str, metric: str):
    with engine.connect() as conn:
        query = text(f"""
            SELECT {metric} FROM market_stats
            WHERE market = :market AND date = :date
        """)
        result = conn.execute(query, {"market": market, "date": date}).fetchone()
        
        if not result:
            return {
                "value": None
            }
        
        return {
            "market": market,
            "date": date,
            "metric": metric,
            "value": result[0]
        }


#특정 시장 내의 TopN 종목 조회
ALLOWED_METRICS = ["adjusted_close", "volume", "change", "change_rate"]

@app.get("/stocks/topn", response_model=Union[str, List[str]])
def get_topn_stocks(
    market: Optional[str] = Query(None, description="시장명"),  
    metric: str = Query(...),
    date: str = Query(...),
    topn: int=Query(...)
):
    if metric not in ALLOWED_METRICS:
        raise HTTPException(status_code=400, detail="Invalid metric name")

    with engine.connect() as conn:
        query = text(f"""
            SELECT s.name AS stock_name, dp.{metric}
            FROM daily_prices dp
            JOIN stocks s ON dp.stock_id = s.stock_id
            WHERE dp.date = :date AND s.market = :market
            ORDER BY dp.{metric} DESC
            LIMIT :topn
        """)
        results = conn.execute(query, {"date": date, "market": market, "topn": topn}).mappings().fetchall()

        if not results:
            raise HTTPException(status_code=404, detail="No data found")

        if topn == 1:
            row = results[0]
            amount = int(row[metric]) if metric == "volume" else round(row[metric], 2)
            unit = "주" if metric == "volume" else ""
            return f"{row['stock_name']}({amount}{unit})"
        else:
            return [row["stock_name"] for row in results]

    
