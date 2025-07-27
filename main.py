from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi
from typing import Union, List, Optional

from routes import simple, signals, complex

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

#라우터 설정
app.include_router(simple.router)
app.include_router(signals.router)
app.include_router(complex.router)

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