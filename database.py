

# ==============================================================================
# AWS RDS 연결 설정
# ==============================================================================


from sqlalchemy import create_engine

'''USER = os.getenv("USER_NAME")
PASSWORD = os.getenv("USER_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
ENDPOINT = os.getenv("DB_ENDPOINT")
PORT = os.getenv("DB_PORT", 3306)  # 기본 포트 3306'''

DB_URL = "mysql+pymysql://alstpdusMin:Alstpdus!!@finance-db.c36egosuec87.ap-northeast-2.rds.amazonaws.com:3306/stock_data"
engine = create_engine(DB_URL, echo=True)