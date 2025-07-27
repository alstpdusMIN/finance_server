from fastapi import APIRouter, Query, HTTPException
from sqlalchemy.sql import text
from typing import Optional, Union, List
from main import engine


router = APIRouter(prefix="/complex", tags=["Complex Query"])

