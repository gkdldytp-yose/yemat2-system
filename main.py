from fastapi import FastAPI, Request, Depends, HTTPException, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
import sqlite3
import hashlib
from datetime import datetime, date, timedelta
from typing import Optional, List
import json
from pathlib import Path

app = FastAPI(title="예맛 생산관리 시스템")

# 템플릿 설정
templates = Jinja2Templates(directory="templates")
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / 'yemat.db'

# 데이터베이스 연결 함수
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn

# 세션 저장소 (간단한 버전)
sessions = {}

# 로그인 확인
def get_current_user(request: Request):
    session_id = request.cookies.get("session_id")
    if not session_id or session_id not in sessions:
        return None
    return sessions[session_id]

def require_login(request: Request):
    user = get_current_user(request)
    if not user:
        raise HTTPException(status_code=401, detail="로그인이 필요합니다")
    return user

def require_admin(request: Request):
    user = require_login(request)
    if not user.get("is_admin"):
        raise HTTPException(status_code=403, detail="관리자 권한이 필요합니다")
    return user

# 라우트
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    """메인 페이지 (대시보드)"""
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
    conn = get_db()
    cursor = conn.cursor()
    
    # 대시보드 데이터 조회
    # 1. 발주 필요 부자재 (최소 재고 이하)
    cursor.execute('''
        SELECT m.id, m.name, m.category, m.current_stock, m.min_stock, s.name as supplier_name
        FROM materials m
        LEFT JOIN suppliers s ON m.supplier_id = s.id
        WHERE m.current_stock <= m.min_stock
        ORDER BY m.category, m.name
        LIMIT 10
    ''')
    low_stock_materials = cursor.fetchall()
    
    # 2. 금주 생산 스케줄
    today = date.today()
    week_end = today + timedelta(days=7)
    cursor.execute('''
        SELECT ps.id, ps.scheduled_date, ps.planned_boxes, ps.status, ps.note,
               p.name as product_name
        FROM production_schedules ps
        LEFT JOIN products p ON ps.product_id = p.id
        WHERE ps.scheduled_date BETWEEN ? AND ?
        ORDER BY ps.scheduled_date
    ''', (today.isoformat(), week_end.isoformat()))
    schedules = cursor.fetchall()
    
    # 3. 최근 생산 통계 (최근 30일)
    days_ago_30 = today - timedelta(days=30)
    cursor.execute('''
        SELECT p.name, SUM(pr.actual_boxes) as total_boxes, COUNT(*) as production_count
        FROM productions pr
        LEFT JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.status = '완료'
        GROUP BY pr.product_id
        ORDER BY total_boxes DESC
        LIMIT 5
    ''', (days_ago_30.isoformat(),))
    production_stats = cursor.fetchall()
    
    # 4. 대기중인 발주
    cursor.execute('''
        SELECT po.id, po.order_date, po.expected_delivery_date, po.status,
               s.name as supplier_name, COUNT(poi.id) as item_count
        FROM purchase_orders po
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN purchase_order_items poi ON po.id = poi.purchase_order_id
        WHERE po.status = '발주'
        GROUP BY po.id
        ORDER BY po.order_date DESC
        LIMIT 5
    ''')
    pending_orders = cursor.fetchall()
    
    conn.close()
    
    return templates.TemplateResponse("dashboard.html", {
        "request": request,
        "user": user,
        "low_stock_materials": low_stock_materials,
        "schedules": schedules,
        "production_stats": production_stats,
        "pending_orders": pending_orders,
        "today": today
    })

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    """로그인 페이지"""
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login(username: str = Form(...), password: str = Form(...)):
    """로그인 처리"""
    conn = get_db()
    cursor = conn.cursor()
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    cursor.execute(
        "SELECT id, username, is_admin FROM users WHERE username = ? AND password_hash = ?",
        (username, password_hash)
    )
    user = cursor.fetchone()
    conn.close()
    
    if not user:
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 잘못되었습니다")
    
    # 세션 생성
    session_id = hashlib.sha256(f"{username}{datetime.now()}".encode()).hexdigest()
    sessions[session_id] = {
        "id": user[0],
        "username": user[1],
        "is_admin": bool(user[2])
    }
    
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(key="session_id", value=session_id)
    return response

@app.get("/logout")
async def logout(request: Request):
    """로그아웃"""
    session_id = request.cookies.get("session_id")
    if session_id and session_id in sessions:
        del sessions[session_id]
    
    response = RedirectResponse(url="/login", status_code=303)
    response.delete_cookie("session_id")
    return response

@app.get("/suppliers", response_class=HTMLResponse)
async def suppliers_list(request: Request, user: dict = Depends(require_login)):
    """업체 목록"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT s.*, COUNT(m.id) as material_count
        FROM suppliers s
        LEFT JOIN materials m ON s.id = m.supplier_id
        GROUP BY s.id
        ORDER BY s.name
    ''')
    suppliers = cursor.fetchall()
    conn.close()
    
    return templates.TemplateResponse("suppliers.html", {
        "request": request,
        "user": user,
        "suppliers": suppliers
    })

@app.get("/materials", response_class=HTMLResponse)
async def materials_list(request: Request, category: Optional[str] = None, user: dict = Depends(require_login)):
    """부자재 목록"""
    conn = get_db()
    cursor = conn.cursor()
    
    # 카테고리 목록
    cursor.execute("SELECT DISTINCT category FROM materials ORDER BY category")
    categories = [row[0] for row in cursor.fetchall()]
    
    # 부자재 조회
    if category:
        cursor.execute('''
            SELECT m.*, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE m.category = ?
            ORDER BY m.name
        ''', (category,))
    else:
        cursor.execute('''
            SELECT m.*, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            ORDER BY m.category, m.name
        ''')
    
    materials = cursor.fetchall()
    conn.close()
    
    return templates.TemplateResponse("materials.html", {
        "request": request,
        "user": user,
        "materials": materials,
        "categories": categories,
        "selected_category": category
    })

@app.get("/products", response_class=HTMLResponse)
async def products_list(request: Request, user: dict = Depends(require_login)):
    """상품 목록"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT p.*, COUNT(b.id) as bom_count
        FROM products p
        LEFT JOIN bom b ON p.id = b.product_id
        GROUP BY p.id
        ORDER BY p.name
    ''')
    products = cursor.fetchall()
    conn.close()
    
    return templates.TemplateResponse("products.html", {
        "request": request,
        "user": user,
        "products": products
    })

@app.get("/schedules", response_class=HTMLResponse)
async def schedules_list(request: Request, user: dict = Depends(require_login)):
    """생산 스케줄"""
    conn = get_db()
    cursor = conn.cursor()
    
    # 이번 달 스케줄
    today = date.today()
    month_start = date(today.year, today.month, 1)
    if today.month == 12:
        month_end = date(today.year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(today.year, today.month + 1, 1) - timedelta(days=1)
    
    cursor.execute('''
        SELECT ps.*, p.name as product_name
        FROM production_schedules ps
        LEFT JOIN products p ON ps.product_id = p.id
        WHERE ps.scheduled_date BETWEEN ? AND ?
        ORDER BY ps.scheduled_date
    ''', (month_start.isoformat(), month_end.isoformat()))
    schedules = cursor.fetchall()
    
    # 상품 목록 (스케줄 등록용)
    cursor.execute("SELECT id, name FROM products ORDER BY name")
    products = cursor.fetchall()
    
    conn.close()
    
    return templates.TemplateResponse("schedules.html", {
        "request": request,
        "user": user,
        "schedules": schedules,
        "products": products,
        "month_start": month_start,
        "month_end": month_end
    })

@app.get("/purchase-orders", response_class=HTMLResponse)
async def purchase_orders_list(request: Request, user: dict = Depends(require_login)):
    """발주 목록"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT po.*, s.name as supplier_name, 
               COUNT(poi.id) as item_count,
               SUM(poi.total_price) as total_amount
        FROM purchase_orders po
        LEFT JOIN suppliers s ON po.supplier_id = s.id
        LEFT JOIN purchase_order_items poi ON po.id = poi.purchase_order_id
        GROUP BY po.id
        ORDER BY po.order_date DESC
    ''')
    orders = cursor.fetchall()
    
    # 업체 목록
    cursor.execute("SELECT id, name FROM suppliers ORDER BY name")
    suppliers = cursor.fetchall()
    
    conn.close()
    
    return templates.TemplateResponse("purchase_orders.html", {
        "request": request,
        "user": user,
        "orders": orders,
        "suppliers": suppliers
    })

# API 엔드포인트들
@app.get("/api/materials/by-supplier/{supplier_id}")
async def get_materials_by_supplier(supplier_id: int, user: dict = Depends(require_login)):
    """업체별 부자재 조회 API"""
    conn = get_db()
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, code, name, unit, unit_price, current_stock
        FROM materials
        WHERE supplier_id = ?
        ORDER BY name
    ''', (supplier_id,))
    
    materials = []
    for row in cursor.fetchall():
        materials.append({
            "id": row[0],
            "code": row[1],
            "name": row[2],
            "unit": row[3],
            "unit_price": row[4],
            "current_stock": row[5]
        })
    
    conn.close()
    return materials

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
