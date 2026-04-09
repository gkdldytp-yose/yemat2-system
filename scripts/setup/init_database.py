import sqlite3
import pandas as pd
import hashlib
from datetime import datetime

def create_tables(conn):
    """데이터베이스 테이블 생성"""
    cursor = conn.cursor()
    
    # 사용자 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        is_admin INTEGER DEFAULT 0,
        name TEXT,
        phone TEXT,
        email TEXT,
        department TEXT,
        workplace1 TEXT,
        workplace2 TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 업체 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS suppliers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT UNIQUE,
        name TEXT NOT NULL,
        contact TEXT,
        address TEXT,
        note TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # 부자재 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS materials (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_id INTEGER,
        code TEXT UNIQUE,
        name TEXT NOT NULL,
        category TEXT,
        spec TEXT,
        unit TEXT,
        upper_unit TEXT,
        upper_unit_qty REAL,
        moq TEXT,
        lead_time TEXT,
        unit_price REAL DEFAULT 0,
        current_stock REAL DEFAULT 0,
        min_stock REAL DEFAULT 0,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
    )
    ''')
    
    # 상품 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL,
        code TEXT UNIQUE,
        description TEXT,
        box_quantity INTEGER DEFAULT 1,
        sheets_per_pack INTEGER DEFAULT 24,
        cuts_per_sheet INTEGER DEFAULT 9,
        sok_per_box REAL DEFAULT 0,
        sok_per_box_2 REAL,
        sok_per_box_3 REAL,
        sheets_per_pack_2 INTEGER,
        sheets_per_pack_3 INTEGER,
        expiry_months INTEGER DEFAULT 12,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # BOM 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS bom (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        material_id INTEGER,
        quantity_per_box REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(id),
        FOREIGN KEY (material_id) REFERENCES materials(id)
    )
    ''')
    
    # 생산 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS productions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        production_date DATE,
        planned_boxes INTEGER,
        actual_boxes INTEGER,
        status TEXT DEFAULT '계획',
        note TEXT,
        raw_sok_mode INTEGER DEFAULT 1,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    ''')
    
    # 생산별 부자재 사용 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS production_material_usage (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        production_id INTEGER,
        material_id INTEGER,
        expected_quantity REAL,
        actual_quantity REAL,
        loss_quantity REAL,
        usage_note TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (production_id) REFERENCES productions(id),
        FOREIGN KEY (material_id) REFERENCES materials(id)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS raw_material_checksheet_notes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_material_id INTEGER NOT NULL,
        use_date TEXT NOT NULL,
        note TEXT,
        created_by TEXT,
        updated_by TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(raw_material_id, use_date)
    )
    ''')
    
    # 발주 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS purchase_orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        supplier_id INTEGER,
        order_date DATE,
        expected_delivery_date DATE,
        actual_delivery_date DATE,
        status TEXT DEFAULT '발주',
        requester TEXT,
        order_number TEXT,
        note TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (supplier_id) REFERENCES suppliers(id)
    )
    ''')
    
    # 발주 항목 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS purchase_order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        purchase_order_id INTEGER,
        material_id INTEGER,
        quantity REAL,
        unit_price REAL,
        total_price REAL,
        received_quantity REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (purchase_order_id) REFERENCES purchase_orders(id),
        FOREIGN KEY (material_id) REFERENCES materials(id)
    )
    ''')
    
    # 생산 스케줄 테이블
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS production_schedules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        product_id INTEGER,
        scheduled_date DATE,
        planned_boxes INTEGER,
        status TEXT DEFAULT '예정',
        note TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (product_id) REFERENCES products(id)
    )
    ''')
    
    conn.commit()
    print("✓ 데이터베이스 테이블 생성 완료!")

def create_admin_user(conn):
    """관리자 계정 생성"""
    cursor = conn.cursor()
    
    # 기존 admin 확인
    cursor.execute("SELECT * FROM users WHERE username = 'admin'")
    if cursor.fetchone():
        print("⚠ 관리자 계정이 이미 존재합니다.")
        return
    
    # 비밀번호 해시 (간단한 버전)
    password_hash = hashlib.sha256("1111".encode()).hexdigest()
    
    cursor.execute(
        "INSERT INTO users (username, password_hash, is_admin) VALUES (?, ?, ?)",
        ("admin", password_hash, 1)
    )
    conn.commit()
    print("✓ 관리자 계정 생성 완료! (ID: admin, PW: 1111)")

def import_suppliers_and_materials(conn):
    """업체 및 부자재 데이터 import"""
    print("\n" + "=" * 80)
    print("발주.xlsx - 단가표 시트 데이터 import 시작...")
    print("=" * 80)
    
    cursor = conn.cursor()
    df = pd.read_excel('/mnt/user-data/uploads/발주.xlsx', sheet_name='단가표', header=None)
    
    suppliers_dict = {}
    materials_count = 0
    
    for idx in range(4, len(df)):
        row = df.iloc[idx]
        
        # 업체명
        supplier_name = row[2]
        if pd.isna(supplier_name) or str(supplier_name).strip() == '':
            continue
        supplier_name = str(supplier_name).strip()
        
        # 품명
        material_name = row[3]
        if pd.isna(material_name) or str(material_name).strip() == '':
            continue
        material_name = str(material_name).strip()
        
        # 업체 등록
        if supplier_name not in suppliers_dict:
            supplier_code = f"S{len(suppliers_dict)+1:05d}"
            cursor.execute(
                "INSERT INTO suppliers (code, name) VALUES (?, ?)",
                (supplier_code, supplier_name)
            )
            supplier_id = cursor.lastrowid
            suppliers_dict[supplier_name] = supplier_id
            print(f"✓ 업체 등록: {supplier_name} (코드: {supplier_code})")
        else:
            supplier_id = suppliers_dict[supplier_name]
        
        # 부자재 정보
        spec = str(row[4]) if not pd.isna(row[4]) else None
        unit = str(row[5]) if not pd.isna(row[5]) else None
        moq = str(row[13]) if not pd.isna(row[13]) and str(row[13]) != '-' else None
        lead_time = str(row[14]) if not pd.isna(row[14]) and str(row[14]) != '-' else None
        
        # 카테고리 분류
        category = "기타"
        if "내포" in material_name:
            category = "내포"
        elif "외포" in material_name:
            category = "외포"
        elif "트레이" in material_name:
            category = "트레이"
        elif "박스" in material_name:
            category = "박스"
        elif "뚜껑" in material_name:
            category = "뚜껑"
        elif "각대" in material_name:
            category = "각대"
        elif "실리카" in material_name:
            category = "실리카"
        elif "기름" in material_name or "오일" in material_name:
            category = "기름"
        elif "소금" in material_name:
            category = "소금"
        elif "김" in material_name or "원초" in material_name:
            category = "원초"
        
        material_code = f"M{materials_count+1:05d}"
        
        cursor.execute('''
            INSERT INTO materials 
            (supplier_id, code, name, category, spec, unit, moq, lead_time, unit_price, current_stock, min_stock)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0, 0)
        ''', (supplier_id, material_code, material_name, category, spec, unit, moq, lead_time))
        
        materials_count += 1
        if materials_count % 50 == 0:
            print(f"  → {materials_count}개 부자재 처리 중...")
    
    conn.commit()
    print(f"\n✓ 총 {len(suppliers_dict)}개 업체, {materials_count}개 부자재 등록 완료!")

def main():
    """메인 함수"""
    print("\n" + "=" * 80)
    print("예맛 생산관리 시스템 - 데이터베이스 초기화")
    print("=" * 80 + "\n")
    
    # 데이터베이스 연결
    conn = sqlite3.connect('yemat.db')
    
    try:
        # 1. 테이블 생성
        print("데이터베이스 테이블 생성 중...")
        create_tables(conn)
        
        # 2. 관리자 계정 생성
        print("\n" + "=" * 80)
        print("관리자 계정 생성...")
        print("=" * 80)
        create_admin_user(conn)
        
        # 3. 업체 및 부자재 데이터 import
        import_suppliers_and_materials(conn)
        
        print("\n" + "=" * 80)
        print("✓ 데이터 import 완료!")
        print("=" * 80)
        print("\n데이터베이스 파일: yemat.db")
        print("관리자 계정: admin / 1111")
        
    except Exception as e:
        print(f"\n✗ 에러 발생: {e}")
        import traceback
        traceback.print_exc()
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()
