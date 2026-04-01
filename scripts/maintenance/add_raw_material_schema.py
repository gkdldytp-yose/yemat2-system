#!/usr/bin/env python3
"""
원초 관리 시스템 DB 스키마 추가
"""
import sqlite3

def add_raw_material_schema():
    conn = sqlite3.connect('yemat.db')
    cursor = conn.cursor()
    
    print("=== 원초 관리 테이블 생성 ===\n")
    
    # 원초 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            sheets_per_sok INTEGER NOT NULL,
            receiving_date DATE,
            car_number TEXT,
            total_stock REAL DEFAULT 0,
            current_stock REAL DEFAULT 0,
            used_quantity REAL DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    print("✅ raw_materials 테이블 생성 완료")
    
    # BOM 테이블에 raw_material_id 컬럼 추가
    try:
        cursor.execute('ALTER TABLE bom ADD COLUMN raw_material_id INTEGER')
        print("✅ bom 테이블에 raw_material_id 컬럼 추가")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("ℹ️  raw_material_id 컬럼 이미 존재")
        else:
            print(f"⚠️  {e}")
    
    # BOM 테이블에 sok_per_box 컬럼 추가 (속수)
    try:
        cursor.execute('ALTER TABLE bom ADD COLUMN sok_per_box REAL')
        print("✅ bom 테이블에 sok_per_box 컬럼 추가 (1box당 속수)")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("ℹ️  sok_per_box 컬럼 이미 존재")
        else:
            print(f"⚠️  {e}")
    
    # production_material_usage에 yield_rate 컬럼 추가 (수율)
    try:
        cursor.execute('ALTER TABLE production_material_usage ADD COLUMN yield_rate REAL')
        print("✅ production_material_usage 테이블에 yield_rate 컬럼 추가 (수율)")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("ℹ️  yield_rate 컬럼 이미 존재")
        else:
            print(f"⚠️  {e}")
    
    conn.commit()
    conn.close()
    
    print("\n=== 원초 관리 시스템 DB 준비 완료 ===")

if __name__ == "__main__":
    add_raw_material_schema()
