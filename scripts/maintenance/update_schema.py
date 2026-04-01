#!/usr/bin/env python3
"""
DB 스키마 업데이트: 생산 스케줄 ↔ 생산 관리 양방향 연동 + BOM 개선
"""
import sqlite3

def update_schema():
    conn = sqlite3.connect('yemat.db')
    cursor = conn.cursor()
    
    print("=== DB 스키마 업데이트 시작 ===\n")
    
    # 1. production_schedules 테이블에 production_id 컬럼 추가
    print("1. production_schedules 테이블 업데이트...")
    try:
        cursor.execute("ALTER TABLE production_schedules ADD COLUMN production_id INTEGER")
        print("   ✅ production_id 컬럼 추가 (양방향 연동)")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("   ℹ️  production_id 컬럼 이미 존재")
        else:
            print(f"   ⚠️  {e}")
    
    # 2. productions 테이블에 schedule_id 컬럼 추가
    print("\n2. productions 테이블 업데이트...")
    try:
        cursor.execute("ALTER TABLE productions ADD COLUMN schedule_id INTEGER")
        print("   ✅ schedule_id 컬럼 추가 (양방향 연동)")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("   ℹ️  schedule_id 컬럼 이미 존재")
        else:
            print(f"   ⚠️  {e}")
    
    # 3. products 테이블에 BOM 상세 정보 추가
    print("\n3. products 테이블에 BOM 상세 정보 추가...")
    product_columns = [
        ("sheets_per_pack", "INTEGER DEFAULT 24"),  # 매수
        ("cuts_per_sheet", "INTEGER DEFAULT 9"),    # 절단
    ]
    
    for col_name, col_type in product_columns:
        try:
            cursor.execute(f"ALTER TABLE products ADD COLUMN {col_name} {col_type}")
            print(f"   ✅ {col_name} 컬럼 추가")
        except sqlite3.OperationalError as e:
            if "duplicate column" in str(e).lower():
                print(f"   ℹ️  {col_name} 컬럼 이미 존재")
            else:
                print(f"   ⚠️  {e}")
    
    # 4. bom 테이블에 원초 직접입력 컬럼 추가
    print("\n4. bom 테이블에 원초 직접입력 컬럼 추가...")
    try:
        cursor.execute("ALTER TABLE bom ADD COLUMN raw_material_name TEXT")
        print("   ✅ raw_material_name 컬럼 추가 (원초 직접입력)")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("   ℹ️  raw_material_name 컬럼 이미 존재")
        else:
            print(f"   ⚠️  {e}")
    
    # 5. production_material_usage에도 원초 직접입력 컬럼 추가
    print("\n5. production_material_usage 테이블 업데이트...")
    try:
        cursor.execute("ALTER TABLE production_material_usage ADD COLUMN raw_material_name TEXT")
        print("   ✅ raw_material_name 컬럼 추가")
    except sqlite3.OperationalError as e:
        if "duplicate column" in str(e).lower():
            print("   ℹ️  raw_material_name 컬럼 이미 존재")
        else:
            print(f"   ⚠️  {e}")
    
    # 6. 기존 데이터 확인
    print("\n6. 기존 데이터 확인...")
    cursor.execute("SELECT COUNT(*) FROM production_schedules")
    schedule_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM productions")
    production_count = cursor.fetchone()[0]
    print(f"   생산 스케줄: {schedule_count}건")
    print(f"   생산 관리: {production_count}건")
    
    conn.commit()
    conn.close()
    
    print("\n=== DB 스키마 업데이트 완료 ===")
    print("\n✅ 양방향 연동 준비 완료!")
    print("✅ BOM 개선 준비 완료!")

if __name__ == "__main__":
    update_schema()
