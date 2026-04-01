"""
작업장별 관리를 위한 DB 마이그레이션
- 모든 주요 테이블에 workplace 컬럼 추가
- users 테이블에 workplaces 컬럼 추가
- 재고 이동 테이블 생성
"""

import sqlite3

def migrate():
    conn = sqlite3.connect('yemat.db')
    cursor = conn.cursor()
    
    # 작업장 목록
    workplaces = ['1동 조미', '1동 자반', '2동 신관 1층', '2동 신관 2층']
    
    print("🏭 작업장 기능 마이그레이션 시작...")
    
    # 1. 테이블별 workplace 컬럼 추가
    tables_to_migrate = [
        'products',
        'raw_materials', 
        'materials',
        'productions',
        'production_schedules',
        'purchase_requests'
    ]
    
    for table in tables_to_migrate:
        try:
            # 컬럼 존재 여부 확인
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in cursor.fetchall()]
            
            if 'workplace' not in columns:
                cursor.execute(f"ALTER TABLE {table} ADD COLUMN workplace TEXT DEFAULT '1동 조미'")
                print(f"✅ {table}.workplace 컬럼 추가")
            else:
                print(f"⏭️  {table}.workplace 이미 존재")
        except Exception as e:
            print(f"❌ {table} 오류: {e}")
    
    # 2. users 테이블에 workplaces 컬럼 추가
    try:
        cursor.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]
        
        if 'workplaces' not in columns:
            # 관리자는 모든 작업장, 일반 사용자는 1동 조미만
            cursor.execute("ALTER TABLE users ADD COLUMN workplaces TEXT DEFAULT '1동 조미'")
            
            # 관리자(is_admin=1)는 모든 작업장 접근 가능
            cursor.execute("""
                UPDATE users 
                SET workplaces = '1동 조미,1동 자반,2동 신관 1층,2동 신관 2층' 
                WHERE is_admin = 1
            """)
            
            print("✅ users.workplaces 컬럼 추가")
        else:
            print("⏭️  users.workplaces 이미 존재")
    except Exception as e:
        print(f"❌ users 오류: {e}")
    
    # 3. 재고 이동 테이블 생성
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS inventory_transfers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transfer_type TEXT NOT NULL,  -- 'raw_material' or 'material'
                item_id INTEGER NOT NULL,
                item_name TEXT NOT NULL,
                quantity REAL NOT NULL,
                from_workplace TEXT NOT NULL,
                to_workplace TEXT NOT NULL,
                reason TEXT,
                note TEXT,
                transferred_by TEXT NOT NULL,
                transferred_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'completed'  -- 'pending', 'completed', 'cancelled'
            )
        """)
        print("✅ inventory_transfers 테이블 생성")
    except Exception as e:
        print(f"❌ inventory_transfers 오류: {e}")
    
    # 4. 작업장별 요약 뷰 생성
    try:
        cursor.execute("DROP VIEW IF EXISTS workplace_summary")
        cursor.execute("""
            CREATE VIEW workplace_summary AS
            SELECT 
                workplace,
                COUNT(DISTINCT p.id) as product_count,
                COUNT(DISTINCT pr.id) as production_count,
                COUNT(DISTINCT rm.id) as raw_material_count,
                COUNT(DISTINCT m.id) as material_count
            FROM 
                (SELECT DISTINCT workplace FROM products 
                 UNION SELECT DISTINCT workplace FROM productions
                 UNION SELECT DISTINCT workplace FROM raw_materials
                 UNION SELECT DISTINCT workplace FROM materials) w
            LEFT JOIN products p ON p.workplace = w.workplace
            LEFT JOIN productions pr ON pr.workplace = w.workplace
            LEFT JOIN raw_materials rm ON rm.workplace = w.workplace
            LEFT JOIN materials m ON m.workplace = w.workplace
            GROUP BY w.workplace
        """)
        print("✅ workplace_summary 뷰 생성")
    except Exception as e:
        print(f"❌ workplace_summary 오류: {e}")
    
    conn.commit()
    
    # 5. 마이그레이션 결과 확인
    print("\n📊 마이그레이션 결과:")
    
    for table in tables_to_migrate:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(DISTINCT workplace) FROM {table}")
        wp_count = cursor.fetchone()[0]
        print(f"  {table}: {count}행, {wp_count}개 작업장")
    
    cursor.execute("SELECT username, workplaces FROM users")
    users = cursor.fetchall()
    print(f"\n👥 사용자 작업장 권한:")
    for user in users:
        print(f"  {user[0]}: {user[1]}")
    
    conn.close()
    print("\n✅ 마이그레이션 완료!")

if __name__ == '__main__':
    migrate()
