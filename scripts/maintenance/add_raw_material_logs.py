#!/usr/bin/env python3
"""
원초 로그 테이블 추가
"""
import sqlite3

def add_raw_material_logs():
    conn = sqlite3.connect('yemat.db')
    cursor = conn.cursor()
    
    print("=== 원초 로그 테이블 생성 ===\n")
    
    # 원초 로그 테이블 생성
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS raw_material_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            raw_material_id INTEGER NOT NULL,
            type TEXT NOT NULL,
            quantity REAL NOT NULL,
            note TEXT,
            production_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by TEXT,
            FOREIGN KEY (raw_material_id) REFERENCES raw_materials(id),
            FOREIGN KEY (production_id) REFERENCES productions(id)
        )
    ''')
    print("✅ raw_material_logs 테이블 생성 완료")
    
    conn.commit()
    conn.close()
    
    print("\n=== 원초 로그 시스템 준비 완료 ===")

if __name__ == "__main__":
    add_raw_material_logs()
