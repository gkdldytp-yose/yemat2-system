#!/usr/bin/env python3
"""
데이터베이스 락 문제 해결
"""
import sqlite3
import os

def fix_database():
    db_path = 'yemat.db'
    
    print("=== 데이터베이스 최적화 시작 ===\n")
    
    # WAL 모드로 전환
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA busy_timeout=30000')
    conn.execute('PRAGMA synchronous=NORMAL')
    conn.execute('PRAGMA cache_size=10000')
    conn.execute('PRAGMA temp_store=MEMORY')
    
    print("✅ WAL 모드 활성화")
    print("✅ busy_timeout 30초 설정")
    print("✅ 동시 접근 최적화 완료")
    
    # VACUUM으로 데이터베이스 정리
    print("\n데이터베이스 정리 중...")
    conn.execute('VACUUM')
    
    conn.commit()
    conn.close()
    
    print("\n=== 데이터베이스 최적화 완료 ===")
    print("\n⚠️  앱을 재시작하세요!")

if __name__ == "__main__":
    fix_database()
