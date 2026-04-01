#!/usr/bin/env python3
"""
데이터베이스 완전 재생성 - WAL 모드로
"""
import sqlite3
import shutil
import os

def recreate_database():
    print("=== 데이터베이스 재생성 시작 ===\n")
    
    # 백업
    if os.path.exists('yemat.db'):
        shutil.copy('yemat.db', 'yemat_backup.db')
        print("✅ 기존 DB 백업 완료: yemat_backup.db")
    
    # WAL 파일들 삭제
    for ext in ['-wal', '-shm']:
        if os.path.exists(f'yemat.db{ext}'):
            os.remove(f'yemat.db{ext}')
            print(f"✅ 삭제: yemat.db{ext}")
    
    # 데이터베이스 재연결 및 최적화
    conn = sqlite3.connect('yemat.db', isolation_level=None, timeout=60.0)
    
    # WAL 모드 및 최적화 설정
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL') 
    conn.execute('PRAGMA busy_timeout=60000')
    conn.execute('PRAGMA wal_autocheckpoint=1000')
    conn.execute('PRAGMA cache_size=-64000')  # 64MB 캐시
    
    # 체크포인트 강제 실행
    conn.execute('PRAGMA wal_checkpoint(TRUNCATE)')
    
    print("\n✅ WAL 모드 활성화")
    print("✅ 타임아웃 60초 설정")
    print("✅ 자동 체크포인트 설정")
    
    conn.close()
    
    print("\n=== 데이터베이스 재생성 완료 ===")
    print("\n⚠️  Flask 앱을 완전히 재시작하세요!")
    print("   (Ctrl+C로 종료 후 다시 python app.py)")

if __name__ == "__main__":
    recreate_database()
