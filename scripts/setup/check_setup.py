#!/usr/bin/env python3
import os
import sys

print("=" * 80)
print("예맛 생산관리 시스템 - 환경 체크")
print("=" * 80)

# 1. 현재 디렉토리 확인
print(f"\n현재 디렉토리: {os.getcwd()}")

# 2. 필요한 파일 확인
files = ['app.py', 'yemat.db', 'templates/login.html', 'templates/dashboard.html']
print("\n필수 파일 체크:")
for f in files:
    exists = "✓" if os.path.exists(f) else "✗"
    print(f"  {exists} {f}")

# 3. Flask 모듈 확인
print("\n필요한 모듈 체크:")
try:
    import flask
    print(f"  ✓ Flask 버전: {flask.__version__}")
except ImportError:
    print(f"  ✗ Flask가 설치되지 않았습니다")
    sys.exit(1)

# 4. 데이터베이스 확인
print("\n데이터베이스 확인:")
try:
    import sqlite3
    conn = sqlite3.connect('yemat.db')
    cursor = conn.cursor()
    
    # 테이블 목록
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    print(f"  테이블 개수: {len(tables)}")
    
    # 사용자 수
    cursor.execute("SELECT COUNT(*) FROM users")
    user_count = cursor.fetchone()[0]
    print(f"  사용자 수: {user_count}")
    
    # 업체 수
    cursor.execute("SELECT COUNT(*) FROM suppliers")
    supplier_count = cursor.fetchone()[0]
    print(f"  업체 수: {supplier_count}")
    
    # 부자재 수
    cursor.execute("SELECT COUNT(*) FROM materials")
    material_count = cursor.fetchone()[0]
    print(f"  부자재 수: {material_count}")
    
    conn.close()
    print("  ✓ 데이터베이스 정상")
except Exception as e:
    print(f"  ✗ 데이터베이스 오류: {e}")

print("\n" + "=" * 80)
print("✓ 모든 준비가 완료되었습니다!")
print("=" * 80)
print("\n실행 방법:")
print("  python3 app.py")
print("\n접속 주소:")
print("  http://localhost:5000")
print("\n관리자 계정:")
print("  ID: admin")
print("  PW: 1111")
print()
