"""
기존 데이터를 모두 '2동 신관 2층'으로 변경
"""

import sqlite3

def update_workplace_to_2floor():
    conn = sqlite3.connect('yemat.db')
    cursor = conn.cursor()
    
    target_workplace = '2동 신관 2층'
    
    print("🔄 기존 데이터를 '2동 신관 2층'으로 변경 중...")
    
    tables = [
        'products',
        'raw_materials',
        'materials',
        'productions',
        'production_schedules',
        'purchase_requests'
    ]
    
    for table in tables:
        try:
            # 현재 상태 확인
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            total = cursor.fetchone()[0]
            
            if total > 0:
                # workplace 업데이트
                cursor.execute(f'''
                    UPDATE {table} 
                    SET workplace = ? 
                    WHERE workplace != ? OR workplace IS NULL
                ''', (target_workplace, target_workplace))
                
                updated = cursor.rowcount
                
                # 결과 확인
                cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE workplace = ?", (target_workplace,))
                final_count = cursor.fetchone()[0]
                
                print(f"✅ {table}: {total}행 중 {updated}행 업데이트 → 현재 {final_count}행이 '{target_workplace}'")
            else:
                print(f"⏭️  {table}: 데이터 없음")
                
        except Exception as e:
            print(f"❌ {table} 오류: {e}")
    
    conn.commit()
    
    # 최종 확인
    print("\n📊 최종 확인:")
    for table in tables:
        cursor.execute(f'''
            SELECT workplace, COUNT(*) as cnt 
            FROM {table} 
            GROUP BY workplace
        ''')
        results = cursor.fetchall()
        if results:
            print(f"\n{table}:")
            for row in results:
                print(f"  {row[0]}: {row[1]}행")
        else:
            print(f"\n{table}: 데이터 없음")
    
    conn.close()
    print("\n✅ 완료! 모든 데이터가 '2동 신관 2층'으로 설정되었습니다.")

if __name__ == '__main__':
    update_workplace_to_2floor()
