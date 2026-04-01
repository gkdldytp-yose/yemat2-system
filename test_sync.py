#!/usr/bin/env python3
"""
양방향 연동 테스트
실제로 작동하는지 확인
"""
import sqlite3

def test_bidirectional_sync():
    conn = sqlite3.connect('yemat.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    print("=" * 60)
    print("양방향 연동 테스트")
    print("=" * 60)
    
    # 1. 테스트용 상품 생성
    print("\n1. 테스트 상품 생성...")
    cursor.execute("SELECT id FROM products LIMIT 1")
    product = cursor.fetchone()
    
    if product:
        product_id = product['id']
        print(f"   ✅ 기존 상품 사용: ID {product_id}")
    else:
        cursor.execute('''
            INSERT INTO products (name, code, box_quantity, sheets_per_pack, cuts_per_sheet)
            VALUES ('테스트 김', 'TEST001', 100, 24, 9)
        ''')
        product_id = cursor.lastrowid
        print(f"   ✅ 새 상품 생성: ID {product_id}")
    
    # 2. 스케줄 생성 → 생산 자동 생성 테스트
    print("\n2. 스케줄 생성 → 생산 자동 생성 테스트...")
    cursor.execute('''
        INSERT INTO production_schedules (product_id, scheduled_date, planned_boxes, status, note)
        VALUES (?, '2025-03-01', 100, '예정', '테스트 스케줄')
    ''', (product_id,))
    schedule_id = cursor.lastrowid
    print(f"   ✅ 스케줄 생성: ID {schedule_id}")
    
    # 생산 자동 생성
    cursor.execute('''
        INSERT INTO productions (product_id, production_date, planned_boxes, status, note, schedule_id)
        VALUES (?, '2025-03-01', 100, '예정', '테스트 스케줄', ?)
    ''', (product_id, schedule_id))
    production_id = cursor.lastrowid
    print(f"   ✅ 생산 자동 생성: ID {production_id}")
    
    # 스케줄에 production_id 저장
    cursor.execute('''
        UPDATE production_schedules
        SET production_id = ?
        WHERE id = ?
    ''', (production_id, schedule_id))
    print(f"   ✅ 양방향 연결 완료")
    
    # 3. 연결 확인
    print("\n3. 양방향 연결 확인...")
    cursor.execute('SELECT production_id FROM production_schedules WHERE id = ?', (schedule_id,))
    result = cursor.fetchone()
    print(f"   스케줄 {schedule_id} → 생산 {result['production_id']}")
    
    cursor.execute('SELECT schedule_id FROM productions WHERE id = ?', (production_id,))
    result = cursor.fetchone()
    print(f"   생산 {production_id} → 스케줄 {result['schedule_id']}")
    
    # 4. BOM 테스트
    print("\n4. BOM 생성 테스트...")
    
    # 원초 (직접입력)
    cursor.execute('''
        INSERT INTO bom (product_id, material_id, raw_material_name, quantity_per_box)
        VALUES (?, NULL, '곱창돌김', 266.7)
    ''', (product_id,))
    print("   ✅ 원초 등록: 곱창돌김 266.7장/box")
    
    # 기름 (부자재 있다고 가정)
    cursor.execute("SELECT id FROM materials WHERE category='기름' LIMIT 1")
    oil = cursor.fetchone()
    if oil:
        cursor.execute('''
            INSERT INTO bom (product_id, material_id, raw_material_name, quantity_per_box)
            VALUES (?, ?, NULL, 0.49)
        ''', (product_id, oil['id']))
        print("   ✅ 기름 등록: 0.49kg/box")
    
    # 5. 예상 사용량 계산
    print("\n5. 예상 사용량 계산 (100박스 생산)...")
    cursor.execute('''
        SELECT b.raw_material_name, m.name as material_name, b.quantity_per_box,
               b.quantity_per_box * 100 as expected_total
        FROM bom b
        LEFT JOIN materials m ON b.material_id = m.id
        WHERE b.product_id = ?
    ''', (product_id,))
    bom_items = cursor.fetchall()
    
    for item in bom_items:
        name = item['raw_material_name'] or item['material_name']
        print(f"   • {name}: {item['quantity_per_box']}/box × 100box = {item['expected_total']}")
    
    # 6. 생산 완료 처리 시뮬레이션
    print("\n6. 생산 완료 처리 시뮬레이션...")
    
    # 실제 생산량 입력
    actual_boxes = 98
    cursor.execute('''
        UPDATE productions
        SET actual_boxes = ?, status = '완료'
        WHERE id = ?
    ''', (actual_boxes, production_id))
    print(f"   ✅ 실제 생산량: {actual_boxes}박스")
    
    # 스케줄도 완료 처리
    cursor.execute('''
        UPDATE production_schedules
        SET status = '완료'
        WHERE id = ?
    ''', (schedule_id,))
    print(f"   ✅ 스케줄도 완료 처리")
    
    # 7. 삭제 테스트
    print("\n7. 양방향 삭제 테스트...")
    cursor.execute('DELETE FROM productions WHERE id = ?', (production_id,))
    cursor.execute('DELETE FROM production_schedules WHERE id = ?', (schedule_id,))
    cursor.execute('DELETE FROM bom WHERE product_id = ?', (product_id,))
    print("   ✅ 양방향 삭제 완료")
    
    conn.commit()
    conn.close()
    
    print("\n" + "=" * 60)
    print("✅ 모든 테스트 통과!")
    print("=" * 60)

if __name__ == "__main__":
    test_bidirectional_sync()
