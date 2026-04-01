"""
양방향 연동 헬퍼 함수
생산 스케줄 ↔ 생산 관리 동기화
"""
import sqlite3

def sync_schedule_to_production(conn, schedule_id):
    """
    생산 스케줄 → 생산 관리 동기화
    스케줄 등록/수정 시 호출
    """
    cursor = conn.cursor()
    
    # 스케줄 정보 조회
    cursor.execute('''
        SELECT product_id, scheduled_date, planned_boxes, note, line, production_id
        FROM production_schedules
        WHERE id = ?
    ''', (schedule_id,))
    schedule = cursor.fetchone()
    
    if not schedule:
        return None
    
    product_id, scheduled_date, planned_boxes, note, line, production_id = schedule
    
    if production_id:
        # 기존 생산 업데이트
        cursor.execute('''
            UPDATE productions
            SET production_date = ?, planned_boxes = ?, note = ?
            WHERE id = ?
        ''', (scheduled_date, planned_boxes, note, production_id))
        return production_id
    else:
        # 새 생산 등록 생성
        cursor.execute('''
            INSERT INTO productions (product_id, production_date, planned_boxes, status, note, schedule_id)
            VALUES (?, ?, ?, '예정', ?, ?)
        ''', (product_id, scheduled_date, planned_boxes, note, schedule_id))
        new_production_id = cursor.lastrowid
        
        # 스케줄에 production_id 저장
        cursor.execute('''
            UPDATE production_schedules
            SET production_id = ?
            WHERE id = ?
        ''', (new_production_id, schedule_id))
        
        return new_production_id

def sync_production_to_schedule(conn, production_id):
    """
    생산 관리 → 생산 스케줄 동기화
    생산 등록/수정 시 호출
    """
    cursor = conn.cursor()
    
    # 생산 정보 조회
    cursor.execute('''
        SELECT product_id, production_date, planned_boxes, note, schedule_id
        FROM productions
        WHERE id = ?
    ''', (production_id,))
    production = cursor.fetchone()
    
    if not production:
        return None
    
    product_id, production_date, planned_boxes, note, schedule_id = production
    
    if schedule_id:
        # 기존 스케줄 업데이트
        cursor.execute('''
            UPDATE production_schedules
            SET scheduled_date = ?, planned_boxes = ?, note = ?
            WHERE id = ?
        ''', (production_date, planned_boxes, note, schedule_id))
        return schedule_id
    else:
        # 새 스케줄 생성
        cursor.execute('''
            INSERT INTO production_schedules (product_id, scheduled_date, planned_boxes, status, note, production_id)
            VALUES (?, ?, ?, '예정', ?, ?)
        ''', (product_id, production_date, planned_boxes, note, production_id))
        new_schedule_id = cursor.lastrowid
        
        # 생산에 schedule_id 저장
        cursor.execute('''
            UPDATE productions
            SET schedule_id = ?
            WHERE id = ?
        ''', (new_schedule_id, production_id))
        
        return new_schedule_id

def delete_schedule_and_production(conn, schedule_id):
    """
    스케줄 삭제 시 연결된 생산도 삭제 (양방향)
    """
    cursor = conn.cursor()
    
    # production_id 조회
    cursor.execute('SELECT production_id FROM production_schedules WHERE id = ?', (schedule_id,))
    row = cursor.fetchone()
    
    if row and row[0]:
        production_id = row[0]
        # 생산 삭제
        cursor.execute('DELETE FROM productions WHERE id = ?', (production_id,))
        # 생산 자재 사용 내역도 삭제
        cursor.execute('DELETE FROM production_material_usage WHERE production_id = ?', (production_id,))
    
    # 스케줄 삭제
    cursor.execute('DELETE FROM production_schedules WHERE id = ?', (schedule_id,))

def delete_production_and_schedule(conn, production_id):
    """
    생산 삭제 시 연결된 스케줄도 삭제 (양방향)
    """
    cursor = conn.cursor()
    
    # schedule_id 조회
    cursor.execute('SELECT schedule_id FROM productions WHERE id = ?', (production_id,))
    row = cursor.fetchone()
    
    if row and row[0]:
        schedule_id = row[0]
        # 스케줄 삭제
        cursor.execute('DELETE FROM production_schedules WHERE id = ?', (schedule_id,))
    
    # 생산 삭제
    cursor.execute('DELETE FROM productions WHERE id = ?', (production_id,))
    # 생산 자재 사용 내역도 삭제
    cursor.execute('DELETE FROM production_material_usage WHERE production_id = ?', (production_id,))

def create_material_usage_from_bom(conn, production_id):
    """
    BOM 기반으로 예상 자재 사용량 생성
    """
    cursor = conn.cursor()
    
    # 생산 정보 조회
    cursor.execute('''
        SELECT product_id, planned_boxes
        FROM productions
        WHERE id = ?
    ''', (production_id,))
    production = cursor.fetchone()
    
    if not production:
        return
    
    product_id, planned_boxes = production
    
    # BOM 조회
    cursor.execute('''
        SELECT b.id, b.material_id, b.raw_material_name, b.quantity_per_box,
               m.name, m.category, m.unit
        FROM bom b
        LEFT JOIN materials m ON b.material_id = m.id
        WHERE b.product_id = ?
    ''', (product_id,))
    bom_items = cursor.fetchall()
    
    # 각 BOM 항목에 대해 예상 사용량 계산
    for bom in bom_items:
        bom_id, material_id, raw_material_name, quantity_per_box, mat_name, category, unit = bom
        expected_quantity = quantity_per_box * planned_boxes
        
        # material_usage 생성
        cursor.execute('''
            INSERT INTO production_material_usage 
            (production_id, material_id, raw_material_name, expected_quantity, actual_quantity, loss_quantity)
            VALUES (?, ?, ?, ?, NULL, NULL)
        ''', (production_id, material_id, raw_material_name, expected_quantity))
