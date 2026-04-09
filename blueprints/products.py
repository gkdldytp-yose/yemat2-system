from flask import Blueprint, render_template, request, redirect, url_for, session
import sqlite3

from core import get_db, login_required, role_required, get_workplace, SHARED_WORKPLACE

bp = Blueprint('products', __name__)

BOM_CATEGORY_SORT_CASE = """
    CASE
        WHEN b.raw_material_id IS NOT NULL THEN 0
        WHEN COALESCE(m.category, '') = '내포' THEN 1
        WHEN COALESCE(m.category, '') = '외포' THEN 2
        WHEN COALESCE(m.category, '') = '박스' THEN 3
        WHEN COALESCE(m.category, '') = '실리카' THEN 4
        WHEN COALESCE(m.category, '') = '트레이' THEN 5
        ELSE 6
    END
"""


def _round_to_2_decimal(value, default=0.0):
    try:
        return round(float(value or 0) + 1e-9, 2)
    except (TypeError, ValueError):
        return float(default or 0)


def _parse_raw_option_values(form):
    values = []
    for sok_key, sheet_key in (
        ('sok_per_box', 'sheets_per_pack'),
        ('sok_per_box_2', 'sheets_per_pack_2'),
        ('sok_per_box_3', 'sheets_per_pack_3'),
    ):
        sok_raw = (form.get(sok_key) or '').strip()
        sheet_raw = (form.get(sheet_key) or '').strip()
        if not sok_raw:
            continue
        try:
            sok_num = round(float(sok_raw) + 1e-9, 2)
        except (TypeError, ValueError):
            continue
        try:
            sheet_num = int(float(sheet_raw or 0)) if sheet_raw else None
        except (TypeError, ValueError):
            sheet_num = None
        values.append({'sok': sok_num, 'sheets': sheet_num})
    values = values[:3]
    while len(values) < 3:
        values.append({'sok': None, 'sheets': None})
    return values


@bp.route('/products')
@login_required
def products():
    """상품 목록"""
    workplace = get_workplace()
    category = request.args.get('category', '')
    search_keyword = request.args.get('search', '').strip()
    conn = get_db()
    cursor = conn.cursor()
    query = '''
        SELECT p.*, COUNT(b.id) as bom_count
        FROM products p LEFT JOIN bom b ON p.id = b.product_id
        WHERE p.workplace = ?
    '''
    params = [workplace]

    if category:
        query += ' AND p.category = ?'
        params.append(category)

    if search_keyword:
        query += ' AND (p.name LIKE ? OR p.code LIKE ?)'
        like_q = f'%{search_keyword}%'
        params.extend([like_q, like_q])

    query += ' GROUP BY p.id ORDER BY p.category, p.name'
    cursor.execute(query, params)
    products = cursor.fetchall()
    conn.close()
    return render_template('products.html',
                           user=session['user'],
                           products=products,
                           selected_category=category,
                           search_keyword=search_keyword)


@bp.route('/products/add', methods=['POST'])
@role_required('production')
def add_product():
    """상품 추가"""
    workplace = get_workplace()
    name = request.form.get('name')
    code = request.form.get('code')
    description = request.form.get('description')
    box_quantity = request.form.get('box_quantity', 1)
    category = request.form.get('category', '기타')
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT INTO products (name, code, description, box_quantity, category, workplace)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (name, code, description, box_quantity, category, workplace))
        conn.commit()
        conn.close()
        return redirect(url_for('products.products'))
    except Exception as e:
        conn.close()
        return f"에러: {e}", 400


@bp.route('/products/<int:product_id>/delete', methods=['POST'])
@role_required('production')
def delete_product(product_id):
    """상품 삭제"""
    conn = get_db()
    cursor = conn.cursor()

    # BOM 먼저 삭제
    cursor.execute('DELETE FROM bom WHERE product_id = ?', (product_id,))
    # 상품 삭제
    cursor.execute('DELETE FROM products WHERE id = ?', (product_id,))

    conn.commit()
    conn.close()

    return redirect(url_for('products.products'))


@bp.route('/products/<int:product_id>/bom')
@login_required
def product_bom(product_id):
    """상품 BOM 관리"""
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()

    # 상품 정보
    cursor.execute('SELECT * FROM products WHERE id = ?', (product_id,))
    product = cursor.fetchone()

    # 현재 BOM (원초 정보 포함 - 입고일, 자호 추가)
    cursor.execute('''
        SELECT b.*, 
               m.name as material_name, m.unit, m.category,
               rm.name as raw_material_display_name,
               rm.code as raw_code,
               rm.lot as raw_lot,
               rm.sheets_per_sok,
               rm.receiving_date as raw_receiving_date,
               rm.car_number as raw_car_number
        FROM bom b
        LEFT JOIN materials m ON b.material_id = m.id
        LEFT JOIN raw_materials rm ON b.raw_material_id = rm.id
        WHERE b.product_id = ?
          AND (
                b.raw_material_id IS NULL
                OR NOT (
                    COALESCE(rm.total_stock, 0) > 0
                    AND COALESCE(rm.current_stock, 0) <= 0
                    AND COALESCE(rm.used_quantity, 0) >= COALESCE(rm.total_stock, 0)
                )
              )
        ORDER BY
            {bom_sort},
            COALESCE(m.category, ''),
            COALESCE(m.name, rm.name, ''),
            b.id
    '''.format(bom_sort=BOM_CATEGORY_SORT_CASE), (product_id,))
    bom_items = cursor.fetchall()

    # 전체 부자재 목록 (작업장 + 공통)
    cursor.execute('''
        SELECT m.*, s.name as supplier_name
        FROM materials m
        LEFT JOIN suppliers s ON m.supplier_id = s.id
        WHERE (m.workplace = ? OR m.workplace = ? OR m.workplace IS NULL)
        ORDER BY m.category, m.name
    ''', (workplace, SHARED_WORKPLACE))
    materials = cursor.fetchall()

    # 원초 목록 (재고 있는 것만 표시, 작업장 필터)
    cursor.execute('''
        WITH rm_base AS (
            SELECT
                id,
                name,
                COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as code,
                COALESCE(sheets_per_sok, 0) as sheets_per_sok,
                COALESCE(current_stock, 0) as current_stock,
                receiving_date,
                COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), '')) as car_number
            FROM raw_materials
            WHERE workplace = ?
              AND COALESCE(current_stock, 0) > 0
        )
        SELECT
            MIN(id) as id,
            MIN(name) as name,
            code,
            MAX(sheets_per_sok) as sheets_per_sok,
            COALESCE(SUM(current_stock), 0) as current_stock,
            MAX(receiving_date) as receiving_date,
            MIN(car_number) as car_number
        FROM rm_base
        GROUP BY code
        ORDER BY code ASC
    ''', (workplace,))
    raw_materials = cursor.fetchall()

    conn.close()

    return render_template('product_bom.html',
                         user=session['user'],
                         product=product,
                         bom_items=bom_items,
                         materials=materials,
                         raw_materials=raw_materials)


@bp.route('/products/<int:product_id>/update-info', methods=['POST'])
@role_required('production')
def update_product_info(product_id):
    """상품 기본 정보 업데이트"""
    box_quantity = request.form.get('box_quantity')
    sheets_per_pack = request.form.get('sheets_per_pack')
    cuts_per_sheet = request.form.get('cuts_per_sheet')
    category = (request.form.get('category') or '기타').strip() or '기타'
    raw_option_values = _parse_raw_option_values(request.form)
    first_option, second_option, third_option = raw_option_values
    sok_per_box = first_option['sok'] if first_option['sok'] is not None else 0
    sheets_per_pack = first_option['sheets'] if first_option['sheets'] is not None else sheets_per_pack
    sok_per_box_2 = second_option['sok']
    sok_per_box_3 = third_option['sok']
    sheets_per_pack_2 = second_option['sheets']
    sheets_per_pack_3 = third_option['sheets']
    expiry_months = request.form.get('expiry_months', 12)
    try:
        expiry_months = int(expiry_months)
    except (TypeError, ValueError):
        expiry_months = 12
    if expiry_months < 1 or expiry_months > 12:
        expiry_months = 12

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        UPDATE products
        SET box_quantity = ?, sheets_per_pack = ?, cuts_per_sheet = ?, category = ?,
            sok_per_box = ?, sok_per_box_2 = ?, sok_per_box_3 = ?,
            sheets_per_pack_2 = ?, sheets_per_pack_3 = ?, expiry_months = ?
        WHERE id = ?
    ''', (
        box_quantity, sheets_per_pack, cuts_per_sheet, category,
        sok_per_box, sok_per_box_2, sok_per_box_3,
        sheets_per_pack_2, sheets_per_pack_3, expiry_months, product_id
    ))

    conn.commit()
    conn.close()

    return redirect(url_for('products.product_bom', product_id=product_id))


@bp.route('/products/<int:product_id>/bom/add-individual', methods=['POST'])
@role_required('production')
def add_bom_individual(product_id):
    item_type = request.form.get('item_type')
    conn = get_db(); cursor = conn.cursor()

    try:
        if item_type == 'raw':
            raw_id = request.form.get('raw_id')
            qty = request.form.get('raw_quantity')

            cursor.execute(
                '''
                SELECT COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as code
                FROM raw_materials
                WHERE id = ?
                ''',
                (raw_id,),
            )
            selected = cursor.fetchone()
            if not selected:
                conn.rollback()
                return '선택한 원초 코드가 없습니다.', 400
            raw_code = selected['code']

            # 코드 기준 UPSERT: 같은 코드는 1개 BOM만 유지
            cursor.execute(
                '''
                SELECT b.id
                FROM bom b
                JOIN raw_materials rm ON rm.id = b.raw_material_id
                WHERE b.product_id = ?
                  AND COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) = ?
                LIMIT 1
                ''',
                (product_id, raw_code),
            )
            row = cursor.fetchone()
            if row:
                cursor.execute('UPDATE bom SET quantity_per_box = ?, sok_per_box = ? WHERE id = ?', (qty, qty, row['id']))
            else:
                cursor.execute(
                    '''
                    INSERT INTO bom (product_id, raw_material_id, sok_per_box, quantity_per_box)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (product_id, raw_id, qty, qty),
                )

        else: # 부자재 다중 처리
            mat_ids = request.form.getlist('mat_ids[]')
            mat_qtys = request.form.getlist('mat_quantities[]')
            for m_id, m_qty in zip(mat_ids, mat_qtys):
                cursor.execute('SELECT id FROM bom WHERE product_id = ? AND material_id = ?', (product_id, m_id))
                row = cursor.fetchone()
                if row:
                    cursor.execute('UPDATE bom SET quantity_per_box = ? WHERE id = ?', (m_qty, row['id']))
                else:
                    cursor.execute('INSERT INTO bom (product_id, material_id, quantity_per_box) VALUES (?, ?, ?)', (product_id, m_id, m_qty))

        conn.commit()
    except Exception as e:
        conn.rollback()
        return f"DB 오류: {e}", 500
    finally:
        conn.close()
    return redirect(url_for('products.product_bom', product_id=product_id))


@bp.route('/products/<int:product_id>/bom/add-multi', methods=['POST'])
@role_required('production')
def add_bom_multi(product_id):
    """BOM 항목 다중 추가"""
    item_type = request.form.get('item_type')
    selected_ids = request.form.getlist('selected_ids[]')

    conn = get_db()
    cursor = conn.cursor()

    if item_type == 'raw':
        # 원초
        quantity = request.form.get('raw_quantity')
        for raw_id in selected_ids:
            cursor.execute('''
                INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box)
                VALUES (?, NULL, ?, NULL, ?, ?)
            ''', (product_id, raw_id, quantity, quantity))
    else:
        # 부자재
        quantity = request.form.get('mat_quantity')
        for mat_id in selected_ids:
            cursor.execute('''
                INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box)
                VALUES (?, ?, NULL, NULL, NULL, ?)
            ''', (product_id, mat_id, quantity))

    conn.commit()
    conn.close()

    return redirect(url_for('products.product_bom', product_id=product_id))


@bp.route('/products/<int:product_id>/bom/add', methods=['POST'])
@role_required('production')
def add_bom_item(product_id):
    """BOM 항목 추가 - 다중 선택 지원"""
    item_type = request.form.get('item_type')

    conn = get_db()
    cursor = conn.cursor()

    if item_type == 'raw_material':
        # 원초 다중 선택
        raw_material_ids = request.form.getlist('raw_material_id')
        sok_per_box = request.form.get('sok_per_box')

        for raw_id in raw_material_ids:
            cursor.execute('''
                INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box)
                VALUES (?, NULL, ?, NULL, ?, ?)
            ''', (product_id, raw_id, sok_per_box, sok_per_box))
    else:
        # 부자재 다중 선택
        material_ids = request.form.getlist('material_id')
        quantity_per_box = request.form.get('quantity_per_box')

        for mat_id in material_ids:
            cursor.execute('''
                INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box)
                VALUES (?, ?, NULL, NULL, NULL, ?)
            ''', (product_id, mat_id, quantity_per_box))

    conn.commit()
    conn.close()

    return redirect(url_for('products.product_bom', product_id=product_id))


@bp.route('/bom/<int:bom_id>/delete', methods=['POST'])
@role_required('production')
def delete_bom_item(bom_id):
    """BOM 항목 삭제"""
    conn = get_db()
    cursor = conn.cursor()

    # product_id 먼저 가져오기
    cursor.execute('SELECT product_id FROM bom WHERE id = ?', (bom_id,))
    result = cursor.fetchone()
    product_id = result[0] if result else None

    cursor.execute('DELETE FROM bom WHERE id = ?', (bom_id,))
    conn.commit()
    conn.close()

    if product_id:
        return redirect(url_for('products.product_bom', product_id=product_id))
    return redirect(url_for('products.products'))


@bp.route('/bom/<int:bom_id>/update', methods=['POST'])
@role_required('production')
def update_bom_item(bom_id):
    """BOM 항목 수정"""
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT b.id, b.product_id, b.material_id, b.raw_material_id, b.sok_per_box, b.quantity_per_box, p.sok_per_box as product_sok_per_box
            FROM bom b
            JOIN products p ON p.id = b.product_id
            WHERE b.id = ?
            ''',
            (bom_id,),
        )
        bom = cursor.fetchone()
        if not bom:
            conn.close()
            return redirect(url_for('products.products'))

        product_id = bom['product_id']

        # 부자재 BOM 수량 수정
        if bom['material_id']:
            qty = request.form.get('quantity_per_box', type=float)
            if qty is None or qty <= 0:
                conn.close()
                return "<script>alert('사용량은 0보다 커야 합니다.');history.back();</script>"
            cursor.execute(
                'UPDATE bom SET quantity_per_box = ? WHERE id = ?',
                (qty, bom_id),
            )
            conn.commit()
            conn.close()
            return redirect(url_for('products.product_bom', product_id=product_id))

        # 원초 BOM 코드(대표 lot) 변경
        if bom['raw_material_id']:
            new_raw_id = request.form.get('raw_material_id', type=int)
            if not new_raw_id:
                conn.close()
                return "<script>alert('원초를 선택해주세요.');history.back();</script>"

            cursor.execute(
                '''
                SELECT COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as code
                FROM raw_materials
                WHERE id = ?
                ''',
                (new_raw_id,),
            )
            target = cursor.fetchone()
            if not target:
                conn.close()
                return "<script>alert('선택한 원초를 찾을 수 없습니다.');history.back();</script>"
            target_code = target['code']

            # 같은 상품 내 동일 코드가 이미 있으면 병합 유지(코드 기준 1개)
            cursor.execute(
                '''
                SELECT b.id
                FROM bom b
                JOIN raw_materials rm ON rm.id = b.raw_material_id
                WHERE b.product_id = ?
                  AND b.id != ?
                  AND COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) = ?
                LIMIT 1
                ''',
                (product_id, bom_id, target_code),
            )
            dup = cursor.fetchone()

            raw_qty = float(bom['sok_per_box'] or 0)
            if raw_qty <= 0:
                raw_qty = float(bom['quantity_per_box'] or 0)
            if raw_qty <= 0:
                raw_qty = float(bom['product_sok_per_box'] or 0)
            if raw_qty <= 0:
                raw_qty = 0

            if dup:
                cursor.execute(
                    'UPDATE bom SET raw_material_id = ?, sok_per_box = ?, quantity_per_box = ? WHERE id = ?',
                    (new_raw_id, raw_qty, raw_qty, dup['id']),
                )
                cursor.execute('DELETE FROM bom WHERE id = ?', (bom_id,))
            else:
                cursor.execute(
                    'UPDATE bom SET raw_material_id = ?, sok_per_box = ?, quantity_per_box = ? WHERE id = ?',
                    (new_raw_id, raw_qty, raw_qty, bom_id),
                )

            conn.commit()
            conn.close()
            return redirect(url_for('products.product_bom', product_id=product_id))

        conn.close()
        return redirect(url_for('products.product_bom', product_id=product_id))
    except Exception:
        conn.rollback()
        conn.close()
        raise
