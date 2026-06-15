from flask import Blueprint, render_template, request, redirect, url_for, session, send_file
from fractions import Fraction
from pathlib import Path
from uuid import uuid4

from core import db_connection, db_transaction, login_required, role_required, get_workplace, SHARED_WORKPLACE

bp = Blueprint('products', __name__)


def _clean_next_url(raw_value, fallback=''):
    value = str(raw_value or '').strip()
    return value or fallback


def _current_products_list_url():
    return _clean_next_url(request.full_path, url_for('products.products')).rstrip('?')


def _product_bom_url(product_id, return_to=''):
    target = _clean_next_url(return_to)
    if target:
        return url_for('products.product_bom', product_id=product_id, return_to=target)
    return url_for('products.product_bom', product_id=product_id)
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_SHEET_DIR = PROJECT_ROOT / 'uploads' / 'product_specs'
SPEC_SHEET_DIR.mkdir(parents=True, exist_ok=True)

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


def _round_to_4_decimal(value, default=0.0):
    try:
        return round(float(value or 0) + 1e-9, 4)
    except (TypeError, ValueError):
        return float(default or 0)


def _parse_bom_quantity_input(raw_value):
    text = str(raw_value or '').strip()
    if not text:
        raise ValueError('사용량을 입력해주세요.')
    normalized = text.replace(' ', '')
    try:
        value = float(Fraction(normalized))
    except (ValueError, ZeroDivisionError):
        raise ValueError('사용량은 숫자 또는 1/3 같은 분수 형태로 입력해주세요.')
    if value <= 0:
        raise ValueError('사용량은 0보다 커야 합니다.')
    expr = normalized if '/' in normalized else ''
    return float(value), expr


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
            sok_num = _round_to_4_decimal(sok_raw)
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


def _build_spec_sheet_name(product_id, original_name):
    suffix = Path(original_name or '').suffix.lower() or '.pdf'
    return f'product_{product_id}_{uuid4().hex}{suffix}'


@bp.route('/products')
@login_required
def products():
    """?곹뭹 紐⑸줉"""
    workplace = get_workplace()
    category = request.args.get('category', '')
    search_keyword = request.args.get('search', '').strip()
    with db_connection() as conn:
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
        base_count_query = '''
            SELECT COALESCE(p.category, '') AS category, COUNT(*) AS cnt
            FROM products p
            WHERE p.workplace = ?
            GROUP BY COALESCE(p.category, "")
        '''
        cursor.execute(base_count_query, [workplace])
        base_category_counts = {
            (row['category'] or '').strip(): int(row['cnt'] or 0)
            for row in cursor.fetchall()
        }
        count_query = '''
            SELECT COALESCE(p.category, '') AS category, COUNT(*) AS cnt
            FROM products p
            WHERE p.workplace = ?
        '''
        count_params = [workplace]
        if search_keyword:
            count_query += ' AND (p.name LIKE ? OR p.code LIKE ?)'
            like_q = f'%{search_keyword}%'
            count_params.extend([like_q, like_q])
        count_query += ' GROUP BY COALESCE(p.category, "")'
        cursor.execute(count_query, count_params)
        category_counts = {
            (row['category'] or '').strip(): int(row['cnt'] or 0)
            for row in cursor.fetchall()
        }
        total_count = sum(category_counts.values())
    return render_template('products.html',
                           user=session['user'],
                           products=products,
                           selected_category=category,
                           search_keyword=search_keyword,
                           category_counts=category_counts,
                           total_count=total_count,
                           available_categories=[cat for cat, cnt in base_category_counts.items() if cnt > 0],
                           current_list_url=_current_products_list_url())

@bp.route('/products/<int:product_id>/spec-sheet', methods=['POST'])
@role_required('production')
def upload_product_spec_sheet(product_id):
    workplace = get_workplace()
    upload = request.files.get('spec_sheet')
    if not upload or not upload.filename:
        return "<meta charset='utf-8'><script>alert('?깅줉??PDF ?뚯씪???좏깮?댁＜?몄슂.'); history.back();</script>"
    if not upload.filename.lower().endswith('.pdf'):
        return "<meta charset='utf-8'><script>alert('PDF ?뚯씪留??깅줉?????덉뒿?덈떎.'); history.back();</script>"

    previous_stored_name = ''
    stored_name = _build_spec_sheet_name(product_id, upload.filename)
    file_path = SPEC_SHEET_DIR / stored_name
    upload.save(file_path)

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id, spec_sheet_stored_name FROM products WHERE id = ? AND workplace = ?', (product_id, workplace))
            product_row = cursor.fetchone()
            if not product_row:
                try:
                    if file_path.exists():
                        file_path.unlink()
                except Exception:
                    pass
                return redirect(url_for('products.products'))

            previous_stored_name = (product_row['spec_sheet_stored_name'] or '').strip()
            cursor.execute(
                '''
                UPDATE products
                SET spec_sheet_file_name = ?,
                    spec_sheet_stored_name = ?,
                    spec_sheet_uploaded_at = CURRENT_TIMESTAMP
                WHERE id = ?
                ''',
                (upload.filename, stored_name, product_id),
            )
    except Exception:
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            pass
        raise

    if previous_stored_name and previous_stored_name != stored_name:
        try:
            previous_file = SPEC_SHEET_DIR / Path(previous_stored_name).name
            if previous_file.exists():
                previous_file.unlink()
        except Exception:
            pass

    next_url = (request.form.get('next') or '').strip()
    if next_url:
        next_url += '&updated=1' if '?' in next_url else '?updated=1'
        return redirect(next_url)
    return redirect(url_for('products.products'))

@bp.route('/products/<int:product_id>/spec-sheet/view')
@login_required
def view_product_spec_sheet(product_id):
    workplace = get_workplace()
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT spec_sheet_file_name, spec_sheet_stored_name
            FROM products
            WHERE id = ? AND workplace = ?
            ''',
            (product_id, workplace),
        )
        product_row = cursor.fetchone()
    if not product_row or not (product_row['spec_sheet_stored_name'] or '').strip():
        return "<meta charset='utf-8'><script>alert('?깅줉???ъ뼇?쒓? ?놁뒿?덈떎.'); window.close();</script>"

    target = SPEC_SHEET_DIR / Path(product_row['spec_sheet_stored_name']).name
    if not target.exists():
        return "<meta charset='utf-8'><script>alert('?ъ뼇???뚯씪??李얠쓣 ???놁뒿?덈떎.'); window.close();</script>"

    response = send_file(target, mimetype='application/pdf', as_attachment=False)
    response.headers['Content-Disposition'] = 'inline'
    return response

@bp.route('/products/<int:product_id>/spec-sheet/manage')
@login_required
def manage_product_spec_sheet(product_id):
    workplace = get_workplace()
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, name, code, spec_sheet_file_name, spec_sheet_stored_name, spec_sheet_uploaded_at
            FROM products
            WHERE id = ? AND workplace = ?
            ''',
            (product_id, workplace),
        )
        product = cursor.fetchone()
    if not product:
        return redirect(url_for('products.products'))

    user = session.get('user') or {}
    role = user.get('role') or ('admin' if user.get('is_admin') else 'readonly')
    can_edit = role in ['admin', 'production']
    return render_template(
        'product_spec_sheet_popup.html',
        product=product,
        can_edit=can_edit,
    )

@bp.route('/products/add', methods=['POST'])
@role_required('production')
def add_product():
    """?곹뭹 異붽?"""
    workplace = get_workplace()
    name = request.form.get('name')
    code = request.form.get('code')
    description = request.form.get('description')
    box_quantity = request.form.get('box_quantity', 1)
    category = request.form.get('category', '湲고?')
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO products (name, code, description, box_quantity, category, workplace)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (name, code, description, box_quantity, category, workplace))
        return redirect(url_for('products.products'))
    except Exception as e:
        return f"?먮윭: {e}", 400

@bp.route('/products/<int:product_id>/delete', methods=['POST'])
@role_required('production')
def delete_product(product_id):
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT spec_sheet_stored_name FROM products WHERE id = ?', (product_id,))
        product_row = cursor.fetchone()
        stored_name = (product_row['spec_sheet_stored_name'] or '') if product_row else ''

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM bom WHERE product_id = ?', (product_id,))
            cursor.execute('DELETE FROM products WHERE id = ?', (product_id,))
    except ValueError as e:
        return f"<meta charset='utf-8'><script>alert({e.args[0]!r}); history.back();</script>"

    if stored_name:
        try:
            target = SPEC_SHEET_DIR / Path(stored_name).name
            if target.exists():
                target.unlink()
        except Exception:
            pass

    return redirect(url_for('products.products'))

@bp.route('/products/<int:product_id>/bom')
@login_required
def product_bom(product_id):
    """?곹뭹 BOM 愿由?"""
    workplace = get_workplace()
    with db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM products WHERE id = ?', (product_id,))
        product = cursor.fetchone()

        cursor.execute('''
            SELECT b.*, 
                   COALESCE(b.quantity_per_box_expr, '') as quantity_per_box_expr,
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
            ORDER BY
                {bom_sort},
                COALESCE(m.category, ''),
                COALESCE(m.name, rm.name, ''),
                b.id
        '''.format(bom_sort=BOM_CATEGORY_SORT_CASE), (product_id,))
        bom_items = cursor.fetchall()
        grouped_bom_items = []
        raw_item_indexes = {}

        for item in bom_items:
            if not item['raw_material_id']:
                grouped_bom_items.append(item)
                continue

            raw_code = (item['raw_code'] or '').strip()
            if not raw_code:
                raw_code = f"RM{item['raw_material_id']:05d}"

            existing_index = raw_item_indexes.get(raw_code)
            if existing_index is None:
                raw_item_indexes[raw_code] = len(grouped_bom_items)
                grouped_bom_items.append(item)
                continue

            existing_item = grouped_bom_items[existing_index]
            if (item['id'] or 0) > (existing_item['id'] or 0):
                grouped_bom_items[existing_index] = item

        cursor.execute('''
            SELECT m.*, s.name as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON m.supplier_id = s.id
            WHERE (m.workplace = ? OR m.workplace = ? OR m.workplace IS NULL)
            ORDER BY m.category, m.name
        ''', (workplace, SHARED_WORKPLACE))
        materials = cursor.fetchall()

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

    return_to = _clean_next_url(request.args.get('return_to'), url_for('products.products'))

    return render_template('product_bom.html',
                          user=session['user'],
                          product=product,
                          bom_items=grouped_bom_items,
                          materials=materials,
                          raw_materials=raw_materials,
                          return_to=return_to)

@bp.route('/products/<int:product_id>/update-info', methods=['POST'])
@role_required('production')
def update_product_info(product_id):
    """?곹뭹 湲곕낯 ?뺣낫 ?낅뜲?댄듃"""
    box_quantity = request.form.get('box_quantity')
    sheets_per_pack = request.form.get('sheets_per_pack')
    cuts_per_sheet = request.form.get('cuts_per_sheet')
    category = (request.form.get('category') or '湲고?').strip() or '湲고?'
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

    with db_transaction() as conn:
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

    return redirect(_product_bom_url(product_id, request.form.get('return_to')))

@bp.route('/products/<int:product_id>/bom/add-individual', methods=['POST'])
@role_required('production')
def add_bom_individual(product_id):
    item_type = request.form.get('item_type')

    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            if item_type == 'raw':
                raw_id = request.form.get('raw_id')
                qty, qty_expr = _parse_bom_quantity_input(request.form.get('raw_quantity'))

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
                    return '?좏깮???먯큹 肄붾뱶媛 ?놁뒿?덈떎.', 400
                raw_code = selected['code']

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
                    cursor.execute('UPDATE bom SET quantity_per_box = ?, quantity_per_box_expr = ?, sok_per_box = ? WHERE id = ?', (qty, qty_expr, qty, row['id']))
                else:
                    cursor.execute(
                        '''
                        INSERT INTO bom (product_id, raw_material_id, sok_per_box, quantity_per_box, quantity_per_box_expr)
                        VALUES (?, ?, ?, ?, ?)
                        ''',
                        (product_id, raw_id, qty, qty, qty_expr),
                    )
            else:
                mat_ids = request.form.getlist('mat_ids[]')
                mat_qtys = request.form.getlist('mat_quantities[]')
                for m_id, m_qty in zip(mat_ids, mat_qtys):
                    qty, qty_expr = _parse_bom_quantity_input(m_qty)
                    cursor.execute('SELECT id FROM bom WHERE product_id = ? AND material_id = ?', (product_id, m_id))
                    row = cursor.fetchone()
                    if row:
                        cursor.execute('UPDATE bom SET quantity_per_box = ?, quantity_per_box_expr = ? WHERE id = ?', (qty, qty_expr, row['id']))
                    else:
                        cursor.execute('INSERT INTO bom (product_id, material_id, quantity_per_box, quantity_per_box_expr) VALUES (?, ?, ?, ?)', (product_id, m_id, qty, qty_expr))
    except ValueError as e:
        return f"<meta charset='utf-8'><script>alert({e.args[0]!r}); history.back();</script>"
    except Exception as e:
        return f"DB ?ㅻ쪟: {e}", 500
    return redirect(_product_bom_url(product_id, request.form.get('return_to')))

@bp.route('/products/<int:product_id>/bom/add-multi', methods=['POST'])
@role_required('production')
def add_bom_multi(product_id):
    """BOM ??ぉ ?ㅼ쨷 異붽?"""
    item_type = request.form.get('item_type')
    selected_ids = request.form.getlist('selected_ids[]')

    with db_transaction() as conn:
        cursor = conn.cursor()
        if item_type == 'raw':
            quantity, quantity_expr = _parse_bom_quantity_input(request.form.get('raw_quantity'))
            for raw_id in selected_ids:
                cursor.execute('''
                    INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box, quantity_per_box_expr)
                    VALUES (?, NULL, ?, NULL, ?, ?, ?)
                ''', (product_id, raw_id, quantity, quantity, quantity_expr))
        else:
            quantity, quantity_expr = _parse_bom_quantity_input(request.form.get('mat_quantity'))
            for mat_id in selected_ids:
                cursor.execute('''
                    INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box, quantity_per_box_expr)
                    VALUES (?, ?, NULL, NULL, NULL, ?, ?)
                ''', (product_id, mat_id, quantity, quantity_expr))

    return redirect(_product_bom_url(product_id, request.form.get('return_to')))

@bp.route('/products/<int:product_id>/bom/add', methods=['POST'])
@role_required('production')
def add_bom_item(product_id):
    """BOM ??ぉ 異붽? - ?ㅼ쨷 ?좏깮 吏??"""
    item_type = request.form.get('item_type')

    with db_transaction() as conn:
        cursor = conn.cursor()
        if item_type == 'raw_material':
            raw_material_ids = request.form.getlist('raw_material_id')
            sok_per_box, quantity_expr = _parse_bom_quantity_input(request.form.get('sok_per_box'))

            for raw_id in raw_material_ids:
                cursor.execute('''
                    INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box, quantity_per_box_expr)
                    VALUES (?, NULL, ?, NULL, ?, ?, ?)
                ''', (product_id, raw_id, sok_per_box, sok_per_box, quantity_expr))
        else:
            material_ids = request.form.getlist('material_id')
            quantity_per_box, quantity_expr = _parse_bom_quantity_input(request.form.get('quantity_per_box'))

            for mat_id in material_ids:
                cursor.execute('''
                    INSERT INTO bom (product_id, material_id, raw_material_id, raw_material_name, sok_per_box, quantity_per_box, quantity_per_box_expr)
                    VALUES (?, ?, NULL, NULL, NULL, ?, ?)
                ''', (product_id, mat_id, quantity_per_box, quantity_expr))

    return redirect(_product_bom_url(product_id, request.form.get('return_to')))

@bp.route('/bom/<int:bom_id>/delete', methods=['POST'])
@role_required('production')
def delete_bom_item(bom_id):
    """BOM ??ぉ ??젣"""
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT product_id FROM bom WHERE id = ?', (bom_id,))
        result = cursor.fetchone()
        product_id = result[0] if result else None

        cursor.execute('DELETE FROM bom WHERE id = ?', (bom_id,))

    if product_id:
        return redirect(_product_bom_url(product_id, request.form.get('return_to')))
    return redirect(url_for('products.products'))

@bp.route('/bom/<int:bom_id>/update', methods=['POST'])
@role_required('production')
def update_bom_item(bom_id):
    """BOM ??ぉ ?섏젙"""
    try:
        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT b.id, b.product_id, b.material_id, b.raw_material_id, b.sok_per_box, b.quantity_per_box, COALESCE(b.quantity_per_box_expr, '') as quantity_per_box_expr, p.sok_per_box as product_sok_per_box
                FROM bom b
                JOIN products p ON p.id = b.product_id
                WHERE b.id = ?
                ''',
                (bom_id,),
            )
            bom = cursor.fetchone()
            if not bom:
                return redirect(url_for('products.products'))

            product_id = bom['product_id']

            if bom['material_id']:
                qty, qty_expr = _parse_bom_quantity_input(request.form.get('quantity_per_box'))
                cursor.execute(
                    'UPDATE bom SET quantity_per_box = ?, quantity_per_box_expr = ? WHERE id = ?',
                    (qty, qty_expr, bom_id),
                )
                return redirect(_product_bom_url(product_id, request.form.get('return_to')))

            if bom['raw_material_id']:
                new_raw_id = request.form.get('raw_material_id', type=int)
                if not new_raw_id:
                    return "<script>alert('?먯큹瑜??좏깮?댁＜?몄슂.');history.back();</script>"

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
                    return "<script>alert('?좏깮???먯큹瑜?李얠쓣 ???놁뒿?덈떎.');history.back();</script>"
                target_code = target['code']

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

                return redirect(_product_bom_url(product_id, request.form.get('return_to')))

            return redirect(_product_bom_url(product_id, request.form.get('return_to')))
    except ValueError as e:
        return f"<meta charset='utf-8'><script>alert({e.args[0]!r}); history.back();</script>"



