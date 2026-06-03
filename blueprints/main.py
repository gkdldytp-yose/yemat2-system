from flask import Blueprint, render_template, request, redirect, url_for, session, flash
import json
from datetime import datetime, date, timedelta

from core import (
    SHARED_WORKPLACE,
    get_db,
    get_workplace,
    hash_password,
    login_required,
    today_local,
    verify_password,
)

bp = Blueprint('main', __name__)

LOW_STOCK_MATERIAL_GROUP_ORDER = ['내포', '외포', '박스', '실리카', '트레이']


def _normalize_dashboard_schedule_status(status_value):
    s = (status_value or '').strip()
    if not s:
        return '예정'
    if s == '완료' or '완료' in s:
        return '완료'
    if s == '진행중':
        return '진행중'
    if s in ('계획', '예정') or '예정' in s:
        return '예정'
    return s


def _low_stock_material_group_rank(name_value):
    name = (name_value or '').strip()
    for idx, keyword in enumerate(LOW_STOCK_MATERIAL_GROUP_ORDER):
        if keyword in name:
            return idx
    return len(LOW_STOCK_MATERIAL_GROUP_ORDER)


def _parse_dashboard_todo_due_date(raw_value):
    value = (raw_value or '').strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, '%Y-%m-%d').date()
    except ValueError:
        return None


def _todo_sort_key(row):
    due_date = str(row['due_date'] or '').strip()
    has_due = 0 if due_date else 1
    return (has_due, due_date or '9999-12-31', -(int(row['id'] or 0)))


def _normalize_todo_importance(raw_value):
    value = (raw_value or '').strip().lower()
    if value in ('high', 'medium', 'low'):
        return value
    return ''


@bp.route('/dashboard/prefill-shortage-issues', methods=['POST'])
@login_required
def prefill_shortage_issues():
    material_ids = request.form.getlist('material_id[]')
    shortage_qtys = request.form.getlist('shortage_qty[]')
    material_names = request.form.getlist('material_name[]')
    material_units = request.form.getlist('material_unit[]')

    items = []
    for idx, raw_id in enumerate(material_ids):
        try:
            material_id = int(raw_id or 0)
        except Exception:
            material_id = 0
        try:
            shortage_qty = float(shortage_qtys[idx] if idx < len(shortage_qtys) else 0)
        except Exception:
            shortage_qty = 0
        if material_id <= 0 or shortage_qty <= 0:
            continue
        items.append({
            'material_id': material_id,
            'requested_quantity': round(shortage_qty, 2),
            'material_name': (material_names[idx] if idx < len(material_names) else '').strip(),
            'unit': (material_units[idx] if idx < len(material_units) else '').strip(),
            'note': '대시보드 부족 부자재 일괄 추가',
        })

    session['dashboard_issue_prefill'] = items
    return redirect(url_for('materials.materials', req_tab='issue', issue_status='pending'))


@bp.route('/dashboard/todos', methods=['POST'])
@login_required
def create_dashboard_todo():
    workplace = get_workplace()
    username = (session.get('user') or {}).get('username')
    title = (request.form.get('title') or '').strip()
    detail = (request.form.get('detail') or '').strip()
    importance = _normalize_todo_importance(request.form.get('importance'))
    due_date_raw = (request.form.get('due_date') or '').strip()
    due_date = _parse_dashboard_todo_due_date(due_date_raw)

    if not title:
        flash('업무 제목을 입력해주세요.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))
    if due_date_raw and due_date is None:
        flash('데드라인 날짜 형식이 올바르지 않습니다.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''
        INSERT INTO dashboard_todos (workplace, title, detail, importance, due_date, created_by)
        VALUES (?, ?, ?, ?, ?, ?)
        ''',
        (workplace, title[:120], detail[:2000], importance or None, due_date.isoformat() if due_date else None, username),
    )
    conn.commit()
    conn.close()
    flash('업무 To-Do를 등록했습니다.', 'success')
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))


@bp.route('/dashboard/todos/<int:todo_id>/toggle', methods=['POST'])
@login_required
def toggle_dashboard_todo(todo_id):
    workplace = get_workplace()
    username = (session.get('user') or {}).get('username')
    is_done = 1 if (request.form.get('is_done') or '').strip() in ('1', 'true', 'on', 'yes') else 0

    conn = get_db()
    cursor = conn.cursor()
    todo_row = cursor.execute(
        '''
        SELECT id, workplace, is_done
        FROM dashboard_todos
        WHERE id = ? AND workplace = ?
        ''',
        (todo_id, workplace),
    ).fetchone()
    if not todo_row:
        conn.close()
        flash('업무 항목을 찾을 수 없습니다.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

    cursor.execute(
        '''
        UPDATE dashboard_todos
        SET is_done = ?,
            done_by = CASE WHEN ? = 1 THEN ? ELSE NULL END,
            done_at = CASE WHEN ? = 1 THEN CURRENT_TIMESTAMP ELSE NULL END
        WHERE id = ? AND workplace = ?
        ''',
        (is_done, is_done, username, is_done, todo_id, workplace),
    )
    conn.commit()
    conn.close()
    flash('업무 완료 상태를 반영했습니다.' if is_done else '업무를 미완료로 되돌렸습니다.', 'success')
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

@bp.route('/dashboard/todos/<int:todo_id>/edit', methods=['POST'])
@login_required
def update_dashboard_todo(todo_id):
    workplace = get_workplace()
    title = (request.form.get('title') or '').strip()
    detail = (request.form.get('detail') or '').strip()
    importance = _normalize_todo_importance(request.form.get('importance'))
    due_date_raw = (request.form.get('due_date') or '').strip()
    due_date = _parse_dashboard_todo_due_date(due_date_raw)

    if not title:
        flash('업무 제목을 입력해주세요.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))
    if due_date_raw and due_date is None:
        flash('데드라인 날짜 형식이 올바르지 않습니다.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

    conn = get_db()
    cursor = conn.cursor()
    existing = cursor.execute(
        '''
        SELECT id
        FROM dashboard_todos
        WHERE id = ? AND workplace = ?
        ''',
        (todo_id, workplace),
    ).fetchone()
    if not existing:
        conn.close()
        flash('수정할 업무 항목을 찾을 수 없습니다.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

    cursor.execute(
        '''
        UPDATE dashboard_todos
        SET title = ?,
            detail = ?,
            importance = ?,
            due_date = ?
        WHERE id = ? AND workplace = ?
        ''',
        (title[:120], detail[:2000], importance or None, due_date.isoformat() if due_date else None, todo_id, workplace),
    )
    conn.commit()
    conn.close()
    flash('업무 To-Do를 수정했습니다.', 'success')
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

@bp.route('/dashboard/todos/<int:todo_id>/delete', methods=['POST'])
@login_required
def delete_dashboard_todo(todo_id):
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()
    existing = cursor.execute(
        '''
        SELECT id
        FROM dashboard_todos
        WHERE id = ? AND workplace = ?
        ''',
        (todo_id, workplace),
    ).fetchone()
    if not existing:
        conn.close()
        flash('삭제할 업무 항목을 찾을 수 없습니다.', 'warning')
        return redirect(request.form.get('next') or request.referrer or url_for('main.index'))
    cursor.execute(
        '''
        DELETE FROM dashboard_todos
        WHERE id = ? AND workplace = ?
        ''',
        (todo_id, workplace),
    )
    conn.commit()
    conn.close()
    flash('업무 To-Do를 삭제했습니다.', 'success')
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))

@bp.route('/')
@login_required
def index():
    """대시보드"""
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()

    # 금주 생산 스케줄
    today = today_local()
    week_end = today + timedelta(days=7)
    cursor.execute('''
        SELECT ps.id, ps.scheduled_date, ps.planned_boxes, ps.status, ps.note,
               p.name as product_name
        FROM production_schedules ps
        LEFT JOIN products p ON ps.product_id = p.id
        WHERE ps.scheduled_date BETWEEN ? AND ? AND ps.workplace = ?
        ORDER BY ps.scheduled_date
    ''', (today.isoformat(), week_end.isoformat(), workplace))
    schedules = cursor.fetchall()

    # 현재 등록된 예정 생산 일정 기준 원초 부족
    cursor.execute(
        '''
        SELECT ps.product_id, ps.planned_boxes, ps.status
        FROM production_schedules ps
        WHERE ps.workplace = ?
        ORDER BY ps.scheduled_date, ps.id
        ''',
        (workplace,),
    )
    product_box_map = {}
    for row in cursor.fetchall():
        status = _normalize_dashboard_schedule_status(row['status'])
        if status != '예정':
            continue
        product_id = int(row['product_id'] or 0)
        planned_boxes = float(row['planned_boxes'] or 0)
        if product_id <= 0 or planned_boxes <= 0:
            continue
        product_box_map[product_id] = product_box_map.get(product_id, 0.0) + planned_boxes

    low_stock_materials = []
    raw_shortages = []
    material_need_map = {}
    if product_box_map:
        product_ids = list(product_box_map.keys())
        placeholders = ','.join(['?'] * len(product_ids))
        cursor.execute(
            f'''
            SELECT
                b.product_id,
                b.material_id,
                COALESCE(b.quantity_per_box, 0) as quantity_per_box,
                m.name as material_name,
                COALESCE(m.code, printf('M%05d', m.id)) as material_code,
                COALESCE(m.unit, '') as unit
            FROM bom b
            JOIN materials m ON m.id = b.material_id
            WHERE b.product_id IN ({placeholders})
              AND b.material_id IS NOT NULL
            ''',
            product_ids,
        )
        material_bom_rows = cursor.fetchall()

        material_ids = sorted({int(row['material_id'] or 0) for row in material_bom_rows if int(row['material_id'] or 0) > 0})
        workplace_material_stock_map = {}
        if material_ids:
            workplace_location = cursor.execute(
                '''
                SELECT id
                FROM inv_locations
                WHERE name = ? OR workplace_code = ?
                ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
                LIMIT 1
                ''',
                (workplace, workplace, workplace),
            ).fetchone()
            if workplace_location:
                material_placeholders = ','.join(['?'] * len(material_ids))
                cursor.execute(
                    f'''
                    SELECT ml.material_id, COALESCE(SUM(b.qty), 0) as qty
                    FROM inv_material_lot_balances b
                    JOIN material_lots ml ON ml.id = b.material_lot_id
                    WHERE b.location_id = ?
                      AND ml.material_id IN ({material_placeholders})
                      AND COALESCE(ml.is_disposed, 0) = 0
                    GROUP BY ml.material_id
                    ''',
                    [int(workplace_location['id']), *material_ids],
                )
                workplace_material_stock_map = {int(r['material_id']): float(r['qty'] or 0) for r in cursor.fetchall()}

        for row in material_bom_rows:
            product_id = int(row['product_id'] or 0)
            material_id = int(row['material_id'] or 0)
            if product_id <= 0 or material_id <= 0 or product_id not in product_box_map:
                continue
            qty_per_box = float(row['quantity_per_box'] or 0)
            if qty_per_box <= 0:
                continue
            required_qty = qty_per_box * float(product_box_map.get(product_id) or 0)
            if required_qty <= 0:
                continue
            if material_id not in material_need_map:
                material_need_map[material_id] = {
                    'id': material_id,
                    'code': row['material_code'] or f'M{material_id:05d}',
                    'name': row['material_name'] or f'자재 {material_id}',
                    'unit': row['unit'] or '',
                    'current_stock': float(workplace_material_stock_map.get(material_id, 0.0) or 0.0),
                    'required_qty': 0.0,
                }
            material_need_map[material_id]['required_qty'] += required_qty

        for item in material_need_map.values():
            current_stock = float(item['current_stock'] or 0)
            required_qty = float(item['required_qty'] or 0)

            # Keep the shortage decision aligned with the dashboard's displayed precision
            # so tiny floating-point residuals don't survive as "부족 0.0".
            current_stock_rounded = round(current_stock, 1)
            required_qty_rounded = round(required_qty, 1)
            shortage_qty = round(required_qty_rounded - current_stock_rounded, 1)

            if shortage_qty > 0:
                item['current_stock'] = current_stock_rounded
                item['required_qty'] = required_qty_rounded
                item['shortage_qty'] = shortage_qty
                low_stock_materials.append(item)

        low_stock_materials.sort(
            key=lambda x: (
                _low_stock_material_group_rank(x.get('name')),
                x.get('name') or '',
                x.get('code') or '',
                -float(x.get('shortage_qty') or 0),
            )
        )

        cursor.execute(
            f'''
            SELECT
                b.product_id,
                b.raw_material_id,
                COALESCE(p.sok_per_box, b.quantity_per_box, 0) as raw_qty_per_box,
                COALESCE(b.quantity_per_box, 0) as quantity_per_box,
                rm.name as raw_name,
                COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as raw_code
            FROM bom b
            JOIN products p ON p.id = b.product_id
            JOIN raw_materials rm ON rm.id = b.raw_material_id
            WHERE b.product_id IN ({placeholders})
              AND b.raw_material_id IS NOT NULL
            ''',
            product_ids,
        )
        bom_rows = cursor.fetchall()

        cursor.execute(
            '''
            SELECT
                COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id)) as raw_code,
                MIN(name) as raw_name,
                COALESCE(SUM(COALESCE(current_stock, 0)), 0) as current_stock
            FROM raw_materials
            WHERE workplace = ?
            GROUP BY COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id))
            ''',
            (workplace,),
        )
        raw_stock_map = {str(r['raw_code']): float(r['current_stock'] or 0) for r in cursor.fetchall()}

        raw_need_map = {}
        seen_product_raw_keys = set()
        for row in bom_rows:
            product_id = int(row['product_id'] or 0)
            if product_id not in product_box_map:
                continue
            raw_code = str(row['raw_code'] or '').strip()
            if not raw_code:
                continue
            dedupe_key = (product_id, raw_code)
            if dedupe_key in seen_product_raw_keys:
                continue
            seen_product_raw_keys.add(dedupe_key)
            qty_per_box = float(row['raw_qty_per_box'] or row['quantity_per_box'] or 0)
            if qty_per_box <= 0:
                continue
            required_qty = qty_per_box * float(product_box_map.get(product_id) or 0)
            if required_qty <= 0:
                continue
            if raw_code not in raw_need_map:
                raw_need_map[raw_code] = {
                    'code': raw_code,
                    'name': row['raw_name'] or raw_code,
                    'unit': '속',
                    'current_stock': float(raw_stock_map.get(raw_code, 0.0) or 0.0),
                    'required_qty': 0.0,
                }
            raw_need_map[raw_code]['required_qty'] += required_qty

        for item in raw_need_map.values():
            current_stock = float(item['current_stock'] or 0)
            required_qty = float(item['required_qty'] or 0)
            current_stock_rounded = round(current_stock, 1)
            required_qty_rounded = round(required_qty, 1)
            shortage_qty = round(required_qty_rounded - current_stock_rounded, 1)
            if shortage_qty > 0:
                item['current_stock'] = current_stock_rounded
                item['required_qty'] = required_qty_rounded
                item['shortage_qty'] = shortage_qty
                raw_shortages.append(item)

        raw_shortages.sort(key=lambda x: (-x['shortage_qty'], x['code'], x['name']))

    # 최근 생산 통계
    days_ago_30 = today - timedelta(days=30)
    cursor.execute('''
        SELECT p.name, SUM(pr.actual_boxes) as total_boxes, COUNT(*) as production_count
        FROM productions pr
        LEFT JOIN products p ON pr.product_id = p.id
        WHERE pr.production_date >= ? AND pr.status = '완료' AND pr.workplace = ?
        GROUP BY pr.product_id
        ORDER BY total_boxes DESC
        LIMIT 5
    ''', (days_ago_30.isoformat(), workplace))
    production_stats = cursor.fetchall()

    # 진행 중 불출 요청
    cursor.execute('''
        SELECT
            lir.id,
            lir.status,
            lir.requested_quantity,
            lir.requested_at,
            lir.note,
            lir.requester_workplace,
            m.name as material_name,
            m.unit as material_unit
        FROM logistics_issue_requests lir
        JOIN materials m ON lir.material_id = m.id
        WHERE lir.request_type = 'ISSUE'
          AND lir.status = '요청'
          AND lir.requester_workplace = ?
        ORDER BY lir.requested_at DESC, lir.id DESC
        LIMIT 5
    ''', (workplace,))
    pending_issues = cursor.fetchall()

    cursor.execute(
        '''
        SELECT
            id,
            workplace,
            title,
            detail,
            due_date,
            is_done,
            created_by,
            created_at,
            done_by,
            done_at
        FROM dashboard_todos
        WHERE workplace = ?
        ''',
        (workplace,),
    )
    todo_rows = cursor.fetchall()
    dashboard_todos = []
    completed_dashboard_todos = []
    due_soon_dashboard_todos = []
    overdue_dashboard_todos = []
    for row in todo_rows:
        item = dict(row)
        due_date = _parse_dashboard_todo_due_date(item.get('due_date'))
        item['due_date_obj'] = due_date
        item['is_due_soon'] = bool(due_date and today <= due_date <= (today + timedelta(days=3)))
        item['is_overdue'] = bool(due_date and due_date < today and not int(item.get('is_done') or 0))
        if int(item.get('is_done') or 0):
            completed_dashboard_todos.append(item)
        else:
            dashboard_todos.append(item)
            if item['is_due_soon']:
                due_soon_dashboard_todos.append(item)
            if item['is_overdue']:
                overdue_dashboard_todos.append(item)

    dashboard_todos.sort(key=_todo_sort_key)
    completed_dashboard_todos.sort(key=lambda item: (str(item.get('done_at') or ''), str(item.get('created_at') or '')), reverse=True)
    due_soon_dashboard_todos.sort(key=_todo_sort_key)
    overdue_dashboard_todos.sort(key=_todo_sort_key)

    conn.close()

    return render_template('dashboard.html',
                         user=session['user'],
                         workplace=workplace,
                         low_stock_materials=low_stock_materials,
                         low_stock_material_ids_json=json.dumps([int(item['id']) for item in low_stock_materials], ensure_ascii=False),
                         low_stock_material_ids_csv=','.join(str(int(item['id'])) for item in low_stock_materials),
                          raw_shortages=raw_shortages,
                          schedules=schedules,
                          production_stats=production_stats,
                          pending_issues=pending_issues,
                          dashboard_todos=dashboard_todos,
                          completed_dashboard_todos=completed_dashboard_todos,
                          due_soon_dashboard_todos=due_soon_dashboard_todos,
                          overdue_dashboard_todos=overdue_dashboard_todos,
                          today=today)


@bp.route('/select-workplace')
@login_required
def select_workplace():
    """작업장 선택 페이지"""
    return render_template('select_workplace.html', user=session['user'])


@bp.route('/select-workplace/<workplace>')
@login_required
def set_workplace(workplace):
    """작업장 설정"""
    user_workplaces = session['user']['workplaces']
    if workplace in user_workplaces:
        session['workplace'] = workplace
        return redirect(url_for('main.index'))
    else:
        return "권한이 없습니다", 403


@bp.route('/switch-workplace')
@login_required
def switch_workplace():
    """작업장 전환"""
    return redirect(url_for('main.select_workplace'))


@bp.route('/notifications/<int:notification_id>/read', methods=['POST'])
@login_required
def mark_notification_read(notification_id):
    username = (session.get('user') or {}).get('username')
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''
        UPDATE user_notifications
        SET is_read = 1, read_at = CURRENT_TIMESTAMP
        WHERE id = ? AND username = ?
        ''',
        (notification_id, username),
    )
    conn.commit()
    conn.close()
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))


@bp.route('/notifications/<int:notification_id>/delete', methods=['POST'])
@login_required
def delete_notification(notification_id):
    username = (session.get('user') or {}).get('username')
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''
        DELETE FROM user_notifications
        WHERE id = ? AND username = ?
        ''',
        (notification_id, username),
    )
    conn.commit()
    conn.close()
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))


@bp.route('/notifications/dynamic-read', methods=['POST'])
@login_required
def mark_dynamic_notification_read():
    username = (session.get('user') or {}).get('username')
    notification_key = (request.form.get('notification_key') or '').strip()
    signature = (request.form.get('signature') or '').strip()
    if username and notification_key and signature:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO user_dynamic_notification_reads (username, notification_key, signature, read_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username, notification_key)
            DO UPDATE SET signature = excluded.signature, read_at = CURRENT_TIMESTAMP
            ''',
            (username, notification_key, signature),
        )
        conn.commit()
        conn.close()
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))


@bp.route('/notifications/dynamic-dismiss', methods=['POST'])
@login_required
def dismiss_dynamic_notification():
    username = (session.get('user') or {}).get('username')
    notification_key = (request.form.get('notification_key') or '').strip()
    signature = (request.form.get('signature') or '').strip()
    if username and notification_key and signature:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            '''
            INSERT INTO user_dynamic_notification_reads (username, notification_key, signature, read_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username, notification_key)
            DO UPDATE SET signature = excluded.signature, read_at = CURRENT_TIMESTAMP
            ''',
            (username, notification_key, signature),
        )
        conn.commit()
        conn.close()
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))


@bp.route('/notifications/read-all', methods=['POST'])
@login_required
def mark_all_notifications_read():
    username = (session.get('user') or {}).get('username')
    dynamic_keys = request.form.getlist('dynamic_notification_key[]')
    dynamic_signatures = request.form.getlist('dynamic_signature[]')
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''
        UPDATE user_notifications
        SET is_read = 1, read_at = CURRENT_TIMESTAMP
        WHERE username = ? AND COALESCE(is_read, 0) = 0
        ''',
        (username,),
    )
    for idx, key in enumerate(dynamic_keys):
        notification_key = (key or '').strip()
        signature = (dynamic_signatures[idx] if idx < len(dynamic_signatures) else '').strip()
        if not notification_key or not signature:
            continue
        cursor.execute(
            '''
            INSERT INTO user_dynamic_notification_reads (username, notification_key, signature, read_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(username, notification_key)
            DO UPDATE SET signature = excluded.signature, read_at = CURRENT_TIMESTAMP
            ''',
            (username, notification_key, signature),
        )
    conn.commit()
    conn.close()
    return redirect(request.form.get('next') or request.referrer or url_for('main.index'))


@bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    """사용자 프로필"""
    user_id = session['user']['id']

    if request.method == 'POST':
        name = request.form.get('name')
        phone = request.form.get('phone')
        email = request.form.get('email')
        department = request.form.get('department')
        workplace1 = request.form.get('workplace1')
        workplace2 = request.form.get('workplace2')

        conn = get_db()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE users 
            SET name = ?, phone = ?, email = ?, department = ?, workplace1 = ?, workplace2 = ?
            WHERE id = ?
        ''', (name, phone, email, department, workplace1, workplace2, user_id))

        # 세션 정보 업데이트
        cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
        updated_user = cursor.fetchone()

        workplaces = []
        workplace1_value = (updated_user['workplace1'] or '').strip() if updated_user['workplace1'] else ''
        workplace2_value = (updated_user['workplace2'] or '').strip() if updated_user['workplace2'] else ''
        if workplace1_value:
            workplaces.append(workplace1_value)
        if workplace2_value and workplace2_value not in workplaces:
            workplaces.append(workplace2_value)
        if not workplaces:
            workplaces = ['1동 조미']

        session['user'] = {
            'id': updated_user['id'],
            'username': updated_user['username'],
            'name': updated_user['name'],
            'phone': updated_user['phone'],
            'email': updated_user['email'],
            'department': updated_user['department'],
            'workplace1': updated_user['workplace1'],
            'workplace2': updated_user['workplace2'],
            'workplaces': workplaces,
            'role': updated_user['role'] or ('admin' if updated_user['is_admin'] else 'readonly'),
            'is_admin': updated_user['is_admin']
        }

        current_workplace = session.get('workplace')
        if current_workplace not in workplaces:
            session['workplace'] = workplaces[0]

        conn.commit()
        conn.close()

        return redirect(url_for('main.profile'))

    # GET 요청 시 DB에서 최신 정보 조회
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
    user_data = cursor.fetchone()
    conn.close()

    return render_template('profile.html', user=user_data)


@bp.route('/profile/change-password', methods=['POST'])
@login_required
def change_password():
    """비밀번호 변경"""
    user_id = session['user']['id']
    current_password = request.form.get('current_password')
    new_password = request.form.get('new_password')
    confirm_password = request.form.get('confirm_password')

    if new_password != confirm_password:
        return '''
            <!DOCTYPE html>
            <html><head><meta charset="UTF-8"></head><body>
            <script>
                alert("새 비밀번호가 일치하지 않습니다.");
                window.history.back();
            </script>
            </body></html>
        '''

    conn = get_db()
    cursor = conn.cursor()

    # 현재 비밀번호 확인
    cursor.execute('SELECT password_hash FROM users WHERE id = ?', (user_id,))
    user = cursor.fetchone()

    if not verify_password(user['password_hash'], current_password):
        conn.close()
        return '''
            <!DOCTYPE html>
            <html><head><meta charset="UTF-8"></head><body>
            <script>
                alert("현재 비밀번호가 일치하지 않습니다.");
                window.history.back();
            </script>
            </body></html>
        '''

    # 새 비밀번호 저장
    new_hash = hash_password(new_password)
    cursor.execute('UPDATE users SET password_hash = ? WHERE id = ?', (new_hash, user_id))

    conn.commit()
    conn.close()

    return '''
        <!DOCTYPE html>
        <html><head><meta charset="UTF-8"></head><body>
        <script>
            alert("비밀번호가 변경되었습니다.");
            window.location.href = "/profile";
        </script>
        </body></html>
    '''
