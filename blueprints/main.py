from flask import Blueprint, render_template, request, redirect, url_for, session
import hashlib
from datetime import datetime, date, timedelta

from core import get_db, login_required, get_workplace, SHARED_WORKPLACE

bp = Blueprint('main', __name__)


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

@bp.route('/')
@login_required
def index():
    """대시보드"""
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()

    # 발주 필요 부자재
    cursor.execute('''
        SELECT m.id, m.name, m.category, m.unit, m.current_stock, m.min_stock, s.name as supplier_name
        FROM materials m
        LEFT JOIN suppliers s ON m.supplier_id = s.id
        WHERE m.current_stock <= m.min_stock
          AND EXISTS (
              SELECT 1
              FROM bom b
              JOIN products p ON p.id = b.product_id
              WHERE b.material_id = m.id
                AND p.workplace = ?
          )
        ORDER BY m.category, m.name
        LIMIT 10
    ''', (workplace,))
    low_stock_materials = cursor.fetchall()

    # 금주 생산 스케줄
    today = date.today()
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

    raw_shortages = []
    if product_box_map:
        product_ids = list(product_box_map.keys())
        placeholders = ','.join(['?'] * len(product_ids))
        cursor.execute(
            f'''
            SELECT
                b.product_id,
                b.raw_material_id,
                COALESCE(b.quantity_per_box, 0) as quantity_per_box,
                rm.name as raw_name,
                COALESCE(NULLIF(TRIM(rm.code), ''), printf('RM%05d', rm.id)) as raw_code
            FROM bom b
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
        for row in bom_rows:
            product_id = int(row['product_id'] or 0)
            if product_id not in product_box_map:
                continue
            qty_per_box = float(row['quantity_per_box'] or 0)
            if qty_per_box <= 0:
                continue
            raw_code = str(row['raw_code'] or '').strip()
            if not raw_code:
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
            shortage_qty = required_qty - current_stock
            if shortage_qty > 0:
                item['current_stock'] = round(current_stock, 1)
                item['required_qty'] = round(required_qty, 1)
                item['shortage_qty'] = round(shortage_qty, 1)
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

    # 대기중인 발주 (purchase_requests 기준)
    cursor.execute('''
        SELECT pr.id, pr.status, pr.ordered_quantity, pr.expected_delivery_date,
               m.name as material_name, s.name as supplier_name
        FROM purchase_requests pr
        JOIN materials m ON pr.material_id = m.id
        LEFT JOIN suppliers s ON m.supplier_id = s.id
        WHERE pr.status IN ('발주필요','발주중') AND pr.workplace = ?
        ORDER BY pr.requested_at DESC
        LIMIT 5
    ''', (workplace,))
    pending_orders = cursor.fetchall()

    conn.close()

    return render_template('dashboard.html',
                         user=session['user'],
                         workplace=workplace,
                         low_stock_materials=low_stock_materials,
                         raw_shortages=raw_shortages,
                         schedules=schedules,
                         production_stats=production_stats,
                         pending_orders=pending_orders,
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


@bp.route('/notifications/read-all', methods=['POST'])
@login_required
def mark_all_notifications_read():
    username = (session.get('user') or {}).get('username')
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

        session['user'] = {
            'id': updated_user['id'],
            'username': updated_user['username'],
            'name': updated_user['name'],
            'phone': updated_user['phone'],
            'email': updated_user['email'],
            'department': updated_user['department'],
            'workplace1': updated_user['workplace1'],
            'workplace2': updated_user['workplace2'],
            'role': updated_user['role'] or ('admin' if updated_user['is_admin'] else 'readonly'),
            'is_admin': updated_user['is_admin']
        }

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

    current_hash = hashlib.sha256(current_password.encode()).hexdigest()
    if user['password_hash'] != current_hash:
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
    new_hash = hashlib.sha256(new_password.encode()).hexdigest()
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
