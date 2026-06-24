import hashlib
import logging
import os
from datetime import date, timedelta
from time import perf_counter
from functools import wraps

from flask import Flask, g, request, session
from werkzeug.middleware.proxy_fix import ProxyFix

from core import LOGISTICS_WORKPLACE, SHARED_WORKPLACE, get_db, get_workplace, today_local

DEFAULT_SECRET_KEY = 'yemat-secret-key-2025'
DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8080


def _env_flag(name, default=False):
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {'1', 'true', 'yes', 'on'}


def _resolve_secret_key():
    return (
        os.getenv('YEMAT_SECRET_KEY')
        or os.getenv('SECRET_KEY')
        or DEFAULT_SECRET_KEY
    )


def register_blueprints(app):
    from blueprints.admin import bp as admin_bp
    from blueprints.auth import bp as auth_bp
    from blueprints.imports import bp as imports_bp
    from blueprints.main import bp as main_bp
    from blueprints.materials import bp as materials_bp
    from blueprints.printouts import bp as printouts_bp
    from blueprints.production import bp as production_bp
    from blueprints.products import bp as products_bp
    from blueprints.users import bp as users_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(users_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(products_bp)
    app.register_blueprint(materials_bp)
    app.register_blueprint(production_bp)
    app.register_blueprint(printouts_bp)
    app.register_blueprint(imports_bp)


def create_app():
    app = Flask(__name__)
    app.config['SECRET_KEY'] = _resolve_secret_key()
    app.config['SESSION_COOKIE_HTTPONLY'] = True
    app.config['SESSION_COOKIE_SAMESITE'] = os.getenv('YEMAT_SESSION_COOKIE_SAMESITE', 'Lax')
    app.config['SESSION_COOKIE_SECURE'] = _env_flag('YEMAT_SESSION_COOKIE_SECURE', default=False)
    app.config['PREFERRED_URL_SCHEME'] = 'https' if app.config['SESSION_COOKIE_SECURE'] else 'http'
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

    @app.before_request
    def _track_request_started_at():
        g._request_started_at = perf_counter()

    @app.after_request
    def _write_access_log(response):
        try:
            if request.path.startswith('/static') or request.path == '/favicon.ico':
                return response
            logger = logging.getLogger('yemat.access')
            if not logger.handlers:
                return response
            started_at = getattr(g, '_request_started_at', None)
            elapsed_ms = int((perf_counter() - started_at) * 1000) if started_at else 0
            user = session.get('user') or {}
            username = (user.get('username') or '-').strip() or '-'
            workplace = (session.get('workplace') or '-').strip() or '-'
            ip = (request.headers.get('X-Forwarded-For') or request.remote_addr or '-').split(',')[0].strip() or '-'
            endpoint = (request.endpoint or '-').strip() or '-'
            query = request.query_string.decode('utf-8', errors='ignore').strip()
            path = request.path if not query else f'{request.path}?{query}'
            referer = (request.referrer or '-').strip() or '-'
            logger.info(
                '%s | %s | %s | %s | %s | %s | %s | %sms | %s',
                ip,
                username,
                workplace,
                request.method,
                response.status_code,
                endpoint,
                path,
                elapsed_ms,
                referer,
            )
        except Exception:
            pass
        return response

    register_blueprints(app)

    def _wrap_exception_logging(endpoint_name):
        view = app.view_functions.get(endpoint_name)
        if not view or getattr(view, '_yemat_exception_wrapped', False):
            return

        @wraps(view)
        def _wrapped_view(*args, **kwargs):
            try:
                return view(*args, **kwargs)
            except Exception:
                logging.getLogger('yemat.waitress').exception(
                    'Unhandled exception at %s | path=%s | args=%s | user=%r | workplace=%r',
                    endpoint_name,
                    request.path,
                    dict(request.args),
                    session.get('user'),
                    session.get('workplace'),
                )
                raise

        _wrapped_view._yemat_exception_wrapped = True
        app.view_functions[endpoint_name] = _wrapped_view

    _wrap_exception_logging('materials.raw_materials')
    _wrap_exception_logging('production.schedules')

    def _normalize_schedule_status(status_value):
        s = (status_value or '').strip()
        if not s:
            return '예정'
        if s == '완료' or '완료' in s:
            return '완료'
        if s == '진행중' or '진행중' in s:
            return '진행중'
        if s in ('계획', '예정') or '예정' in s:
            return '예정'
        return s

    def _get_planned_product_box_map(cursor, workplace):
        cursor.execute(
            '''
            SELECT product_id, planned_boxes, status
            FROM production_schedules
            WHERE workplace = ?
            ORDER BY scheduled_date, id
            ''',
            (workplace,),
        )
        product_box_map = {}
        for row in cursor.fetchall():
            if _normalize_schedule_status(row['status']) != '예정':
                continue
            product_id = int(row['product_id'] or 0)
            planned_boxes = float(row['planned_boxes'] or 0)
            if product_id <= 0 or planned_boxes <= 0:
                continue
            product_box_map[product_id] = product_box_map.get(product_id, 0.0) + planned_boxes
        return product_box_map

    def _get_material_shortages(cursor, workplace):
        product_box_map = _get_planned_product_box_map(cursor, workplace)
        if not product_box_map:
            return []

        product_ids = list(product_box_map.keys())
        placeholders = ','.join(['?'] * len(product_ids))
        cursor.execute(
            f'''
            SELECT
                b.product_id,
                b.material_id,
                COALESCE(b.quantity_per_box, 0) as quantity_per_box,
                m.id as material_id_value,
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
        bom_rows = cursor.fetchall()

        material_ids = sorted({int(row['material_id'] or 0) for row in bom_rows if int(row['material_id'] or 0) > 0})
        workplace_stock_map = {}
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
                workplace_stock_map = {int(r['material_id']): float(r['qty'] or 0) for r in cursor.fetchall()}

        need_map = {}
        for row in bom_rows:
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
            if material_id not in need_map:
                need_map[material_id] = {
                    'id': material_id,
                    'code': row['material_code'] or f'M{material_id:05d}',
                    'name': row['material_name'] or f'자재 {material_id}',
                    'unit': row['unit'] or '',
                    'current_stock': float(workplace_stock_map.get(material_id, 0.0) or 0.0),
                    'required_qty': 0.0,
                }
            need_map[material_id]['required_qty'] += required_qty

        shortages = []
        for item in need_map.values():
            shortage_qty = float(item['required_qty'] or 0) - float(item['current_stock'] or 0)
            if shortage_qty > 0:
                item['shortage_qty'] = round(shortage_qty, 1)
                shortages.append(item)
        return shortages

    def _get_raw_shortages(cursor, workplace):
        product_box_map = _get_planned_product_box_map(cursor, workplace)
        if not product_box_map:
            return []

        product_ids = list(product_box_map.keys())
        placeholders = ','.join(['?'] * len(product_ids))
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
                COALESCE(SUM(COALESCE(current_stock, 0)), 0) as current_stock
            FROM raw_materials
            WHERE workplace = ?
            GROUP BY COALESCE(NULLIF(TRIM(code), ''), printf('RM%05d', id))
            ''',
            (workplace,),
        )
        raw_stock_map = {str(r['raw_code']): float(r['current_stock'] or 0) for r in cursor.fetchall()}

        need_map = {}
        seen_keys = set()
        for row in bom_rows:
            product_id = int(row['product_id'] or 0)
            if product_id not in product_box_map:
                continue
            raw_code = str(row['raw_code'] or '').strip()
            if not raw_code:
                continue
            dedupe_key = (product_id, raw_code)
            if dedupe_key in seen_keys:
                continue
            seen_keys.add(dedupe_key)
            qty_per_box = float(row['raw_qty_per_box'] or row['quantity_per_box'] or 0)
            if qty_per_box <= 0:
                continue
            required_qty = qty_per_box * float(product_box_map.get(product_id) or 0)
            if required_qty <= 0:
                continue
            if raw_code not in need_map:
                need_map[raw_code] = {
                    'code': raw_code,
                    'name': row['raw_name'] or raw_code,
                    'current_stock': float(raw_stock_map.get(raw_code, 0.0) or 0.0),
                    'required_qty': 0.0,
                }
            need_map[raw_code]['required_qty'] += required_qty

        shortages = []
        for item in need_map.values():
            shortage_qty = float(item['required_qty'] or 0) - float(item['current_stock'] or 0)
            if shortage_qty > 0:
                item['shortage_qty'] = round(shortage_qty, 1)
                shortages.append(item)
        return shortages

    def _parse_todo_due_date(raw_value):
        value = str(raw_value or '').strip()
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None

    def _todo_sort_key(item):
        due_date = str(item.get('due_date') or '').strip()
        has_due = 0 if due_date else 1
        return (has_due, due_date or '9999-12-31', -(int(item.get('id') or 0)))

    def _normalize_todo_importance(raw_value):
        value = str(raw_value or '').strip().lower()
        if value in {'high', 'medium', 'low'}:
            return value
        return ''

    def _load_nav_todos(cursor, workplace):
        today = today_local()
        completed_date_from = (request.args.get('todo_completed_from') or today.isoformat()).strip() or today.isoformat()
        completed_date_to = (request.args.get('todo_completed_to') or today.isoformat()).strip() or today.isoformat()
        completed_keyword = (request.args.get('todo_completed_keyword') or '').strip()
        completed_importance = _normalize_todo_importance(request.args.get('todo_completed_importance'))
        completed_done_by = (request.args.get('todo_completed_done_by') or '').strip()
        cursor.execute(
            """
            SELECT
                t.id,
                t.workplace,
                t.title,
                t.detail,
                t.importance,
                t.due_date,
                t.is_done,
                t.created_by,
                t.created_at,
                t.done_by,
                t.done_at,
                COALESCE(NULLIF(TRIM(uc.name), ''), t.created_by) AS created_by_name,
                COALESCE(NULLIF(TRIM(ud.name), ''), t.done_by) AS done_by_name
            FROM dashboard_todos t
            LEFT JOIN users uc ON uc.username = t.created_by
            LEFT JOIN users ud ON ud.username = t.done_by
            WHERE workplace = ?
            ORDER BY COALESCE(t.is_done, 0) ASC, COALESCE(t.due_date, '') ASC, t.id DESC
            """,
            (workplace,),
        )
        active_todos = []
        completed_todos = []
        due_soon_todos = []
        overdue_todos = []
        for row in cursor.fetchall():
            item = dict(row)
            item['importance'] = _normalize_todo_importance(item.get('importance'))
            due_date = _parse_todo_due_date(item.get('due_date'))
            item['is_due_soon'] = bool(due_date and today <= due_date <= (today + timedelta(days=3)))
            item['is_overdue'] = bool(due_date and due_date < today and not int(item.get('is_done') or 0))
            if int(item.get('is_done') or 0):
                done_date = str(item.get('done_at') or '')[:10]
                matches = True
                if completed_date_from and done_date and done_date < completed_date_from:
                    matches = False
                if completed_date_to and done_date and done_date > completed_date_to:
                    matches = False
                if completed_keyword:
                    haystack = f"{item.get('title') or ''} {item.get('detail') or ''}".lower()
                    if completed_keyword.lower() not in haystack:
                        matches = False
                if completed_importance and item['importance'] != completed_importance:
                    matches = False
                done_by_search_text = f"{item.get('done_by_name') or ''} {item.get('done_by') or ''}".lower()
                if completed_done_by and completed_done_by.lower() not in done_by_search_text:
                    matches = False
                if matches:
                    completed_todos.append(item)
            else:
                active_todos.append(item)
                if item['is_due_soon']:
                    due_soon_todos.append(item)
                if item['is_overdue']:
                    overdue_todos.append(item)
        active_todos.sort(key=_todo_sort_key)
        completed_todos.sort(key=lambda item: (str(item.get('done_at') or ''), str(item.get('created_at') or '')), reverse=True)
        due_soon_todos.sort(key=_todo_sort_key)
        overdue_todos.sort(key=_todo_sort_key)
        return {
            'nav_dashboard_todos': active_todos,
            'nav_completed_dashboard_todos': completed_todos,
            'nav_due_soon_dashboard_todos': due_soon_todos,
            'nav_overdue_dashboard_todos': overdue_todos,
            'nav_today': today,
            'nav_todo_filters': {
                'completed_from': completed_date_from,
                'completed_to': completed_date_to,
                'completed_keyword': completed_keyword,
                'completed_importance': completed_importance,
                'completed_done_by': completed_done_by,
            },
        }

    def _build_dynamic_notifications(cursor, user):
        notifications = []
        is_admin = bool(user.get('is_admin'))
        username = (user.get('username') or '').strip()

        def push_dynamic_notification(key, title, body, link, level='info', icon='🔔', cta_label='열기'):
            signature_source = f'{title}|{body}|{link or ""}'
            signature = hashlib.sha256(signature_source.encode('utf-8')).hexdigest()
            notifications.append(
                {
                    'id': None,
                    'dynamic_key': key,
                    'dynamic_signature': signature,
                    'title': title,
                    'body': body,
                    'link': link,
                    'level': level,
                    'icon': icon,
                    'cta_label': cta_label,
                    'is_read': 1,
                    'created_at': '',
                }
            )

        if username:
            cursor.execute(
                """
                SELECT COUNT(*) AS cnt
                FROM logistics_issue_requests
                WHERE requester_username = ?
                  AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
                  AND status = '??'
                """,
                (username,),
            )
            pending_issue_row = cursor.fetchone()
            pending_issue_count = int(pending_issue_row['cnt'] or 0) if pending_issue_row else 0
            if pending_issue_count > 0:
                push_dynamic_notification(
                    'issue_receipt_pending',
                    f'불출 입고 확인 대기 {pending_issue_count}건',
                    '완료 처리된 불출 건을 확인하고 입고 완료 여부를 정리해주세요.',
                    '/materials?req_tab=issue&issue_status=pending',
                    level='warning',
                    icon='📥',
                    cta_label='확인',
                )

        workplace = get_workplace()
        if workplace:
            today = today_local()
            deadline_limit = today + timedelta(days=3)
            cursor.execute(
                """
                SELECT id, title, detail, due_date
                FROM dashboard_todos
                WHERE workplace = ?
                  AND COALESCE(is_done, 0) = 0
                  AND due_date IS NOT NULL
                  AND TRIM(due_date) <> ''
                  AND due_date BETWEEN ? AND ?
                ORDER BY due_date ASC, id DESC
                LIMIT 3
                """,
                (workplace, today.isoformat(), deadline_limit.isoformat()),
            )
            for todo_row in cursor.fetchall():
                due_date = str(todo_row['due_date'] or '').strip()
                title = str(todo_row['title'] or '업무').strip() or '업무'
                detail = str(todo_row['detail'] or '').strip()
                body = f'{workplace} · 마감 {due_date}'
                if detail:
                    body = f'{body} · {detail[:60]}'
                push_dynamic_notification(
                    f'dashboard_todo_due_{int(todo_row["id"])}',
                    f'To-Do 마감 임박: {title}',
                    body,
                    '/#todo-messenger',
                    level='warning',
                    icon='⏰',
                    cta_label='확인',
                )

        if is_admin:
            cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE status = 'pending'")
            pending_users = int((cursor.fetchone() or {'cnt': 0})['cnt'] or 0)
            if pending_users > 0:
                push_dynamic_notification(
                    'pending_users',
                    f'신규 회원가입 승인 대기 {pending_users}건',
                    '사용자 관리에서 승인 여부와 권한을 확인해주세요.',
                    '/users',
                    level='danger',
                    icon='👤',
                    cta_label='승인',
                )

        return notifications

    @app.context_processor
    def inject_nav_notifications():
        user = session.get('user') or {}
        username = (user.get('username') or '').strip()
        if not username:
            return {
                'nav_notifications': [],
                'nav_unread_notifications': 0,
                'nav_stored_unread_notifications': 0,
                'nav_dashboard_todos': [],
                'nav_completed_dashboard_todos': [],
                'nav_due_soon_dashboard_todos': [],
                'nav_overdue_dashboard_todos': [],
                'nav_today': today_local(),
                'nav_todo_filters': {
                    'completed_from': today_local().isoformat(),
                    'completed_to': today_local().isoformat(),
                    'completed_keyword': '',
                    'completed_importance': '',
                    'completed_done_by': '',
                },
            }

        conn = get_db()
        cursor = conn.cursor()
        workplace = get_workplace()
        try:
            dynamic_notifications = _build_dynamic_notifications(cursor, user)
            todo_context = _load_nav_todos(cursor, workplace) if workplace else {
                'nav_dashboard_todos': [],
                'nav_completed_dashboard_todos': [],
                'nav_due_soon_dashboard_todos': [],
                'nav_overdue_dashboard_todos': [],
                'nav_today': today_local(),
                'nav_todo_filters': {
                    'completed_from': today_local().isoformat(),
                    'completed_to': today_local().isoformat(),
                    'completed_keyword': '',
                    'completed_importance': '',
                    'completed_done_by': '',
                },
            }
            dynamic_read_map = {}
            if dynamic_notifications:
                cursor.execute(
                    '''
                    SELECT notification_key, signature
                    FROM user_dynamic_notification_reads
                    WHERE username = ?
                    ''',
                    (username,),
                )
                dynamic_read_map = {row['notification_key']: row['signature'] for row in cursor.fetchall()}
            cursor.execute(
                '''
                SELECT username, name
                FROM users
                WHERE status = 'pending'
                '''
            )
            pending_signup_titles = set()
            for row in cursor.fetchall():
                pending_username = str(row['username'] or '').strip()
                pending_name = str(row['name'] or '').strip()
                if pending_username:
                    pending_signup_titles.add(f'신규 회원가입 요청: {pending_username}')
                if pending_name:
                    pending_signup_titles.add(f'신규 회원가입 요청: {pending_name}')
            cursor.execute(
                '''
                DELETE FROM user_notifications
                WHERE username = ?
                  AND link = '/users'
                  AND title LIKE '신규 회원가입 요청:%'
                ''',
                (username,),
            )
            conn.commit()
            for title in pending_signup_titles:
                cursor.execute(
                    '''
                    INSERT INTO user_notifications (username, title, body, link, is_read, created_at, read_at)
                    SELECT username, title, body, link, is_read, created_at, read_at
                    FROM user_notifications
                    WHERE 1 = 0
                    ''',
                )
            cursor.execute(
                '''
                SELECT id, title, body, link, is_read, created_at
                FROM user_notifications
                WHERE username = ?
                ORDER BY created_at DESC, id DESC
                LIMIT 6
                ''',
                (username,),
            )
            stored_notifications = cursor.fetchall()
            cursor.execute(
                '''
                SELECT COUNT(*) AS unread_count
                FROM user_notifications
                WHERE username = ? AND COALESCE(is_read, 0) = 0
                ''',
                (username,),
            )
            unread_row = cursor.fetchone()
        finally:
            conn.close()

        def enrich_notification(item):
            item = dict(item)
            item.setdefault('level', 'info')
            item.setdefault('icon', '🔔')
            item.setdefault('cta_label', '열기')
            title = str(item.get('title') or '')
            link = str(item.get('link') or '')
            if '/users' in link or '회원가입' in title:
                item['level'] = 'danger'
                item['icon'] = '👤'
                item['cta_label'] = '승인'
            elif '/materials?req_tab=issue' in link or '불출' in title:
                item['level'] = 'warning'
                item['icon'] = '📦'
                item['cta_label'] = '확인'
            elif '/purchase-orders' in link or '발주' in title or '입고 예정일' in title:
                item['level'] = 'info'
                item['icon'] = '🚚'
                item['cta_label'] = '보기'
            elif '/materials?req_tab=export' in link or '반출' in title:
                item['level'] = 'info'
                item['icon'] = '🔄'
                item['cta_label'] = '보기'
            return item

        unread_count = int(unread_row['unread_count'] or 0) if unread_row else 0
        dynamic_unread_count = 0
        for nt in dynamic_notifications:
            dynamic_key = nt.get('dynamic_key') if isinstance(nt, dict) else None
            dynamic_signature = nt.get('dynamic_signature') if isinstance(nt, dict) else None
            if not dynamic_key or not dynamic_signature:
                nt['is_read'] = 1
                continue
            is_dynamic_read = dynamic_read_map.get(dynamic_key) == dynamic_signature
            nt['is_read'] = 1 if is_dynamic_read else 0
            if not is_dynamic_read:
                dynamic_unread_count += 1
        combined = [enrich_notification(nt) for nt in dynamic_notifications] + [enrich_notification(nt) for nt in stored_notifications]
        notifications = sorted(
            combined,
            key=lambda item: (
                1 if item.get('is_read') else 0,
                '' if item.get('dynamic_key') else '-',
                -(int(item.get('id') or 0)),
            ),
        )[:8]
        return {
            'nav_notifications': notifications,
            'nav_unread_notifications': unread_count + dynamic_unread_count,
            'nav_stored_unread_notifications': unread_count,
            **todo_context,
        }

    return app


app = create_app()


if __name__ == '__main__':
    host = os.getenv('YEMAT_HOST', DEFAULT_HOST)
    port = int(os.getenv('YEMAT_PORT', DEFAULT_PORT))
    using_default_secret = app.config['SECRET_KEY'] == DEFAULT_SECRET_KEY
    print('\n' + '=' * 80)
    print('예맛 통합 생산관리 시스템 서버 시작')
    print('=' * 80)
    print(f'\n접속 URL: http://localhost:{port}')
    if using_default_secret:
        print('[warning] Set YEMAT_SECRET_KEY or SECRET_KEY before production deployment.')
    if app.config['SESSION_COOKIE_SECURE']:
        print('[security] SESSION_COOKIE_SECURE=ON')
    app.run(host=host, port=port, debug=False)


