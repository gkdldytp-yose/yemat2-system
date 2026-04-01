from flask import Flask, session

from core import LOGISTICS_WORKPLACE, SHARED_WORKPLACE, get_db, get_workplace

app = Flask(__name__)
app.secret_key = 'yemat-secret-key-2025'

from blueprints.auth import bp as auth_bp
from blueprints.main import bp as main_bp
from blueprints.users import bp as users_bp
from blueprints.admin import bp as admin_bp
from blueprints.products import bp as products_bp
from blueprints.materials import bp as materials_bp
from blueprints.production import bp as production_bp
from blueprints.printouts import bp as printouts_bp
from blueprints.imports import bp as imports_bp

app.register_blueprint(auth_bp)
app.register_blueprint(main_bp)
app.register_blueprint(users_bp)
app.register_blueprint(admin_bp)
app.register_blueprint(products_bp)
app.register_blueprint(materials_bp)
app.register_blueprint(production_bp)
app.register_blueprint(printouts_bp)
app.register_blueprint(imports_bp)


def _build_dynamic_notifications(cursor, user):
    notifications = []
    role = (user.get('role') or '').strip()
    is_admin = bool(user.get('is_admin'))
    workplace = get_workplace()

    if role == 'logistics':
        cursor.execute(
            '''
            SELECT COUNT(*) AS cnt
            FROM logistics_issue_requests
            WHERE COALESCE(status, '') = '요청'
              AND COALESCE(request_type, 'ISSUE') = 'ISSUE'
            '''
        )
        issue_count = int((cursor.fetchone() or {'cnt': 0})['cnt'] or 0)
        if issue_count > 0:
            notifications.append(
                {
                    'id': None,
                    'title': f'불출 처리 대기 {issue_count}건',
                    'body': '물류 담당자 확인이 필요한 불출 요청이 있습니다.',
                    'link': '/logistics-materials',
                    'is_read': 1,
                    'created_at': '',
                }
            )

        cursor.execute(
            '''
            SELECT COUNT(*) AS cnt
            FROM purchase_requests
            WHERE COALESCE(status, '') = '발주중'
            '''
        )
        purchase_count = int((cursor.fetchone() or {'cnt': 0})['cnt'] or 0)
        if purchase_count > 0:
            notifications.append(
                {
                    'id': None,
                    'title': f'입고 처리 대기 {purchase_count}건',
                    'body': '물류 담당자 확인이 필요한 발주 건이 있습니다.',
                    'link': '/logistics-materials',
                    'is_read': 1,
                    'created_at': '',
                }
            )

    if workplace and workplace != LOGISTICS_WORKPLACE:
        cursor.execute(
            '''
            SELECT name, current_stock, min_stock, unit
            FROM materials
            WHERE COALESCE(min_stock, 0) > 0
              AND COALESCE(current_stock, 0) <= COALESCE(min_stock, 0)
              AND (workplace = ? OR workplace = ? OR workplace IS NULL)
            ORDER BY (COALESCE(min_stock, 0) - COALESCE(current_stock, 0)) DESC, name
            LIMIT 3
            ''',
            (workplace, SHARED_WORKPLACE),
        )
        low_stock_rows = cursor.fetchall()
        if low_stock_rows:
            names = ', '.join(row['name'] for row in low_stock_rows)
            notifications.append(
                {
                    'id': None,
                    'title': f'{workplace} 최소재고 도달 {len(low_stock_rows)}건',
                    'body': names,
                    'link': '/materials',
                    'is_read': 1,
                    'created_at': '',
                }
            )

    if is_admin:
        cursor.execute("SELECT COUNT(*) AS cnt FROM users WHERE status = 'pending'")
        pending_users = int((cursor.fetchone() or {'cnt': 0})['cnt'] or 0)
        if pending_users > 0:
            notifications.append(
                {
                    'id': None,
                    'title': f'회원가입 승인 대기 {pending_users}건',
                    'body': '최고 관리자 확인이 필요한 신규 가입 신청이 있습니다.',
                    'link': '/users',
                    'is_read': 1,
                    'created_at': '',
                }
            )

    return notifications


@app.context_processor
def inject_nav_notifications():
    user = session.get('user') or {}
    username = (user.get('username') or '').strip()
    if not username:
        return {'nav_notifications': [], 'nav_unread_notifications': 0}

    conn = get_db()
    cursor = conn.cursor()
    try:
        dynamic_notifications = _build_dynamic_notifications(cursor, user)
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

    unread_count = int(unread_row['unread_count'] or 0) if unread_row else 0
    notifications = (dynamic_notifications + list(stored_notifications))[:6]
    return {
        'nav_notifications': notifications,
        'nav_unread_notifications': unread_count + len(dynamic_notifications),
    }


if __name__ == '__main__':
    print('\n' + '=' * 80)
    print('예맛 통합 생산관리 시스템 서버 시작')
    print('=' * 80)
    print('\n접속 URL: http://localhost:8080')
    print('관리자 계정: admin / 1111\n')
    app.run(host='0.0.0.0', port=8080, debug=True)
