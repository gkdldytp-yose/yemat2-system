import os

from flask import Flask, session

from core import LOGISTICS_WORKPLACE, SHARED_WORKPLACE, get_db, get_workplace

DEFAULT_SECRET_KEY = 'yemat-secret-key-2025'
DEFAULT_HOST = '0.0.0.0'
DEFAULT_PORT = 8080


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
    app.config['SECRET_KEY'] = os.getenv('YEMAT_SECRET_KEY', DEFAULT_SECRET_KEY)

    register_blueprints(app)

    def _build_dynamic_notifications(cursor, user):
        notifications = []
        is_admin = bool(user.get('is_admin'))
        workplace = get_workplace()

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

    return app


app = create_app()


if __name__ == '__main__':
    host = os.getenv('YEMAT_HOST', DEFAULT_HOST)
    port = int(os.getenv('YEMAT_PORT', DEFAULT_PORT))
    print('\n' + '=' * 80)
    print('예맛 통합 생산관리 시스템 서버 시작')
    print('=' * 80)
    print(f'\n접속 URL: http://localhost:{port}')
    print('관리자 계정: admin / 1111\n')
    app.run(host=host, port=port, debug=True)
