from flask import Blueprint, render_template, request, redirect, url_for, session

from core import (
    WORKPLACES,
    add_user_notification,
    audit_log,
    build_session_user,
    get_db,
    get_usernames_for_notification,
    hash_password,
    now_local,
    password_needs_rehash,
    verify_password,
)

bp = Blueprint('auth', __name__)


def _build_auth_session_payload(user_info, event_name):
    workplaces = user_info.get('workplaces') or []
    if isinstance(workplaces, str):
        workplaces = [value.strip() for value in workplaces.split(',') if value.strip()]
    return {
        'event': event_name,
        'event_at_local': now_local().strftime('%Y-%m-%d %H:%M:%S'),
        'path': request.path,
        'method': request.method,
        'host': request.host,
        'referer': request.referrer,
        'user_agent': request.headers.get('User-Agent'),
        'role': user_info.get('role'),
        'is_admin': bool(user_info.get('is_admin')),
        'current_workplace': session.get('workplace'),
        'available_workplaces': workplaces,
    }


@bp.route('/login', methods=['GET', 'POST'])
def login():
    notice = (request.args.get('notice') or '').strip()

    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute(
            '''
            SELECT id, username, is_admin, name, role, workplaces, workplace_roles, can_integrated_management, status, password_hash
            FROM users
            WHERE username = ?
            ''',
            (username,),
        )
        user = cursor.fetchone()

        if not user or not verify_password(user['password_hash'], password):
            conn.close()
            return render_template('login.html', error='아이디 또는 비밀번호가 올바르지 않습니다.')

        if password_needs_rehash(user['password_hash']):
            cursor.execute(
                'UPDATE users SET password_hash = ? WHERE id = ?',
                (hash_password(password), user['id']),
            )
            conn.commit()

        if user['status'] != 'approved':
            conn.close()
            msg = '승인 대기 중입니다. 최고 관리자 승인 후 로그인 가능합니다.'
            if user['status'] == 'rejected':
                msg = '가입 요청이 반려되었습니다. 관리자에게 문의해 주세요.'
            return render_template('login.html', error=msg)

        conn.close()
        workplaces = user['workplaces'].split(',') if user['workplaces'] else ['1동 조미']
        user_payload = dict(user)
        user_payload['workplaces'] = workplaces
        session['user'] = build_session_user(user_payload, workplaces[0] if len(workplaces) == 1 else None)
        if len(workplaces) == 1:
            session['workplace'] = workplaces[0]

        login_payload = _build_auth_session_payload(session['user'], 'login')
        audit_conn = get_db()
        try:
            audit_log(audit_conn, 'login', 'auth_session', user['id'], login_payload)
            audit_conn.commit()
        finally:
            audit_conn.close()

        if len(workplaces) > 1:
            return redirect(url_for('main.select_workplace'))
        return redirect(url_for('main.index'))

    if notice == 'pending_signup':
        return render_template('login.html', error='가입 요청이 접수되었습니다. 최고 관리자 승인 후 로그인 가능합니다.')
    return render_template('login.html')


@bp.route('/logout')
def logout():
    user = session.get('user') or {}
    if user:
        audit_conn = get_db()
        try:
            audit_log(
                audit_conn,
                'logout',
                'auth_session',
                user.get('id'),
                _build_auth_session_payload(user, 'logout'),
            )
            audit_conn.commit()
        finally:
            audit_conn.close()
    session.pop('user', None)
    session.pop('workplace', None)
    return redirect(url_for('auth.login'))


@bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''
        password_confirm = request.form.get('password_confirm') or ''
        name = request.form.get('name')
        phone = request.form.get('phone')
        email = request.form.get('email')
        department = request.form.get('department')
        workplace1 = request.form.get('workplace1')
        workplace2 = request.form.get('workplace2')

        if password != password_confirm:
            return render_template('register.html', error='비밀번호가 일치하지 않습니다')

        conn = get_db()
        cursor = conn.cursor()
        cursor.execute('SELECT id FROM users WHERE username = ?', (username,))
        if cursor.fetchone():
            conn.close()
            return render_template('register.html', error='이미 존재하는 아이디입니다')

        cursor.execute(
            '''
            INSERT INTO users (
                username,
                password_hash,
                is_admin,
                name,
                phone,
                email,
                department,
                workplace1,
                workplace2,
                status
            )
            VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, 'pending')
            ''',
            (
                username,
                hash_password(password),
                name,
                phone,
                email,
                department,
                workplace1,
                workplace2,
            ),
        )

        admin_users = get_usernames_for_notification(conn, include_admin=True)
        workplace_text = ', '.join([wp for wp in [workplace1, workplace2] if wp]) or '-'
        add_user_notification(
            conn,
            admin_users[0] if admin_users else None,
            f'신규 회원가입 요청: {name or username}',
            f'{department or "-"} / 작업장 {workplace_text}',
            '/users',
        )
        for admin_username in admin_users[1:]:
            add_user_notification(
                conn,
                admin_username,
                f'신규 회원가입 요청: {name or username}',
                f'{department or "-"} / 작업장 {workplace_text}',
                '/users',
            )

        conn.commit()
        conn.close()
        return redirect(url_for('auth.login', notice='pending_signup'))

    return render_template('register.html', workplaces=WORKPLACES)
