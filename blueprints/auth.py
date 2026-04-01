from flask import Blueprint, render_template, request, redirect, url_for, session
import hashlib

from core import (
    add_user_notification,
    get_db,
    get_usernames_for_notification,
    WORKPLACES,
    LOGISTICS_WORKPLACE,
)

bp = Blueprint('auth', __name__)


@bp.route('/login', methods=['GET', 'POST'])
def login():
    """로그인"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        conn = get_db()
        cursor = conn.cursor()

        password_hash = hashlib.sha256(password.encode()).hexdigest()
        cursor.execute(
            "SELECT id, username, is_admin, name, role, workplaces, status FROM users WHERE username = ? AND password_hash = ?",
            (username, password_hash)
        )
        user = cursor.fetchone()
        conn.close()

        if not user:
            return render_template('login.html', error="아이디 또는 비밀번호가 잘못되었습니다")
        if user[6] != 'approved':
            msg = "승인 대기 중입니다. 최고 관리자 승인 후 로그인 가능합니다."
            if user[6] == 'rejected':
                msg = "가입 요청이 반려되었습니다. 관리자에게 문의하세요."
            return render_template('login.html', error=msg)

        role = user[4] if user[4] else ('admin' if user[2] else 'readonly')
        workplaces = user[5].split(',') if user[5] else ['1동 조미']

        session['user'] = {
            'id': user[0],
            'username': user[1],
            'is_admin': bool(user[2]),
            'name': user[3] or user[1],
            'role': role,
            'workplaces': workplaces
        }

        # 물류관리 권한은 물류 작업장으로 고정
        if role == 'logistics':
            session['workplace'] = LOGISTICS_WORKPLACE
            return redirect(url_for('materials.logistics_materials'))

        # 작업장이 여러 개면 선택 페이지로
        if len(workplaces) > 1:
            return redirect(url_for('main.select_workplace'))
        else:
            session['workplace'] = workplaces[0]
            return redirect(url_for('main.index'))

    return render_template('login.html')


@bp.route('/logout')
def logout():
    """로그아웃"""
    session.pop('user', None)
    session.pop('workplace', None)
    return redirect(url_for('auth.login'))


@bp.route('/register', methods=['GET', 'POST'])
def register():
    """회원가입"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        password_confirm = request.form.get('password_confirm')
        name = request.form.get('name')
        phone = request.form.get('phone')
        email = request.form.get('email')
        department = request.form.get('department')
        workplace1 = request.form.get('workplace1')
        workplace2 = request.form.get('workplace2')

        if password != password_confirm:
            return render_template('register.html', error="비밀번호가 일치하지 않습니다")

        conn = get_db()
        cursor = conn.cursor()

        # 중복 확인
        cursor.execute("SELECT id FROM users WHERE username = ?", (username,))
        if cursor.fetchone():
            conn.close()
            return render_template('register.html', error="이미 존재하는 아이디입니다")

        # 사용자 생성
        password_hash = hashlib.sha256(password.encode()).hexdigest()
        cursor.execute('''
            INSERT INTO users (username, password_hash, is_admin, name, phone, email, 
                              department, workplace1, workplace2, status)
            VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, 'pending')
        ''', (username, password_hash, name, phone, email, department, workplace1, workplace2))

        admin_users = get_usernames_for_notification(conn, include_admin=True)
        workplace_text = ', '.join([wp for wp in [workplace1, workplace2] if wp]) or '-'
        add_user_notification(
            conn,
            admin_users[0] if admin_users else None,
            f"새 회원가입 신청: {name or username}",
            f"{department or '-'} / 작업장 {workplace_text}",
            '/users',
        )
        for admin_username in admin_users[1:]:
            add_user_notification(
                conn,
                admin_username,
                f"새 회원가입 신청: {name or username}",
                f"{department or '-'} / 작업장 {workplace_text}",
                '/users',
            )
        conn.commit()
        conn.close()
        return render_template('login.html', error="가입 요청이 접수되었습니다. 최고 관리자 승인 후 로그인 가능합니다.")

    return render_template('register.html')
