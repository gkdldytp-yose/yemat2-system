from flask import Blueprint, render_template, request, redirect, url_for, session

from core import (
    WORKPLACES,
    add_user_notification,
    audit_log,
    build_session_user,
    commit_db,
    db_connection,
    db_transaction,
    get_usernames_for_notification,
    hash_password,
    now_local,
    password_needs_rehash,
    verify_password,
)

bp = Blueprint('auth', __name__)

RECOVERY_QUESTIONS = [
    '가장 좋아하는 음식은 무엇인가요?',
    '초등학교 이름은 무엇인가요?',
    '가장 기억에 남는 여행지는 어디인가요?',
    '어릴 때 별명은 무엇이었나요?',
    '가장 좋아하는 영화 제목은 무엇인가요?',
    '처음 다닌 회사 이름은 무엇인가요?',
    '존경하는 사람의 이름은 무엇인가요?',
]


def _normalize_phone(value):
    return ''.join(ch for ch in str(value or '') if ch.isdigit())


def _normalize_email(value):
    return str(value or '').strip().lower()


def _normalize_recovery_answer(value):
    return ' '.join(str(value or '').strip().lower().split())


def _match_contact(user_row, phone, email):
    normalized_phone = _normalize_phone(phone)
    normalized_email = _normalize_email(email)
    stored_phone = _normalize_phone(user_row.get('phone'))
    stored_email = _normalize_email(user_row.get('email'))
    return bool(
        (normalized_phone and stored_phone and normalized_phone == stored_phone)
        or (normalized_email and stored_email and normalized_email == stored_email)
    )


def _has_recovery_setup(user_row):
    return bool((user_row.get('recovery_question') or '').strip() and (user_row.get('recovery_answer_hash') or '').strip())


def _register_context(error='', form_data=None):
    return {
        'error': error,
        'recovery_questions': RECOVERY_QUESTIONS,
        'form_data': form_data or {},
    }


def _recovery_setup_context(error='', success='', form_data=None, username=''):
    return {
        'error': error,
        'success': success,
        'form_data': form_data or {},
        'recovery_questions': RECOVERY_QUESTIONS,
        'username': username,
    }


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


def _complete_login(user):
    workplaces = user['workplaces'].split(',') if isinstance(user.get('workplaces'), str) and user.get('workplaces') else [WORKPLACES[0]]
    user['workplaces'] = workplaces
    session['user'] = build_session_user(user, workplaces[0] if len(workplaces) == 1 else None)
    if len(workplaces) == 1:
        session['workplace'] = workplaces[0]
    else:
        session.pop('workplace', None)

    login_payload = _build_auth_session_payload(session['user'], 'login')
    with db_transaction() as audit_conn:
        audit_log(audit_conn, 'login', 'auth_session', user['id'], login_payload)

    if len(workplaces) > 1:
        return redirect(url_for('main.select_workplace'))
    return redirect(url_for('main.index'))


def _queue_recovery_setup(user):
    staged = dict(user)
    staged.pop('password_hash', None)
    session['recovery_setup_user'] = staged
    session.pop('user', None)
    session.pop('workplace', None)
    return redirect(url_for('auth.setup_recovery'))


@bp.route('/login', methods=['GET', 'POST'])
def login():
    notice = (request.args.get('notice') or '').strip()

    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password') or ''

        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT
                    id,
                    username,
                    is_admin,
                    name,
                    phone,
                    email,
                    department,
                    workplace1,
                    workplace2,
                    role,
                    workplaces,
                    workplace_roles,
                    can_integrated_management,
                    status,
                    password_hash,
                    recovery_question,
                    recovery_answer_hash
                FROM users
                WHERE username = ?
                """,
                (username,),
            )
            user = cursor.fetchone()

            if not user or not verify_password(user['password_hash'], password):
                return render_template('login.html', error='아이디 또는 비밀번호가 올바르지 않습니다.')

            user = dict(user)

            if password_needs_rehash(user['password_hash']):
                cursor.execute(
                    'UPDATE users SET password_hash = ? WHERE id = ?',
                    (hash_password(password), user['id']),
                )
                commit_db(conn)

            if user['status'] != 'approved':
                msg = '계정이 아직 승인되지 않았습니다. 관리자 승인 후 로그인할 수 있습니다.'
                if user['status'] == 'rejected':
                    msg = '회원가입 요청이 반려되었습니다. 관리자에게 문의해 주세요.'
                return render_template('login.html', error=msg)

        if not _has_recovery_setup(user):
            return _queue_recovery_setup(user)

        return _complete_login(user)

    if notice == 'pending_signup':
        return render_template('login.html', error='회원가입 요청이 접수되었습니다. 관리자 승인 후 로그인할 수 있습니다.')
    return render_template('login.html')


@bp.route('/logout')
def logout():
    user = session.get('user') or {}
    if user:
        with db_transaction() as audit_conn:
            audit_log(
                audit_conn,
                'logout',
                'auth_session',
                user.get('id'),
                _build_auth_session_payload(user, 'logout'),
            )
    session.pop('user', None)
    session.pop('workplace', None)
    session.pop('recovery_setup_user', None)
    return redirect(url_for('auth.login'))


@bp.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        form_data = {
            'username': (request.form.get('username') or '').strip(),
            'name': (request.form.get('name') or '').strip(),
            'phone': (request.form.get('phone') or '').strip(),
            'email': (request.form.get('email') or '').strip(),
            'department': (request.form.get('department') or '').strip(),
            'recovery_question': (request.form.get('recovery_question') or '').strip(),
            'recovery_answer': request.form.get('recovery_answer') or '',
            'recovery_answer_confirm': request.form.get('recovery_answer_confirm') or '',
        }
        password = request.form.get('password') or ''
        password_confirm = request.form.get('password_confirm') or ''
        workplace1 = request.form.get('workplace1')
        workplace2 = request.form.get('workplace2')

        if password != password_confirm:
            return render_template('register.html', **_register_context('비밀번호가 일치하지 않습니다.', form_data))
        if form_data['recovery_question'] not in RECOVERY_QUESTIONS:
            return render_template('register.html', **_register_context('암호찾기 질문을 선택해 주세요.', form_data))
        if not form_data['recovery_answer'].strip():
            return render_template('register.html', **_register_context('암호찾기 답변을 입력해 주세요.', form_data))
        if form_data['recovery_answer'] != form_data['recovery_answer_confirm']:
            return render_template('register.html', **_register_context('암호찾기 답변이 일치하지 않습니다.', form_data))

        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM users WHERE username = ?', (form_data['username'],))
            if cursor.fetchone():
                return render_template('register.html', **_register_context('이미 존재하는 아이디입니다.', form_data))

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
                    status,
                    recovery_question,
                    recovery_answer_hash
                )
                VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
                ''',
                (
                    form_data['username'],
                    hash_password(password),
                    form_data['name'],
                    form_data['phone'],
                    form_data['email'],
                    form_data['department'],
                    workplace1,
                    workplace2,
                    form_data['recovery_question'],
                    hash_password(_normalize_recovery_answer(form_data['recovery_answer'])),
                ),
            )

            admin_users = get_usernames_for_notification(conn, include_admin=True)
            workplace_text = ', '.join([wp for wp in [workplace1, workplace2] if wp]) or '-'
            add_user_notification(
                conn,
                admin_users[0] if admin_users else None,
                f'신규 회원가입 요청: {form_data["name"] or form_data["username"]}',
                f'{form_data["department"] or "-"} / 작업장 {workplace_text}',
                '/users',
            )
            for admin_username in admin_users[1:]:
                add_user_notification(
                    conn,
                    admin_username,
                    f'신규 회원가입 요청: {form_data["name"] or form_data["username"]}',
                    f'{form_data["department"] or "-"} / 작업장 {workplace_text}',
                    '/users',
                )

        return redirect(url_for('auth.login', notice='pending_signup'))

    return render_template('register.html', **_register_context())


@bp.route('/recovery-setup', methods=['GET', 'POST'])
def setup_recovery():
    pending_user = session.get('recovery_setup_user') or {}
    logged_in_user = session.get('user') or {}
    active_user = pending_user or logged_in_user

    if not active_user:
        return redirect(url_for('auth.login'))

    username = (active_user.get('username') or '').strip()
    form_data = {'recovery_question': '', 'recovery_answer': '', 'recovery_answer_confirm': ''}

    if request.method == 'POST':
        recovery_question = (request.form.get('recovery_question') or '').strip()
        recovery_answer = request.form.get('recovery_answer') or ''
        recovery_answer_confirm = request.form.get('recovery_answer_confirm') or ''
        form_data = {
            'recovery_question': recovery_question,
            'recovery_answer': recovery_answer,
            'recovery_answer_confirm': recovery_answer_confirm,
        }

        if recovery_question not in RECOVERY_QUESTIONS:
            return render_template(
                'recovery_setup.html',
                **_recovery_setup_context('암호찾기 질문을 선택해 주세요.', '', form_data, username),
            )
        if not recovery_answer.strip():
            return render_template(
                'recovery_setup.html',
                **_recovery_setup_context('암호찾기 답변을 입력해 주세요.', '', form_data, username),
            )
        if recovery_answer != recovery_answer_confirm:
            return render_template(
                'recovery_setup.html',
                **_recovery_setup_context('암호찾기 답변이 일치하지 않습니다.', '', form_data, username),
            )

        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                UPDATE users
                SET recovery_question = ?, recovery_answer_hash = ?
                WHERE id = ?
                ''',
                (
                    recovery_question,
                    hash_password(_normalize_recovery_answer(recovery_answer)),
                    active_user['id'],
                ),
            )

            cursor.execute('SELECT * FROM users WHERE id = ?', (active_user['id'],))
            refreshed = cursor.fetchone()

        if not refreshed:
            session.pop('recovery_setup_user', None)
            session.pop('user', None)
            session.pop('workplace', None)
            return redirect(url_for('auth.login'))

        refreshed_user = dict(refreshed)

        if pending_user:
            session.pop('recovery_setup_user', None)
            return _complete_login(refreshed_user)

        workplaces = refreshed_user['workplaces'].split(',') if refreshed_user.get('workplaces') else [WORKPLACES[0]]
        session['user'] = build_session_user(refreshed_user, session.get('workplace') or workplaces[0])
        if len(workplaces) == 1 and not session.get('workplace'):
            session['workplace'] = workplaces[0]
        return render_template(
            'recovery_setup.html',
            **_recovery_setup_context('', '암호찾기 질문과 답변이 저장되었습니다.', {'recovery_question': recovery_question}, username),
        )

    return render_template(
        'recovery_setup.html',
        **_recovery_setup_context(
            '기존 계정은 로그인 후 암호찾기 질문과 답변을 먼저 등록해야 합니다.' if pending_user else '',
            '',
            form_data,
            username,
        ),
    )


@bp.route('/account-recovery', methods=['GET', 'POST'])
def account_recovery():
    mode = (request.values.get('mode') or 'username').strip().lower()
    if mode not in {'username', 'password'}:
        mode = 'username'

    context = {
        'mode': mode,
        'username_result': [],
        'username_form': {
            'name': '',
            'phone': '',
            'email': '',
        },
        'password_form': {
            'username': '',
            'answer': '',
        },
        'password_question': '',
        'password_question_ready': False,
        'username_error': '',
        'password_error': '',
        'password_success': '',
    }

    if request.method == 'POST':
        if mode == 'username':
            name = (request.form.get('name') or '').strip()
            phone = (request.form.get('phone') or '').strip()
            email = (request.form.get('email') or '').strip()
            context['username_form'] = {'name': name, 'phone': phone, 'email': email}

            if not name:
                context['username_error'] = '이름을 입력해 주세요.'
                return render_template('account_recovery.html', **context)
            if not phone and not email:
                context['username_error'] = '연락처 또는 이메일 중 하나는 입력해 주세요.'
                return render_template('account_recovery.html', **context)

            with db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    '''
                    SELECT username, status, name, phone, email
                    FROM users
                    WHERE TRIM(COALESCE(name, '')) = ?
                    ORDER BY created_at DESC, id DESC
                    ''',
                    (name,),
                )
                matched_users = []
                for row in cursor.fetchall():
                    user_row = dict(row)
                    if _match_contact(user_row, phone, email):
                        matched_users.append(
                            {
                                'username': user_row.get('username') or '',
                                'status': user_row.get('status') or '',
                            }
                        )

            if not matched_users:
                context['username_error'] = '입력하신 정보와 일치하는 계정을 찾지 못했습니다.'
            else:
                context['username_result'] = matched_users

            return render_template('account_recovery.html', **context)

        action = (request.form.get('action') or 'lookup').strip().lower()
        username = (request.form.get('username') or '').strip()
        answer = request.form.get('answer') or ''
        context['password_form'] = {
            'username': username,
            'answer': answer,
        }

        if not username:
            context['password_error'] = '아이디를 입력해 주세요.'
            return render_template('account_recovery.html', **context)

        with db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT id, username, recovery_question, recovery_answer_hash
                FROM users
                WHERE username = ?
                ''',
                (username,),
            )
            user_row = cursor.fetchone()

        if not user_row:
            context['password_error'] = '입력하신 아이디의 계정을 찾지 못했습니다.'
            return render_template('account_recovery.html', **context)

        user_row = dict(user_row)
        recovery_question = (user_row.get('recovery_question') or '').strip()
        recovery_answer_hash = (user_row.get('recovery_answer_hash') or '').strip()

        if not recovery_question or not recovery_answer_hash:
            context['password_error'] = '등록된 암호찾기 질문/답변이 없습니다. 로그인 후 먼저 등록해 주세요.'
            return render_template('account_recovery.html', **context)

        context['password_question'] = recovery_question
        context['password_question_ready'] = True

        if action == 'lookup':
            return render_template('account_recovery.html', **context)

        new_password = request.form.get('new_password') or ''
        confirm_password = request.form.get('confirm_password') or ''

        if not answer.strip():
            context['password_error'] = '질문에 대한 답변을 입력해 주세요.'
            return render_template('account_recovery.html', **context)
        if not verify_password(recovery_answer_hash, _normalize_recovery_answer(answer)):
            context['password_error'] = '질문 답변이 일치하지 않습니다.'
            return render_template('account_recovery.html', **context)
        if not new_password:
            context['password_error'] = '새 비밀번호를 입력해 주세요.'
            return render_template('account_recovery.html', **context)
        if len(new_password) < 4:
            context['password_error'] = '비밀번호는 4자 이상으로 입력해 주세요.'
            return render_template('account_recovery.html', **context)
        if new_password != confirm_password:
            context['password_error'] = '새 비밀번호와 확인 비밀번호가 일치하지 않습니다.'
            return render_template('account_recovery.html', **context)

        with db_transaction() as conn:
            cursor = conn.cursor()
            cursor.execute(
                'UPDATE users SET password_hash = ? WHERE id = ?',
                (hash_password(new_password), user_row['id']),
            )

        context['password_success'] = '비밀번호를 새로 설정했습니다. 이제 로그인해 주세요.'
        context['password_form'] = {'username': username, 'answer': ''}
        return render_template('account_recovery.html', **context)

    return render_template('account_recovery.html', **context)
