from flask import Blueprint, render_template, request, redirect, url_for, session

from core import get_db, admin_required, WORKPLACES

bp = Blueprint('users', __name__)


def _normalize_role_input(role_value):
    role = (role_value or 'readonly').strip()
    return role


def _normalize_workplaces_input(values):
    seen = set()
    normalized = []
    for value in values or []:
        cleaned = (value or '').strip()
        if not cleaned or cleaned not in WORKPLACES or cleaned in seen:
            continue
        seen.add(cleaned)
        normalized.append(cleaned)
    return normalized


@bp.route('/users')
@admin_required
def user_management():
    """사용자 관리 (관리자 전용)"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, username, name, is_admin, role, department, phone, email, created_at, status, workplaces
        FROM users
        WHERE status = 'approved'
        ORDER BY created_at DESC
    """)
    users_list = [dict(row) for row in cursor.fetchall()]

    cursor.execute("""
        SELECT id, username, name, phone, email, department, workplace1, workplace2, created_at
        FROM users
        WHERE status = 'pending'
        ORDER BY created_at DESC
    """)
    pending_users = [dict(row) for row in cursor.fetchall()]
    conn.close()

    for user_row in users_list:
        workplaces = [value.strip() for value in (user_row.get('workplaces') or '').split(',') if value.strip()]
        user_row['workplace_list'] = workplaces
        user_row['role_value'] = user_row.get('role') or ('admin' if user_row.get('is_admin') else 'readonly')

    for pending_row in pending_users:
        requested = []
        for field_name in ('workplace1', 'workplace2'):
            cleaned = (pending_row.get(field_name) or '').strip()
            if cleaned and cleaned not in requested:
                requested.append(cleaned)
        pending_row['requested_workplaces'] = requested

    return render_template('user_management.html',
                           user=session['user'],
                           users_list=users_list,
                           pending_users=pending_users,
                           session_user_id=session['user']['id'],
                           workplaces=WORKPLACES)


@bp.route('/users/<int:user_id>/update-role', methods=['POST'])
@admin_required
def update_user_role(user_id):
    """사용자 권한 변경"""
    role = _normalize_role_input(request.form.get('role', 'readonly'))
    conn = get_db()
    conn.execute("UPDATE users SET role=? WHERE id=?", (role, user_id))
    conn.commit()
    conn.close()
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/update-workplaces', methods=['POST'])
@admin_required
def update_user_workplaces(user_id):
    """사용자 작업장 변경"""
    workplaces = _normalize_workplaces_input(request.form.getlist('workplaces'))
    if not workplaces:
        return redirect(url_for('users.user_management'))
    workplaces_str = ','.join(workplaces)
    conn = get_db()
    conn.execute("UPDATE users SET workplaces=? WHERE id=?", (workplaces_str, user_id))
    conn.commit()
    conn.close()

    if session.get('user') and session['user']['id'] == user_id:
        session['user']['workplaces'] = workplaces
        if session.get('workplace') not in workplaces:
            session['workplace'] = workplaces[0]
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/update-access', methods=['POST'])
@admin_required
def update_user_access(user_id):
    """권한과 작업장을 함께 수정"""
    if user_id == session['user']['id']:
        return redirect(url_for('users.user_management'))

    role = _normalize_role_input(request.form.get('role', 'readonly'))
    workplaces = _normalize_workplaces_input(request.form.getlist('workplaces'))
    if not workplaces:
        return redirect(url_for('users.user_management'))

    conn = get_db()
    conn.execute("UPDATE users SET role=?, workplaces=? WHERE id=?", (role, ','.join(workplaces), user_id))
    conn.commit()
    conn.close()
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/approve', methods=['POST'])
@admin_required
def approve_user(user_id):
    """회원가입 승인"""
    role = _normalize_role_input(request.form.get('role', 'readonly'))
    workplaces = _normalize_workplaces_input(request.form.getlist('workplaces'))
    if not workplaces:
        return redirect(url_for('users.user_management'))
    workplaces_str = ','.join(workplaces)
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT username, name FROM users WHERE id = ?", (user_id,))
    target_user = cursor.fetchone()
    conn.execute(
        "UPDATE users SET status='approved', role=?, workplaces=? WHERE id=?",
        (role, workplaces_str, user_id)
    )
    if target_user:
        notification_titles = []
        username = (target_user['username'] or '').strip()
        name = (target_user['name'] or '').strip()
        if username:
            notification_titles.append(f'신규 회원가입 요청: {username}')
        if name and name != username:
            notification_titles.append(f'신규 회원가입 요청: {name}')
        for title in notification_titles:
            conn.execute("DELETE FROM user_notifications WHERE title = ? AND link = '/users'", (title,))
    conn.commit()
    conn.close()
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/reject', methods=['POST'])
@admin_required
def reject_user(user_id):
    """회원가입 반려"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute("SELECT username, name FROM users WHERE id = ?", (user_id,))
    target_user = cursor.fetchone()
    conn.execute("UPDATE users SET status='rejected' WHERE id=?", (user_id,))
    if target_user:
        notification_titles = []
        username = (target_user['username'] or '').strip()
        name = (target_user['name'] or '').strip()
        if username:
            notification_titles.append(f'신규 회원가입 요청: {username}')
        if name and name != username:
            notification_titles.append(f'신규 회원가입 요청: {name}')
        for title in notification_titles:
            conn.execute("DELETE FROM user_notifications WHERE title = ? AND link = '/users'", (title,))
    conn.commit()
    conn.close()
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    """사용자 삭제 (본인 제외)"""
    if user_id == session['user']['id']:
        return redirect(url_for('users.user_management'))
    conn = get_db()
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return redirect(url_for('users.user_management'))
