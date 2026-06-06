from flask import Blueprint, render_template, request, redirect, url_for, session

from core import (
    WORKPLACES,
    USER_ROLE_OPTIONS,
    admin_required,
    build_session_user,
    db_connection,
    db_transaction,
    dump_workplace_roles,
    normalize_user_role,
    parse_workplace_roles,
)

bp = Blueprint('users', __name__)


def _normalize_role_input(role_value):
    return normalize_user_role(role_value)


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


def _parse_workplace_roles_input(form, workplaces, fallback_role='readonly'):
    selected = set(workplaces or [])
    keys = form.getlist('workplace_role_keys[]')
    values = form.getlist('workplace_role_values[]')
    role_map = {}
    for idx, raw_key in enumerate(keys):
        workplace = (raw_key or '').strip()
        if workplace not in selected:
            continue
        role_value = values[idx] if idx < len(values) else fallback_role
        role_map[workplace] = _normalize_role_input(role_value or fallback_role)
    for workplace in selected:
        role_map.setdefault(workplace, _normalize_role_input(fallback_role))
    return role_map


def _parse_integrated_access_input(form):
    return 1 if str(form.get('can_integrated_management') or '').strip() in {'1', 'true', 'on', 'yes'} else 0


@bp.route('/users')
@admin_required
def user_management():
    """User management view for admins."""
    with db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, username, name, is_admin, role, department, phone, email, created_at, status, workplaces, workplace_roles, can_integrated_management
            FROM users
            WHERE status = 'approved'
            ORDER BY created_at DESC
            """
        )
        users_list = [dict(row) for row in cursor.fetchall()]

        cursor.execute(
            """
            SELECT id, username, name, phone, email, department, workplace1, workplace2, created_at
            FROM users
            WHERE status = 'pending'
            ORDER BY created_at DESC
            """
        )
        pending_users = [dict(row) for row in cursor.fetchall()]

    for user_row in users_list:
        workplaces = [value.strip() for value in (user_row.get('workplaces') or '').split(',') if value.strip()]
        user_row['workplace_list'] = workplaces
        user_row['role_value'] = _normalize_role_input(
            user_row.get('role') or ('admin' if user_row.get('is_admin') else 'readonly')
        )
        user_row['workplace_roles_map'] = parse_workplace_roles(user_row.get('workplace_roles'))
        user_row['can_integrated_management'] = bool(user_row.get('can_integrated_management'))

    for pending_row in pending_users:
        requested = []
        for field_name in ('workplace1', 'workplace2'):
            cleaned = (pending_row.get(field_name) or '').strip()
            if cleaned and cleaned not in requested:
                requested.append(cleaned)
        pending_row['requested_workplaces'] = requested

    return render_template(
        'user_management.html',
        user=session['user'],
        users_list=users_list,
        pending_users=pending_users,
        session_user_id=session['user']['id'],
        workplaces=WORKPLACES,
        role_options=[role for role in USER_ROLE_OPTIONS if role != 'admin'] + ['admin'],
    )


@bp.route('/users/<int:user_id>/update-role', methods=['POST'])
@admin_required
def update_user_role(user_id):
    """Update a user's base role."""
    role = _normalize_role_input(request.form.get('role', 'readonly'))
    with db_transaction() as conn:
        conn.execute("UPDATE users SET role=? WHERE id=?", (role, user_id))
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/update-workplaces', methods=['POST'])
@admin_required
def update_user_workplaces(user_id):
    """Update the workplaces assigned to a user."""
    workplaces = _normalize_workplaces_input(request.form.getlist('workplaces'))
    if not workplaces:
        return redirect(url_for('users.user_management'))

    workplaces_str = ','.join(workplaces)
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT role, workplace_roles FROM users WHERE id = ?", (user_id,))
        target = cursor.fetchone()
        role = _normalize_role_input((target['role'] if target else None) or 'readonly')
        existing_map = parse_workplace_roles(target['workplace_roles'] if target else None)
        filtered_map = {wp: existing_map.get(wp, role) for wp in workplaces}
        conn.execute(
            "UPDATE users SET workplaces=?, workplace_roles=? WHERE id=?",
            (workplaces_str, dump_workplace_roles(filtered_map, workplaces), user_id),
        )

    if session.get('user') and session['user']['id'] == user_id:
        session['user']['workplaces'] = workplaces
        session['user']['workplace_roles'] = filtered_map
        if session.get('workplace') not in workplaces:
            session['workplace'] = workplaces[0]
        session['user'] = build_session_user(session['user'], session.get('workplace'))
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/update-access', methods=['POST'])
@admin_required
def update_user_access(user_id):
    """Update role/workplace access in one step."""
    if user_id == session['user']['id']:
        return redirect(url_for('users.user_management'))

    role = _normalize_role_input(request.form.get('role', 'readonly'))
    workplaces = _normalize_workplaces_input(request.form.getlist('workplaces'))
    if not workplaces:
        return redirect(url_for('users.user_management'))

    workplace_roles = _parse_workplace_roles_input(request.form, workplaces, role)
    can_integrated_management = _parse_integrated_access_input(request.form)

    with db_transaction() as conn:
        conn.execute(
            "UPDATE users SET role=?, workplaces=?, workplace_roles=?, can_integrated_management=? WHERE id=?",
            (role, ','.join(workplaces), dump_workplace_roles(workplace_roles, workplaces), can_integrated_management, user_id),
        )
        if session.get('user') and session['user']['id'] == user_id:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE id = ?", (user_id,))
            refreshed = cursor.fetchone()
            if refreshed:
                session['user'] = build_session_user(dict(refreshed), session.get('workplace'))
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/approve', methods=['POST'])
@admin_required
def approve_user(user_id):
    """Approve a pending user."""
    role = _normalize_role_input(request.form.get('role', 'readonly'))
    workplaces = _normalize_workplaces_input(request.form.getlist('workplaces'))
    if not workplaces:
        return redirect(url_for('users.user_management'))

    workplace_roles = _parse_workplace_roles_input(request.form, workplaces, role)
    can_integrated_management = _parse_integrated_access_input(request.form)
    workplaces_str = ','.join(workplaces)

    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT username, name FROM users WHERE id = ?", (user_id,))
        target_user = cursor.fetchone()
        conn.execute(
            "UPDATE users SET status='approved', role=?, workplaces=?, workplace_roles=?, can_integrated_management=? WHERE id=?",
            (role, workplaces_str, dump_workplace_roles(workplace_roles, workplaces), can_integrated_management, user_id),
        )
        if target_user:
            notification_titles = []
            username = (target_user['username'] or '').strip()
            name = (target_user['name'] or '').strip()
            if username:
                notification_titles.append(f'?좉퇋 ?뚯썝媛???붿껌: {username}')
            if name and name != username:
                notification_titles.append(f'?좉퇋 ?뚯썝媛???붿껌: {name}')
            for title in notification_titles:
                conn.execute("DELETE FROM user_notifications WHERE title = ? AND link = '/users'", (title,))
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/reject', methods=['POST'])
@admin_required
def reject_user(user_id):
    """Reject a pending user."""
    with db_transaction() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT username, name FROM users WHERE id = ?", (user_id,))
        target_user = cursor.fetchone()
        conn.execute("UPDATE users SET status='rejected' WHERE id=?", (user_id,))
        if target_user:
            notification_titles = []
            username = (target_user['username'] or '').strip()
            name = (target_user['name'] or '').strip()
            if username:
                notification_titles.append(f'?좉퇋 ?뚯썝媛???붿껌: {username}')
            if name and name != username:
                notification_titles.append(f'?좉퇋 ?뚯썝媛???붿껌: {name}')
            for title in notification_titles:
                conn.execute("DELETE FROM user_notifications WHERE title = ? AND link = '/users'", (title,))
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/delete', methods=['POST'])
@admin_required
def delete_user(user_id):
    """Delete a user other than the current session user."""
    if user_id == session['user']['id']:
        return redirect(url_for('users.user_management'))
    with db_transaction() as conn:
        conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    return redirect(url_for('users.user_management'))
