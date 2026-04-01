from flask import Blueprint, render_template, request, redirect, url_for, session

from core import get_db, admin_required, WORKPLACES

bp = Blueprint('users', __name__)


def _normalize_role_input(role_value):
    role = (role_value or 'readonly').strip()
    return role


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
    users_list = cursor.fetchall()

    cursor.execute("""
        SELECT id, username, name, phone, email, department, workplace1, workplace2, created_at
        FROM users
        WHERE status = 'pending'
        ORDER BY created_at DESC
    """)
    pending_users = cursor.fetchall()
    conn.close()
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
    workplaces = request.form.getlist('workplaces')
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


@bp.route('/users/<int:user_id>/approve', methods=['POST'])
@admin_required
def approve_user(user_id):
    """회원가입 승인"""
    role = _normalize_role_input(request.form.get('role', 'readonly'))
    workplaces = request.form.getlist('workplaces')
    if not workplaces:
        return redirect(url_for('users.user_management'))
    workplaces_str = ','.join(workplaces)
    conn = get_db()
    conn.execute(
        "UPDATE users SET status='approved', role=?, workplaces=? WHERE id=?",
        (role, workplaces_str, user_id)
    )
    conn.commit()
    conn.close()
    return redirect(url_for('users.user_management'))


@bp.route('/users/<int:user_id>/reject', methods=['POST'])
@admin_required
def reject_user(user_id):
    """회원가입 반려"""
    conn = get_db()
    conn.execute("UPDATE users SET status='rejected' WHERE id=?", (user_id,))
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
