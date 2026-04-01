from datetime import datetime

from flask import Blueprint, render_template, request, redirect, url_for, session

from core import get_db, login_required, role_required, get_workplace, WORKPLACES, SHARED_WORKPLACE, audit_log

bp = Blueprint('inventory', __name__)


def _get_location_id(cursor, name):
    row = cursor.execute("SELECT id FROM inv_locations WHERE name = ? LIMIT 1", (name,)).fetchone()
    return row['id'] if row else None


def _next_request_no(cursor):
    token = datetime.now().strftime('%Y%m%d')
    prefix = f'IR-{token}-'
    row = cursor.execute(
        "SELECT COALESCE(MAX(CAST(SUBSTR(request_no, -4) AS INTEGER)), 0) + 1 AS n FROM inv_issue_requests WHERE request_no LIKE ?",
        (f'{prefix}%',),
    ).fetchone()
    return f"{prefix}{int(row['n'] if row else 1):04d}"


def _parse_request_items(form):
    raw_material_ids = form.getlist('material_id[]') or form.getlist('material_id')
    raw_qtys = form.getlist('requested_qty[]') or form.getlist('requested_qty')
    raw_notes = form.getlist('item_note[]') or form.getlist('item_note')
    line_items = []
    for idx, material_id in enumerate(raw_material_ids):
        mid = int(material_id or 0)
        qty = float(raw_qtys[idx] or 0) if idx < len(raw_qtys) else 0.0
        line_note = (raw_notes[idx] or '').strip() if idx < len(raw_notes) else ''
        if mid <= 0 or qty <= 0:
            continue
        line_items.append((mid, qty, line_note))
    merged = {}
    for mid, qty, line_note in line_items:
        if mid not in merged:
            merged[mid] = {'qty': 0.0, 'notes': []}
        merged[mid]['qty'] += qty
        if line_note:
            merged[mid]['notes'].append(line_note)
    return merged


def _issue_item_fifo(cursor, item, issue_qty, username):
    material_id = item['material_id']
    remain_need = float(item['requested_qty'] or 0) - float(item['issued_qty'] or 0)
    qty_to_issue = min(float(issue_qty or 0), max(remain_need, 0))
    if qty_to_issue <= 0:
        return 0.0

    wh_loc = _get_location_id(cursor, '물류창고')
    wp_loc = _get_location_id(cursor, item['workplace'])
    if not wh_loc or not wp_loc:
        return 0.0

    lots = cursor.execute(
        '''
        SELECT b.material_lot_id, b.qty, ml.receiving_date, ml.id
        FROM inv_material_lot_balances b
        JOIN material_lots ml ON ml.id = b.material_lot_id
        WHERE b.location_id = ?
          AND ml.material_id = ?
          AND b.qty > 0
        ORDER BY COALESCE(ml.receiving_date, ''), ml.id
        ''',
        (wh_loc, material_id),
    ).fetchall()

    available = sum(float(x['qty'] or 0) for x in lots)
    if available < qty_to_issue:
        qty_to_issue = available
    if qty_to_issue <= 0:
        return 0.0

    remaining = qty_to_issue
    for lot in lots:
        if remaining <= 0:
            break
        lot_qty = float(lot['qty'] or 0)
        use_qty = min(lot_qty, remaining)
        remaining -= use_qty

        cursor.execute(
            '''
            UPDATE inv_material_lot_balances
            SET qty = qty - ?, updated_at = CURRENT_TIMESTAMP
            WHERE location_id = ? AND material_lot_id = ?
            ''',
            (use_qty, wh_loc, lot['material_lot_id']),
        )
        cursor.execute(
            '''
            INSERT INTO inv_material_lot_balances(location_id, material_lot_id, qty, updated_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(location_id, material_lot_id)
            DO UPDATE SET qty = qty + excluded.qty, updated_at = CURRENT_TIMESTAMP
            ''',
            (wp_loc, lot['material_lot_id'], use_qty),
        )
        cursor.execute(
            '''
            INSERT INTO inv_material_txns
            (txn_type, location_from_id, location_to_id, material_id, material_lot_id, qty, ref_type, ref_id, note, created_by)
            VALUES ('ISSUE', ?, ?, ?, ?, ?, 'ISSUE_REQUEST_ITEM', ?, ?, ?)
            ''',
            (wh_loc, wp_loc, material_id, lot['material_lot_id'], use_qty, item['id'], f"{item['request_no']}", username),
        )

    cursor.execute(
        '''
        UPDATE inv_issue_request_items
        SET issued_qty = issued_qty + ?,
            status = CASE WHEN issued_qty + ? >= requested_qty THEN '완료' ELSE '부분출고' END
        WHERE id = ?
        ''',
        (qty_to_issue, qty_to_issue, item['id']),
    )
    return float(qty_to_issue)


@bp.route('/inventory/requests')
@login_required
@role_required('production')
def issue_requests():
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT r.*, COUNT(i.id) AS item_count, COALESCE(SUM(i.requested_qty), 0) AS total_requested, COALESCE(SUM(i.issued_qty), 0) AS total_issued
        FROM inv_issue_requests r
        LEFT JOIN inv_issue_request_items i ON i.request_id = r.id
        WHERE r.workplace = ?
        GROUP BY r.id
        ORDER BY r.id DESC
        ''',
        (workplace,),
    )
    requests = cursor.fetchall()
    request_ids = [int(r['id']) for r in requests]

    items_by_request = {}
    if request_ids:
        ph = ','.join(['?'] * len(request_ids))
        cursor.execute(
            f'''
            SELECT i.request_id, i.id, i.material_id, i.requested_qty, i.issued_qty, i.status, i.note,
                   m.code AS material_code, m.name AS material_name, m.unit
            FROM inv_issue_request_items i
            JOIN materials m ON m.id = i.material_id
            WHERE i.request_id IN ({ph})
            ORDER BY i.request_id DESC, i.id ASC
            ''',
            tuple(request_ids),
        )
        for row in cursor.fetchall():
            rid = int(row['request_id'])
            items_by_request.setdefault(rid, []).append(row)

    cursor.execute(
        '''
        SELECT id, code, name, category, unit
        FROM materials
        WHERE (workplace = ? OR workplace = ?)
        ORDER BY category, name
        ''',
        (workplace, SHARED_WORKPLACE),
    )
    materials = cursor.fetchall()
    conn.close()
    return render_template(
        'inventory_issue_requests.html',
        user=session['user'],
        workplace=workplace,
        requests=requests,
        items_by_request=items_by_request,
        materials=materials,
    )


@bp.route('/inventory/requests/create', methods=['POST'])
@login_required
@role_required('production')
def create_issue_request():
    workplace = get_workplace()
    need_date = (request.form.get('need_date') or '').strip() or None
    header_note = (request.form.get('note') or '').strip()

    merged = _parse_request_items(request.form)
    if not merged:
        # 기존 단일 폼 호환
        material_id = int(request.form.get('material_id') or 0)
        requested_qty = float(request.form.get('requested_qty') or 0)
        if material_id > 0 and requested_qty > 0:
            merged = {material_id: {'qty': requested_qty, 'notes': []}}
    if not merged:
        return redirect(url_for('inventory.issue_requests'))

    conn = get_db()
    cursor = conn.cursor()
    try:
        req_no = _next_request_no(cursor)
        cursor.execute(
            '''
            INSERT INTO inv_issue_requests (request_no, workplace, status, need_date, note, created_by)
            VALUES (?, ?, '요청', ?, ?, ?)
            ''',
            (req_no, workplace, need_date, header_note, (session.get('user') or {}).get('username')),
        )
        req_id = cursor.lastrowid
        for mid, payload in merged.items():
            cursor.execute(
                '''
                INSERT INTO inv_issue_request_items (request_id, material_id, requested_qty, issued_qty, status, note)
                VALUES (?, ?, ?, 0, '요청', ?)
                ''',
                (req_id, mid, round(float(payload['qty']), 4), '; '.join(payload['notes'])[:500]),
            )
        audit_log(
            conn,
            'create',
            'inv_issue_request',
            req_id,
            {
                'request_no': req_no,
                'workplace': workplace,
                'item_count': len(merged),
                'items': [{'material_id': mid, 'requested_qty': round(float(x['qty']), 4)} for mid, x in merged.items()],
            },
        )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.issue_requests'))


@bp.route('/inventory/requests/<int:request_id>/update', methods=['POST'])
@login_required
@role_required('production')
def update_issue_request(request_id):
    workplace = get_workplace()
    need_date = (request.form.get('need_date') or '').strip() or None
    header_note = (request.form.get('note') or '').strip()
    merged = _parse_request_items(request.form)
    if not merged:
        return "<script>alert('요청 품목을 1개 이상 입력해주세요.'); window.history.back();</script>"

    conn = get_db()
    cursor = conn.cursor()
    try:
        req = cursor.execute(
            "SELECT id, workplace, status FROM inv_issue_requests WHERE id = ?",
            (request_id,),
        ).fetchone()
        if not req or req['workplace'] != workplace:
            return "<script>alert('수정 권한이 없습니다.'); window.history.back();</script>"
        if req['status'] in ('완료', '취소'):
            return "<script>alert('완료/취소된 요청은 수정할 수 없습니다.'); window.history.back();</script>"

        row = cursor.execute(
            "SELECT COALESCE(SUM(issued_qty), 0) AS issued_total FROM inv_issue_request_items WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if float(row['issued_total'] or 0) > 0:
            return "<script>alert('이미 출고된 요청은 수정할 수 없습니다.'); window.history.back();</script>"

        cursor.execute(
            '''
            UPDATE inv_issue_requests
            SET need_date = ?, note = ?, status = '요청'
            WHERE id = ?
            ''',
            (need_date, header_note, request_id),
        )
        cursor.execute("DELETE FROM inv_issue_request_items WHERE request_id = ?", (request_id,))
        for mid, payload in merged.items():
            cursor.execute(
                '''
                INSERT INTO inv_issue_request_items (request_id, material_id, requested_qty, issued_qty, status, note)
                VALUES (?, ?, ?, 0, '요청', ?)
                ''',
                (request_id, mid, round(float(payload['qty']), 4), '; '.join(payload['notes'])[:500]),
            )
        audit_log(conn, 'update', 'inv_issue_request', request_id, {'action': 'update', 'item_count': len(merged)})
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.issue_requests'))


@bp.route('/inventory/requests/<int:request_id>/cancel', methods=['POST'])
@login_required
@role_required('production')
def cancel_issue_request(request_id):
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()
    try:
        req = cursor.execute(
            "SELECT id, workplace, status FROM inv_issue_requests WHERE id = ?",
            (request_id,),
        ).fetchone()
        if not req or req['workplace'] != workplace:
            return "<script>alert('취소 권한이 없습니다.'); window.history.back();</script>"
        if req['status'] in ('완료', '취소'):
            return redirect(url_for('inventory.issue_requests'))

        row = cursor.execute(
            "SELECT COALESCE(SUM(issued_qty), 0) AS issued_total FROM inv_issue_request_items WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        if float(row['issued_total'] or 0) > 0:
            return "<script>alert('이미 출고된 요청은 취소할 수 없습니다.'); window.history.back();</script>"

        cursor.execute("UPDATE inv_issue_requests SET status = '취소' WHERE id = ?", (request_id,))
        cursor.execute("UPDATE inv_issue_request_items SET status = '취소' WHERE request_id = ?", (request_id,))
        audit_log(conn, 'update', 'inv_issue_request', request_id, {'action': 'cancel'})
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.issue_requests'))


@bp.route('/inventory/logistics')
@login_required
@role_required('purchase')
def logistics_issue_requests():
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute(
        '''
        SELECT r.*, i.id AS item_id, i.material_id, i.requested_qty, i.issued_qty, i.status AS item_status,
               m.code AS material_code, m.name AS material_name, m.unit
        FROM inv_issue_requests r
        JOIN inv_issue_request_items i ON i.request_id = r.id
        JOIN materials m ON m.id = i.material_id
        WHERE r.status IN ('요청', '부분출고', '승인')
          AND i.status IN ('요청', '부분출고')
        ORDER BY r.id DESC, i.id ASC
        '''
    )
    rows = cursor.fetchall()
    grouped_requests = []
    group_map = {}

    # 물류창고 가용 재고(로트 합)
    cursor.execute(
        '''
        SELECT ml.material_id, COALESCE(SUM(b.qty), 0) AS wh_qty
        FROM inv_material_lot_balances b
        JOIN inv_locations l ON l.id = b.location_id
        JOIN material_lots ml ON ml.id = b.material_lot_id
        WHERE l.name = '물류창고'
        GROUP BY ml.material_id
        '''
    )
    wh_map = {r['material_id']: float(r['wh_qty'] or 0) for r in cursor.fetchall()}
    for row in rows:
        rid = int(row['id'])
        if rid not in group_map:
            group = {
                'id': rid,
                'request_no': row['request_no'],
                'workplace': row['workplace'],
                'status': row['status'],
                'need_date': row['need_date'],
                'created_by': row['created_by'],
                'created_at': row['created_at'],
                'item_rows': [],
            }
            grouped_requests.append(group)
            group_map[rid] = group
        group_map[rid]['item_rows'].append(
            {
                'item_id': row['item_id'],
                'material_id': row['material_id'],
                'material_code': row['material_code'],
                'material_name': row['material_name'],
                'unit': row['unit'],
                'requested_qty': float(row['requested_qty'] or 0),
                'issued_qty': float(row['issued_qty'] or 0),
                'item_status': row['item_status'],
                'wh_qty': float(wh_map.get(row['material_id'], 0)),
            }
        )
    conn.close()
    return render_template(
        'inventory_logistics.html',
        user=session['user'],
        grouped_requests=grouped_requests,
        workplaces=WORKPLACES,
    )


@bp.route('/inventory/logistics/<int:request_id>/approve', methods=['POST'])
@login_required
@role_required('purchase')
def approve_issue_request(request_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        username = (session.get('user') or {}).get('username')
        cursor.execute(
            '''
            UPDATE inv_issue_requests
            SET status = CASE WHEN status = '요청' THEN '승인' ELSE status END,
                approved_by = COALESCE(approved_by, ?),
                approved_at = COALESCE(approved_at, CURRENT_TIMESTAMP)
            WHERE id = ?
            ''',
            (username, request_id),
        )
        audit_log(conn, 'update', 'inv_issue_request', request_id, {'action': 'approve'})
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.logistics_issue_requests'))


@bp.route('/inventory/logistics/<int:request_id>/reject', methods=['POST'])
@login_required
@role_required('purchase')
def reject_issue_request(request_id):
    reject_note = (request.form.get('reject_note') or '').strip()
    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            UPDATE inv_issue_request_items
            SET status = CASE WHEN COALESCE(issued_qty, 0) > 0 THEN status ELSE '취소' END,
                note = CASE
                    WHEN ? = '' THEN note
                    WHEN note IS NULL OR TRIM(note) = '' THEN ?
                    ELSE note || ' / 반려: ' || ?
                END
            WHERE request_id = ?
            ''',
            (reject_note, f'반려:{reject_note}', reject_note, request_id),
        )
        cursor.execute(
            '''
            UPDATE inv_issue_requests
            SET status = '취소',
                note = CASE
                    WHEN ? = '' THEN note
                    WHEN note IS NULL OR TRIM(note) = '' THEN '반려: ' || ?
                    ELSE note || ' / 반려: ' || ?
                END
            WHERE id = ?
            ''',
            (reject_note, reject_note, reject_note, request_id),
        )
        audit_log(conn, 'update', 'inv_issue_request', request_id, {'action': 'reject', 'note': reject_note})
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.logistics_issue_requests'))


@bp.route('/inventory/logistics/<int:request_id>/issue-all', methods=['POST'])
@login_required
@role_required('purchase')
def issue_all_for_request(request_id):
    conn = get_db()
    cursor = conn.cursor()
    try:
        req = cursor.execute(
            "SELECT id, request_no, workplace, status FROM inv_issue_requests WHERE id = ?",
            (request_id,),
        ).fetchone()
        if not req or req['status'] in ('취소',):
            return redirect(url_for('inventory.logistics_issue_requests'))

        items = cursor.execute(
            '''
            SELECT i.*, r.workplace, r.request_no
            FROM inv_issue_request_items i
            JOIN inv_issue_requests r ON r.id = i.request_id
            WHERE i.request_id = ?
              AND i.status IN ('요청', '부분출고')
            ORDER BY i.id ASC
            ''',
            (request_id,),
        ).fetchall()
        username = (session.get('user') or {}).get('username')
        total_issued = 0.0
        for item in items:
            remain_need = float(item['requested_qty'] or 0) - float(item['issued_qty'] or 0)
            if remain_need <= 0:
                continue
            total_issued += _issue_item_fifo(cursor, item, remain_need, username)

        cursor.execute(
            '''
            UPDATE inv_issue_requests
            SET status = (
                CASE
                    WHEN EXISTS(SELECT 1 FROM inv_issue_request_items x WHERE x.request_id = inv_issue_requests.id AND x.status != '완료')
                    THEN '부분출고'
                    ELSE '완료'
                END
            ),
            approved_by = COALESCE(approved_by, ?),
            approved_at = COALESCE(approved_at, CURRENT_TIMESTAMP)
            WHERE id = ?
            ''',
            (username, request_id),
        )
        audit_log(conn, 'update', 'inv_issue_request', request_id, {'action': 'issue_all', 'issued_qty': round(total_issued, 4)})
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.logistics_issue_requests'))


@bp.route('/inventory/logistics/<int:item_id>/issue', methods=['POST'])
@login_required
@role_required('purchase')
def issue_to_workplace(item_id):
    issue_qty = float(request.form.get('issue_qty') or 0)
    if issue_qty <= 0:
        return redirect(url_for('inventory.logistics_issue_requests'))

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT i.*, r.workplace, r.id AS request_id, r.request_no, r.status AS request_status
            FROM inv_issue_request_items i
            JOIN inv_issue_requests r ON r.id = i.request_id
            WHERE i.id = ?
            ''',
            (item_id,),
        )
        item = cursor.fetchone()
        if not item:
            return redirect(url_for('inventory.logistics_issue_requests'))
        if (item['request_status'] or '') in ('취소',):
            return redirect(url_for('inventory.logistics_issue_requests'))
        if (item['status'] or '') not in ('요청', '부분출고'):
            return redirect(url_for('inventory.logistics_issue_requests'))

        remain_need = float(item['requested_qty'] or 0) - float(item['issued_qty'] or 0)
        if remain_need <= 0:
            return redirect(url_for('inventory.logistics_issue_requests'))
        qty_to_issue = min(issue_qty, remain_need)
        if qty_to_issue <= 0:
            return redirect(url_for('inventory.logistics_issue_requests'))

        username = (session.get('user') or {}).get('username')
        qty_issued = _issue_item_fifo(cursor, item, qty_to_issue, username)
        if qty_issued <= 0:
            return redirect(url_for('inventory.logistics_issue_requests'))

        cursor.execute(
            '''
            UPDATE inv_issue_requests
            SET status = (
                CASE
                    WHEN EXISTS(SELECT 1 FROM inv_issue_request_items x WHERE x.request_id = inv_issue_requests.id AND x.status != '완료')
                    THEN '부분출고'
                    ELSE '완료'
                END
            ),
            approved_by = COALESCE(approved_by, ?),
            approved_at = COALESCE(approved_at, CURRENT_TIMESTAMP)
            WHERE id = ?
            ''',
            (username, item['request_id']),
        )
        audit_log(conn, 'update', 'inv_issue_request_item', item_id, {'issued_qty': qty_issued, 'workplace': item['workplace']})
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for('inventory.logistics_issue_requests'))
