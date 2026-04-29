from flask import Blueprint, render_template, session, redirect, url_for, request, abort
from datetime import datetime, timedelta, date
from collections import defaultdict
import math
import calendar

from core import get_db, login_required, get_workplace, SHARED_WORKPLACE
from .production import _get_production_material_section, _get_production_material_sort_key

bp = Blueprint('printouts', __name__)


def _exclude_from_production_print(row):
    name = (row.get('material_name') or '').strip()
    excluded_keywords = ('뚜껑', '밑판', '앵글')
    return any(keyword in name for keyword in excluded_keywords)


def _round_1(value):
    if value is None:
        return None
    return round(float(value), 1)


def _format_print_workplace(workplace):
    text = (workplace or '').strip()
    mapping = {
        '1동 조미': '1동 조미',
        '1동 자반': '1동 자반',
        '2동 신관 1층': '2동 신관 1층',
        '2동 신관 2층': '2동 신관 2층',
    }
    return mapping.get(text, text)


def _base_material_category(category):
    text = (category or '').strip()
    return text == '기름' or text == '소금' or '기름' in text or '유지' in text or '소금' in text


def _normalize_completed_status(value):
    text = (value or '').strip()
    return text == '완료' or '완료' in text


def _base_material_category_override(category):
    text = (category or '').strip()
    return text == '기름' or text == '소금' or '기름' in text or '유지' in text or '소금' in text


def _normalize_completed_status_override(value):
    text = (value or '').strip()
    return text == '완료' or '완료' in text


_base_material_category = _base_material_category_override
_normalize_completed_status = _normalize_completed_status_override


def _resolve_journal_date_range():
    today = date.today()
    default_from = today - timedelta(days=6)
    date_from = (request.args.get('date_from') or '').strip()
    date_to = (request.args.get('date_to') or '').strip()
    try:
        parsed_from = datetime.strptime(date_from, '%Y-%m-%d').date() if date_from else default_from
    except Exception:
        parsed_from = default_from
    try:
        parsed_to = datetime.strptime(date_to, '%Y-%m-%d').date() if date_to else today
    except Exception:
        parsed_to = today
    if parsed_from > parsed_to:
        parsed_from, parsed_to = parsed_to, parsed_from
    return parsed_from.isoformat(), parsed_to.isoformat()


def _get_print_inventory_location_ids(cursor, workplace):
    target = (workplace or '').strip()
    if not target:
        return []
    rows = cursor.execute(
        '''
        SELECT id
        FROM inv_locations
        WHERE name = ?
           OR COALESCE(workplace_code, '') = ?
           OR REPLACE(COALESCE(name, ''), ' ', '') = REPLACE(?, ' ', '')
           OR REPLACE(COALESCE(workplace_code, ''), ' ', '') = REPLACE(?, ' ', '')
        ORDER BY CASE WHEN name = ? THEN 0 ELSE 1 END, id
        ''',
        (target, target, target, target, target),
    ).fetchall()
    return [int(row['id']) for row in rows if int(row['id'] or 0) > 0]


def _get_print_workday(timestamp_text, cutoff_hour=6):
    text = (timestamp_text or '').strip()
    if not text:
        return ''
    try:
        dt = datetime.strptime(text[:19], '%Y-%m-%d %H:%M:%S')
    except Exception:
        try:
            dt = datetime.strptime(text[:10], '%Y-%m-%d')
        except Exception:
            return text[:10]
    if getattr(dt, 'hour', 0) < cutoff_hour:
        dt = dt - timedelta(days=1)
    return dt.strftime('%Y-%m-%d')


@bp.route('/journals')
@login_required
def journals():
    workplace = get_workplace()
    selected_tab = (request.args.get('tab') or 'production').strip()
    raw_status = (request.args.get('raw_status') or 'active').strip()
    if raw_status not in ('active', 'done'):
        raw_status = 'active'
    date_from, date_to = _resolve_journal_date_range()

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT
                pr.id,
                pr.production_date,
                pr.actual_boxes,
                pr.planned_boxes,
                pr.status,
                COALESCE(p.code, '') as product_code,
                COALESCE(p.name, '') as product_name
            FROM productions pr
            LEFT JOIN products p ON p.id = pr.product_id
            WHERE pr.workplace = ?
              AND COALESCE(pr.production_date, '') BETWEEN ? AND ?
            ORDER BY pr.production_date DESC, pr.id DESC
            LIMIT 240
            ''',
            (workplace, date_from, date_to),
        )
        production_rows = []
        for row in cursor.fetchall():
            item = dict(row)
            if _normalize_completed_status(item.get('status')):
                production_rows.append(item)

        cursor.execute(
            '''
            SELECT
                id,
                COALESCE(code, '') as code,
                COALESCE(name, '') as name,
                COALESCE(receiving_date, '') as receiving_date,
                COALESCE(current_stock, 0) as current_stock,
                COALESCE(used_quantity, 0) as used_quantity
            FROM raw_materials
            WHERE workplace = ?
            ORDER BY COALESCE(current_stock, 0) DESC, COALESCE(receiving_date, '') DESC, name
            ''',
            (workplace,),
        )
        raw_all_items = [dict(row) for row in cursor.fetchall()]
        raw_active_items = [row for row in raw_all_items if float(row.get('current_stock') or 0) > 0]
        raw_done_items = [row for row in raw_all_items if float(row.get('current_stock') or 0) <= 0 and float(row.get('used_quantity') or 0) > 0]

        cursor.execute(
            '''
            SELECT
                p.production_date,
                p.status,
                COALESCE(m.category, '') as category,
                pmu.material_id,
                COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty
            FROM production_material_usage pmu
            JOIN productions p ON p.id = pmu.production_id
            LEFT JOIN materials m ON m.id = pmu.material_id
            LEFT JOIN production_material_lot_usage pmlu
              ON pmlu.production_usage_id = pmu.id
            WHERE p.workplace = ?
              AND COALESCE(p.production_date, '') BETWEEN ? AND ?
              AND pmu.material_id IS NOT NULL
            ORDER BY p.production_date DESC, pmu.material_id
            ''',
            (workplace, date_from, date_to),
        )
        material_map = {}
        for row in cursor.fetchall():
            row = dict(row)
            if not _normalize_completed_status(row.get('status')):
                continue
            if not _base_material_category(row.get('category')):
                continue
            key = (row.get('production_date') or '').strip()
            if not key:
                continue
            bucket = material_map.setdefault(key, {'production_date': key, 'item_ids': set(), 'outgoing_total': 0.0})
            if row.get('material_id'):
                bucket['item_ids'].add(int(row['material_id']))
            bucket['outgoing_total'] += float(row.get('qty') or 0)
        material_journal_dates = [
            {
                'production_date': key,
                'item_count': len(bucket['item_ids']),
                'outgoing_total': round(bucket['outgoing_total'], 1),
            }
            for key, bucket in sorted(material_map.items(), key=lambda item: item[0], reverse=True)
        ]

        return render_template(
            'journals.html',
            user=session['user'],
            selected_tab=selected_tab,
            raw_status=raw_status,
            date_from=date_from,
            date_to=date_to,
            current_workplace=workplace,
            production_rows=production_rows,
            raw_active_items=raw_active_items,
            raw_done_items=raw_done_items,
            material_journal_dates=material_journal_dates,
        )
    finally:
        conn.close()


@bp.route('/materials/checksheet-preview')
@login_required
def material_checksheet_preview():
    workplace = get_workplace()
    selected_date = (request.args.get('date') or date.today().isoformat()).strip()
    try:
        report_date = datetime.strptime(selected_date, '%Y-%m-%d').date()
    except Exception:
        report_date = date.today()
        selected_date = report_date.isoformat()

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT
                p.id as production_id,
                p.status,
                pmu.id as production_usage_id,
                pmu.material_id as id,
                COALESCE(m.code, '') as code,
                COALESCE(m.name, '') as name,
                COALESCE(m.category, '') as category,
                COALESCE(m.unit, '') as unit,
                COALESCE(s.name, '') as supplier_name,
                COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty
            FROM production_material_usage pmu
            JOIN productions p ON p.id = pmu.production_id
            LEFT JOIN materials m ON m.id = pmu.material_id
            LEFT JOIN suppliers s ON s.id = m.supplier_id
            LEFT JOIN production_material_lot_usage pmlu
              ON pmlu.production_usage_id = pmu.id
            WHERE p.workplace = ?
              AND COALESCE(p.production_date, '') = ?
              AND pmu.material_id IS NOT NULL
            ORDER BY pmu.material_id, pmu.id
            ''',
            (workplace, selected_date),
        )
        material_rows = [dict(row) for row in cursor.fetchall()]

        completed_production_ids = set()
        material_map = {}
        for row in material_rows:
            if not _normalize_completed_status(row.get('status')):
                continue
            if not _base_material_category(row.get('category')):
                continue
            material_id = int(row.get('id') or 0)
            if material_id <= 0:
                continue
            completed_production_ids.add(int(row.get('production_id') or 0))
            item = material_map.setdefault(
                material_id,
                {
                    'id': material_id,
                    'code': row.get('code') or '',
                    'name': row.get('name') or '',
                    'category': row.get('category') or '',
                    'unit': row.get('unit') or '',
                    'supplier_name': row.get('supplier_name') or '',
                    'outgoing_today': 0.0,
                },
            )
            item['outgoing_today'] += float(row.get('qty') or 0)

        materials = list(material_map.values())
        for row in materials:
            row['base_sort_key'] = _get_production_material_sort_key({'category': row.get('category'), 'material_name': row.get('name')})
        materials.sort(key=lambda item: (item.get('base_sort_key') or '', item.get('name') or ''))

        material_ids = [int(row['id']) for row in materials if int(row.get('id') or 0) > 0]
        primary_lot_map = {}
        opening_stock_map = {}
        received_today_map = {}
        outgoing_today_map = {}
        closing_stock_map = {}

        if material_ids:
            placeholders = ','.join(['?'] * len(material_ids))
            prod_placeholders = ','.join(['?'] * len(completed_production_ids)) if completed_production_ids else ''
            workplace_prefix = (workplace or '').strip()

            if completed_production_ids:
                cursor.execute(
                    f'''
                    SELECT
                        pmlu.material_id,
                        pmlu.material_lot_id,
                        ml.receiving_date,
                        ml.manufacture_date,
                        ml.expiry_date,
                        SUM(COALESCE(pmlu.quantity, 0)) as used_qty
                    FROM production_material_lot_usage pmlu
                    LEFT JOIN material_lots ml ON ml.id = pmlu.material_lot_id
                    WHERE pmlu.production_id IN ({prod_placeholders})
                      AND pmlu.material_id IN ({placeholders})
                    GROUP BY pmlu.material_id, pmlu.material_lot_id, ml.receiving_date, ml.manufacture_date, ml.expiry_date
                    ORDER BY pmlu.material_id, used_qty DESC, pmlu.material_lot_id DESC
                    ''',
                    [*completed_production_ids, *material_ids],
                )
                for row in cursor.fetchall():
                    material_id = int(row['material_id'] or 0)
                    if material_id > 0 and material_id not in primary_lot_map:
                        primary_lot_map[material_id] = dict(row)

            primary_lot_ids = [int(info['material_lot_id']) for info in primary_lot_map.values() if int(info.get('material_lot_id') or 0) > 0]
            if primary_lot_ids:
                lot_placeholders = ','.join(['?'] * len(primary_lot_ids))
                stock_state_map = {}
                cursor.execute(
                    f'''
                    SELECT material_lot_id, material_id, action, quantity, note, created_at
                    FROM material_lot_logs
                    WHERE material_lot_id IN ({lot_placeholders})
                    ORDER BY id
                    ''',
                    primary_lot_ids,
                )
                for row in cursor.fetchall():
                    row = dict(row)
                    material_id = int(row.get('material_id') or 0)
                    if material_id <= 0:
                        continue
                    primary_lot_id = int(primary_lot_map.get(material_id, {}).get('material_lot_id') or 0)
                    if primary_lot_id and int(row.get('material_lot_id') or 0) != primary_lot_id:
                        continue

                    action = (row.get('action') or '').strip()
                    note = (row.get('note') or '').strip()
                    action_date = _get_print_workday(row.get('created_at'))
                    qty = float(row.get('quantity') or 0)
                    previous_stock = stock_state_map.get(material_id, 0.0)

                    delta = 0.0
                    incoming_qty = 0.0
                    outgoing_qty = 0.0

                    if action == 'issue_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                        delta = abs(qty)
                        incoming_qty = abs(qty)
                    elif action == 'consume':
                        delta = -abs(qty)
                        outgoing_qty = abs(qty)
                    elif action == 'rollback':
                        delta = abs(qty)
                        incoming_qty = abs(qty)
                    elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                        delta = -abs(qty)
                        outgoing_qty = abs(qty)
                    elif action == 'adjustment' and note in ('inventory_audit_apply_workplace_plus', 'inventory_audit_apply_workplace_minus'):
                        delta = qty
                        if qty >= 0:
                            incoming_qty = abs(qty)
                        else:
                            outgoing_qty = abs(qty)
                    elif action == 'update':
                        delta = qty - previous_stock
                    else:
                        continue

                    next_stock = previous_stock + delta

                    if action_date < selected_date:
                        opening_stock_map[material_id] = next_stock
                        closing_stock_map[material_id] = next_stock
                    elif action_date == selected_date:
                        if material_id not in opening_stock_map:
                            opening_stock_map[material_id] = previous_stock
                        received_today_map[material_id] = received_today_map.get(material_id, 0.0) + incoming_qty
                        outgoing_today_map[material_id] = outgoing_today_map.get(material_id, 0.0) + outgoing_qty
                        closing_stock_map[material_id] = next_stock

                    stock_state_map[material_id] = next_stock

        rows = []
        for item in materials:
            material_id = int(item['id'])
            opening_stock = opening_stock_map.get(material_id, 0.0)
            received_today = received_today_map.get(material_id, 0.0)
            outgoing_today = outgoing_today_map.get(material_id, float(item.get('outgoing_today') or 0))
            closing_stock = closing_stock_map.get(material_id, opening_stock)
            lot_info = primary_lot_map.get(material_id, {})
            expiry_date = (lot_info.get('expiry_date') or '').strip()
            manufacture_date = (lot_info.get('manufacture_date') or '').strip()
            expiry_or_mfg = f'(\uc18c) {expiry_date}' if expiry_date else (f'(\uc81c) {manufacture_date}' if manufacture_date else '')
            rows.append({
                'code': item.get('code') or '',
                'name': item.get('name') or '',
                'unit': item.get('unit') or '',
                'supplier_name': item.get('supplier_name') or '',
                'receiving_date': (lot_info.get('receiving_date') or '').strip(),
                'expiry_or_mfg': expiry_or_mfg,
                'opening_stock': _round_1(opening_stock),
                'received_today': _round_1(received_today),
                'outgoing_today': _round_1(outgoing_today),
                'closing_stock': _round_1(closing_stock),
                'note': '',
            })

        min_rows = max(12, len(rows))
        while len(rows) < min_rows:
            rows.append({'code': '', 'name': '', 'unit': '', 'supplier_name': '', 'receiving_date': '', 'expiry_or_mfg': '', 'opening_stock': '', 'received_today': '', 'outgoing_today': '', 'closing_stock': '', 'note': ''})

        author_name = (session.get('user', {}) or {}).get('name') or (session.get('user', {}) or {}).get('username') or ''
        weekday_labels = ['\uc6d4', '\ud654', '\uc218', '\ubaa9', '\uae08', '\ud1a0', '\uc77c']
        period_text = f'{report_date.year}\ub144 {report_date.month}\uc6d4 {report_date.day}\uc77c ({weekday_labels[report_date.weekday()]}\uc694\uc77c)'
        workplace_title = _format_print_workplace(workplace).replace('\uc2e0\uad00 2\uce35', '\uc2e0\uad00 2F').replace('\uc2e0\uad00 1\uce35', '\uc2e0\uad00 1F').replace('2\uce35', '2F').replace('1\uce35', '1F')

        return render_template('material_checksheet_preview.html', user=session['user'], report_date=selected_date, period_text=period_text, workplace_title=workplace_title, author_name=author_name, rows=rows[:max(min_rows, len(rows))])
    finally:
        conn.close()


@bp.route('/production/<int:production_id>/print')
@login_required
def production_print(production_id):
    """A4 생산 관리 일지 출력"""
    workplace = get_workplace()
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        '''
        SELECT pr.*, p.name as product_name, p.code as product_code, p.box_quantity, p.expiry_months,
               ps.line as schedule_line,
               COALESCE(
                   pr.supply_line,
                   ps.line,
                   (SELECT ps2.line FROM production_schedules ps2 WHERE ps2.production_id = pr.id ORDER BY ps2.id DESC LIMIT 1),
                   (SELECT ps3.line
                    FROM production_schedules ps3
                    WHERE ps3.product_id = pr.product_id
                      AND ps3.scheduled_date = pr.production_date
                      AND ps3.workplace = pr.workplace
                    ORDER BY ps3.id DESC LIMIT 1),
                   ''
               ) as display_supply_line
        FROM productions pr
        LEFT JOIN products p ON pr.product_id = p.id
        LEFT JOIN production_schedules ps ON pr.schedule_id = ps.id
        WHERE pr.id = ? AND pr.workplace = ?
        ''',
        (production_id, workplace),
    )
    production = cursor.fetchone()
    if not production:
        conn.close()
        return redirect(url_for('production.production_list'))

    cursor.execute(
        '''
        SELECT id
        FROM productions
        WHERE workplace = ?
          AND production_date = ?
          AND status = '완료'
        ORDER BY id ASC
        ''',
        (workplace, production['production_date']),
    )
    same_day_ids = [int(row['id']) for row in cursor.fetchall()]
    same_day_total = len(same_day_ids)
    same_day_index = same_day_ids.index(int(production['id'])) + 1 if int(production['id']) in same_day_ids else 1

    display_expiry = production['expiry_date'] or ''
    if not display_expiry:
        try:
            prod_dt = datetime.strptime(production['production_date'], '%Y-%m-%d').date()
            expiry_months = int(production['expiry_months'] or 12)
            month_index = (prod_dt.month - 1) + expiry_months
            expiry_year = prod_dt.year + (month_index // 12)
            expiry_month = (month_index % 12) + 1
            expiry_day = min(prod_dt.day, calendar.monthrange(expiry_year, expiry_month)[1])
            display_expiry = (datetime(expiry_year, expiry_month, expiry_day) - timedelta(days=1)).strftime('%Y-%m-%d')
        except Exception:
            display_expiry = ''

    # 원재료(원초) 사용 내역
    cursor.execute(
        '''
        SELECT pmu.*, rm.car_number, rm.receiving_date
        FROM production_material_usage pmu
        LEFT JOIN raw_materials rm ON pmu.raw_material_id = rm.id
        WHERE pmu.production_id = ? AND pmu.raw_material_id IS NOT NULL
        ORDER BY rm.receiving_date ASC, rm.id ASC
        ''',
        (production_id,),
    )
    raw_usages = cursor.fetchall()

    # 부자재/포장재 사용 내역 (로트 사용 이력이 있으면 로트 단위로 출력)
    cursor.execute(
        '''
        SELECT
            pmu.id as usage_id,
            pmu.production_id,
            pmu.material_id,
            pmu.expected_quantity,
            pmu.actual_quantity,
            pmu.loss_quantity,
            m.code as material_code,
            m.name as material_name,
            m.category,
            m.unit,
            pmlu.quantity as lot_used_quantity,
            ml.lot as lot_no,
            ml.receiving_date as lot_receiving_date,
            ml.manufacture_date as lot_manufacture_date,
            ml.expiry_date as lot_expiry_date,
            ml.lot_seq
        FROM production_material_usage pmu
        LEFT JOIN materials m ON pmu.material_id = m.id
        LEFT JOIN production_material_lot_usage pmlu
          ON pmlu.production_usage_id = pmu.id
        LEFT JOIN material_lots ml
          ON ml.id = pmlu.material_lot_id
        WHERE pmu.production_id = ? AND pmu.material_id IS NOT NULL
        ORDER BY m.category, m.name, COALESCE(ml.receiving_date, ''), COALESCE(ml.lot_seq, 0), pmu.id
        ''',
        (production_id,),
    )
    material_usage_rows = cursor.fetchall()

    usage_totals = defaultdict(float)
    for row in material_usage_rows:
        if row['lot_used_quantity'] is not None:
            usage_totals[row['usage_id']] += float(row['lot_used_quantity'] or 0)

    grouped_materials = defaultdict(list)
    for row in material_usage_rows:
        item = dict(row)
        total_loss = item.get('loss_quantity')
        lot_used_quantity = item.get('lot_used_quantity')
        total_lot_used = usage_totals.get(item['usage_id'], 0.0)
        if total_loss is None:
            allocated_loss = None
        elif lot_used_quantity is not None and total_lot_used > 0:
            allocated_loss = float(total_loss or 0) * (float(lot_used_quantity or 0) / total_lot_used)
        else:
            allocated_loss = float(total_loss or 0)
        item['allocated_loss_quantity'] = allocated_loss
        item['display_loss_quantity'] = _round_1(allocated_loss)
        if _exclude_from_production_print(item):
            continue
        item['base_sort_key'] = _get_production_material_sort_key(item)
        grouped_materials[_get_production_material_section(item)].append(item)

    sort_key = lambda item: (
        item.get('base_sort_key') or '',
        item.get('material_name') or '',
        item.get('lot_receiving_date') or '',
        item.get('lot_seq') or 0,
        item.get('usage_id') or 0,
    )
    material_usages = sorted(grouped_materials['base'], key=sort_key)
    packaging_usages = (
        sorted(grouped_materials['pack_inner'], key=sort_key)
        + sorted(grouped_materials['pack_outer'], key=sort_key)
        + sorted(grouped_materials['pack_box'], key=sort_key)
        + sorted(grouped_materials['pack_silica'], key=sort_key)
        + sorted(grouped_materials['pack_tray'], key=sort_key)
        + sorted(grouped_materials['pack_other'], key=sort_key)
    )

    conn.close()

    # 날짜 정보
    prod_date = production['production_date']
    try:
        dt = datetime.strptime(prod_date, '%Y-%m-%d')
        date_str = dt.strftime('%Y년 %m월 %d일')
        weekday = ['월요일','화요일','수요일','목요일','금요일','토요일','일요일'][dt.weekday()]
    except Exception:
        date_str = prod_date or ''
        weekday = ''

    return render_template(
        'production_print.html',
        user=session['user'],
        production=production,
        raw_usages=raw_usages,
        material_usages=material_usages,
        packaging_usages=packaging_usages,
        date_str=date_str,
        weekday=weekday,
        workplace=workplace,
        workplace_label=_format_print_workplace(workplace),
        display_expiry=display_expiry,
        same_day_index=same_day_index,
        same_day_total=same_day_total,
    )
