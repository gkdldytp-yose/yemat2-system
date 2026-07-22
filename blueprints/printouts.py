from flask import Blueprint, render_template, session, redirect, url_for, request, abort
from datetime import datetime, timedelta, date
from collections import defaultdict
import math
import calendar

from core import get_db, login_required, get_workplace, SHARED_WORKPLACE, today_local
from .production import (
    _get_production_material_section,
    _get_production_material_sort_key,
    _normalize_production_status,
)

bp = Blueprint('printouts', __name__)


def _exclude_from_production_print(row):
    name = (row.get('material_name') or '').strip()
    excluded_keywords = ('뚜껑', '밑판', '앵글')
    return any(keyword in name for keyword in excluded_keywords)


def _round_1(value):
    if value is None:
        return None
    return round(float(value), 1)


def _is_tray_packaging_row(row):
    category = (row.get('category') or '').strip()
    material_name = (row.get('material_name') or '').strip()
    material_code = (row.get('material_code') or '').strip().upper()
    return (
        category == '트레이'
        or '트레이' in material_name
        or material_code.startswith('T01')
        or material_code.startswith('T02')
        or material_code.startswith('T03')
        or material_code.startswith('T04')
    )


def _format_packaging_print_date(row):
    expiry_date = (row.get('lot_expiry_date') or '').strip()
    manufacture_date = (row.get('lot_manufacture_date') or '').strip()
    receiving_date = (row.get('lot_receiving_date') or '').strip()
    if _is_tray_packaging_row(row) and expiry_date:
        short_expiry = expiry_date[2:] if len(expiry_date) >= 10 else expiry_date
        return f'(\uc18c) {short_expiry}'
    if _is_tray_packaging_row(row) and manufacture_date:
        short_manufacture = manufacture_date[2:] if len(manufacture_date) >= 10 else manufacture_date
        return f'(\uc81c) {short_manufacture}'
    return receiving_date


def _format_print_workplace(workplace):
    text = (workplace or '').strip()
    mapping = {
        '1동 조미': '1동 조미',
        '1동 자반': '1동 자반',
        '2동 신관 1층': '2동 신관 1층',
        '2동 신관 2층': '2동 신관 2층',
    }
    return mapping.get(text, text)


def _format_packaging_export_note(note, workplace_prefix=''):
    note_text = (note or '').strip()
    prefix = (workplace_prefix or '').strip()
    if prefix and note_text.startswith(prefix):
        note_text = note_text[len(prefix):].strip()

    for separator in (':', '-', '/', '|'):
        if note_text.startswith(separator):
            note_text = note_text[len(separator):].strip()

    if note_text.endswith('반출 완료'):
        note_text = note_text[:-5].strip()

    return f'{note_text} 반출 완료'.strip() if note_text else '반출 완료'


def _resolve_packaging_incoming_action_date(row, action):
    action_name = (action or '').strip()
    if action_name in {'issue_request_complete', 'issue_request_update', 'issue_request_cancel'}:
        return _get_print_workday(row.get('created_at'))
    return (row.get('receiving_date') or '').strip() or _get_print_workday(row.get('created_at'))


def _build_production_expiry_rows(production_row, default_expiry_date=''):
    rows = []
    raw_dates = [
        (production_row['expiry_date'] or '').strip() or (default_expiry_date or ''),
        (production_row['expiry_date_2'] or '').strip(),
        (production_row['expiry_date_3'] or '').strip(),
    ]
    raw_boxes = [
        production_row['expiry_boxes_1'],
        production_row['expiry_boxes_2'],
        production_row['expiry_boxes_3'],
    ]
    if raw_boxes[0] in (None, ''):
        raw_boxes[0] = production_row['actual_boxes'] or ''

    for idx, (expiry_date, boxes) in enumerate(zip(raw_dates, raw_boxes), start=1):
        if idx > 1 and not expiry_date and boxes in (None, ''):
            rows.append({'expiry_date': '', 'actual_boxes': '', 'units': ''})
            continue
        box_value = float(boxes or 0) if boxes not in (None, '') else 0.0
        units = box_value * float(production_row['box_quantity'] or 0) if box_value and production_row['box_quantity'] else 0.0
        rows.append(
            {
                'expiry_date': expiry_date,
                'actual_boxes': box_value if box_value else '',
                'units': units if units else '',
            }
        )
    return rows


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


def _is_completed_status_for_printouts(value):
    text = (value or '').strip()
    broken_completed_statuses = {'?꾨즺', '�Ϸ�', '?�료'}
    return text in broken_completed_statuses or '?꾨즺' in text or '완료' in text or text.endswith('료')


_normalize_completed_status = _is_completed_status_for_printouts


def _resolve_journal_date_range():
    today = today_local()
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


def _resolve_default_calendar_month(dates):
    ordered_dates = [str(value or '').strip() for value in (dates or []) if str(value or '').strip()]
    if not ordered_dates:
        return ''
    return ordered_dates[0][:7]


def _normalize_calendar_month(value):
    text = str(value or '').strip()
    if len(text) != 7:
        return ''
    try:
        datetime.strptime(text + '-01', '%Y-%m-%d')
    except Exception:
        return ''
    return text


def _filter_rows_to_calendar_month(rows, date_field, month_token):
    if not month_token:
        return list(rows or [])
    filtered = []
    for row in rows or []:
        value = str((row or {}).get(date_field) or '').strip()
        if value.startswith(month_token):
            filtered.append(row)
    return filtered


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


def _material_checksheet_scope(code_value):
    code = (code_value or '').strip().upper()
    return 'sinan' if '_S' in code else 'yemat'


def _is_salt_material_category(category):
    text = (category or '').strip()
    return text == '소금' or '소금' in text


def _pad_print_rows(rows, target_count, blank_factory):
    padded = list(rows or [])
    while len(padded) < target_count:
        padded.append(blank_factory())
    return padded[:target_count]


def _blank_packaging_incoming_row():
    return {'name': '', 'supplier_name': '', 'quantity': '', 'receiving_date': '', 'expiry_or_mfg': '', 'note': '', 'unit': ''}


def _blank_packaging_outgoing_row():
    return {'name': '', 'target_name': '', 'quantity': '', 'receiving_date': '', 'expiry_or_mfg': '', 'note': '', 'unit': ''}


def _is_packaging_material_row(row):
    section = _get_production_material_section(
        {
            'category': row.get('category'),
            'material_name': row.get('material_name') or row.get('name') or '',
        }
    )
    return bool(section and section.startswith('pack'))


def _get_packaging_bom_material_map(cursor, workplace):
    cursor.execute(
        '''
        SELECT DISTINCT
            m.id,
            COALESCE(m.code, '') as code,
            COALESCE(m.name, '') as name,
            COALESCE(m.category, '') as category,
            COALESCE(m.unit, '') as unit,
            COALESCE(s.name, '') as supplier_name
        FROM bom b
        JOIN products p ON p.id = b.product_id
        JOIN materials m ON m.id = b.material_id
        LEFT JOIN suppliers s ON s.id = m.supplier_id
        WHERE p.workplace = ?
          AND b.material_id IS NOT NULL
        ORDER BY m.name
        ''',
        (workplace,),
    )
    result = {}
    for row in cursor.fetchall():
        item = dict(row)
        material_id = int(item.get('id') or 0)
        if material_id <= 0:
            continue
        if not _is_packaging_material_row(item):
            continue
        result[material_id] = item
    return result


def _get_packaging_material_map(cursor, workplace):
    result = _get_packaging_bom_material_map(cursor, workplace)

    cursor.execute(
        '''
        SELECT DISTINCT
            m.id,
            COALESCE(m.code, '') as code,
            COALESCE(m.name, '') as name,
            COALESCE(m.category, '') as category,
            COALESCE(m.unit, '') as unit,
            COALESCE(s.name, '') as supplier_name
        FROM production_material_usage pmu
        JOIN productions p ON p.id = pmu.production_id
        JOIN materials m ON m.id = pmu.material_id
        LEFT JOIN suppliers s ON s.id = m.supplier_id
        WHERE p.workplace = ?
          AND pmu.material_id IS NOT NULL
        ORDER BY m.name
        ''',
        (workplace,),
    )
    for row in cursor.fetchall():
        item = dict(row)
        material_id = int(item.get('id') or 0)
        if material_id <= 0:
            continue
        if not _is_packaging_material_row(item):
            continue
        result[material_id] = item

    return result


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
    raw_scope = (request.args.get('raw_scope') or '').strip().lower()
    raw_query = (request.args.get('raw_q') or '').strip()
    raw_filter = (request.args.get('raw_filter') or 'all').strip()
    material_scope = (request.args.get('material_scope') or 'yemat').strip().lower()
    selected_production_date = (request.args.get('production_date') or '').strip()
    selected_raw_date = (request.args.get('raw_date') or '').strip()
    selected_material_date = (request.args.get('material_date') or '').strip()
    selected_packaging_date = (request.args.get('packaging_date') or '').strip()
    selected_production_month = _normalize_calendar_month(request.args.get('production_month'))
    selected_raw_month = _normalize_calendar_month(request.args.get('raw_month'))
    selected_material_month = _normalize_calendar_month(request.args.get('material_month'))
    selected_packaging_month = _normalize_calendar_month(request.args.get('packaging_month'))
    if raw_status not in ('active', 'done'):
        raw_status = 'active'
    if raw_scope not in ('all', 'month'):
        raw_scope = ''
    if raw_filter not in ('all', 'code', 'car_number', 'done_date', 'receiving_date'):
        raw_filter = 'all'
    if material_scope not in ('yemat', 'sinan'):
        material_scope = 'yemat'
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
            ORDER BY pr.production_date DESC, pr.id DESC
            LIMIT 1200
            ''',
            (workplace,),
        )
        production_rows = []
        for row in cursor.fetchall():
            item = dict(row)
            if _normalize_completed_status(item.get('status')):
                production_rows.append(item)
        production_available_dates = []
        seen_production_dates = set()
        for row in production_rows:
            production_date = (row.get('production_date') or '').strip()
            if production_date and production_date not in seen_production_dates:
                seen_production_dates.add(production_date)
                production_available_dates.append(production_date)
        if selected_production_date and selected_production_date not in seen_production_dates:
            selected_production_date = ''
        if selected_production_date:
            production_rows = [
                row for row in production_rows
                if (row.get('production_date') or '').strip() == selected_production_date
            ]
        else:
            selected_production_month = selected_production_month or _resolve_default_calendar_month(production_available_dates)
            production_rows = _filter_rows_to_calendar_month(
                production_rows,
                'production_date',
                selected_production_month,
            )

        cursor.execute(
            '''
            SELECT
                id,
                COALESCE(code, '') as code,
                COALESCE(name, '') as name,
                COALESCE(receiving_date, '') as receiving_date,
                COALESCE(NULLIF(TRIM(ja_ho), ''), NULLIF(TRIM(car_number), ''), '') as car_number,
                COALESCE(current_stock, 0) as current_stock,
                COALESCE(used_quantity, 0) as used_quantity
            FROM raw_materials
            WHERE workplace = ?
            ORDER BY COALESCE(current_stock, 0) DESC, COALESCE(receiving_date, '') DESC, name
            ''',
            (workplace,),
        )
        raw_all_items = [dict(row) for row in cursor.fetchall()]
        cursor.execute(
            '''
            SELECT DISTINCT raw_material_id
            FROM raw_material_logs
            WHERE raw_material_id IS NOT NULL
              AND COALESCE(type, '') = 'export'
              AND COALESCE(quantity, 0) < 0
            '''
        )
        exported_raw_ids = {int(row['raw_material_id'] or 0) for row in cursor.fetchall() if int(row['raw_material_id'] or 0) > 0}
        cursor.execute(
            '''
            SELECT
                raw_material_id,
                MAX(SUBSTR(COALESCE(created_at, ''), 1, 10)) as last_log_date
            FROM raw_material_logs
            WHERE raw_material_id IS NOT NULL
              AND COALESCE(created_at, '') != ''
            GROUP BY raw_material_id
            '''
        )
        raw_last_log_dates = {
            int(row['raw_material_id'] or 0): (row['last_log_date'] or '').strip()
            for row in cursor.fetchall()
            if int(row['raw_material_id'] or 0) > 0
        }
        cursor.execute(
            '''
            SELECT
                COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), rm.receiving_date, '') as receiving_date,
                COALESCE(NULLIF(TRIM(pmu.override_car_number), ''), NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), ''), '') as car_number,
                MAX(substr(p.production_date, 1, 10)) as last_production_date,
                SUM(COALESCE(pmu.actual_quantity, 0)) as used_quantity
            FROM production_material_usage pmu
            JOIN productions p
              ON p.id = pmu.production_id
             AND COALESCE(p.production_date, '') != ''
            LEFT JOIN raw_materials rm ON rm.id = pmu.raw_material_id
            WHERE COALESCE(p.workplace, '') = ?
              AND COALESCE(p.entry_mode, '') = 'register'
              AND COALESCE(p.status, '') = '완료'
              AND pmu.raw_material_id IS NOT NULL
              AND COALESCE(pmu.actual_quantity, 0) > 0
            GROUP BY
                COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), rm.receiving_date, ''),
                COALESCE(NULLIF(TRIM(pmu.override_car_number), ''), NULLIF(TRIM(rm.ja_ho), ''), NULLIF(TRIM(rm.car_number), ''), '')
            ''',
            (workplace,),
        )
        raw_register_lot_dates = {}
        for row in cursor.fetchall():
            lot_key = ((row['receiving_date'] or '').strip(), (row['car_number'] or '').strip())
            if not lot_key[0] or not lot_key[1]:
                continue
            if float(row['used_quantity'] or 0) <= 0:
                continue
            raw_register_lot_dates[lot_key] = (row['last_production_date'] or '').strip()

        def _raw_lot_key(row):
            return ((row.get('receiving_date') or '').strip(), (row.get('car_number') or '').strip())

        raw_active_items = [row for row in raw_all_items if float(row.get('current_stock') or 0) > 0]
        raw_done_items = [
            row for row in raw_all_items
            if float(row.get('current_stock') or 0) <= 0
            and (
                float(row.get('used_quantity') or 0) > 0
                or int(row.get('id') or 0) in exported_raw_ids
                or _raw_lot_key(row) in raw_register_lot_dates
            )
        ]
        for row in raw_active_items:
            row['journal_date'] = (row.get('receiving_date') or '').strip()
        for row in raw_done_items:
            row['journal_date'] = (
                raw_register_lot_dates.get(_raw_lot_key(row))
                or raw_last_log_dates.get(int(row.get('id') or 0), '')
            )
        raw_active_available_dates = sorted(
            {row['journal_date'] for row in raw_active_items if row.get('journal_date')},
            reverse=True,
        )
        raw_done_available_dates = sorted(
            {row['journal_date'] for row in raw_done_items if row.get('journal_date')},
            reverse=True,
        )
        raw_available_dates = raw_active_available_dates if raw_status == 'active' else raw_done_available_dates
        if selected_raw_date and selected_raw_date not in raw_available_dates:
            selected_raw_date = ''
        current_raw_month = datetime.now().strftime('%Y-%m')
        if raw_status == 'active':
            raw_scope = 'all'
            selected_raw_date = ''
            selected_raw_month = ''
        else:
            if selected_raw_date and len(selected_raw_date) >= 7:
                selected_raw_month = selected_raw_date[:7]
                raw_scope = 'month'
            elif raw_scope == '':
                raw_scope = 'month'
            if raw_scope == 'month':
                selected_raw_month = selected_raw_month or _resolve_default_calendar_month(raw_done_available_dates) or current_raw_month
            else:
                selected_raw_month = ''

            if selected_raw_date:
                raw_done_items = [
                    row for row in raw_done_items
                    if (row.get('journal_date') or '') == selected_raw_date
                ]
            elif raw_scope == 'month':
                raw_done_items = _filter_rows_to_calendar_month(raw_done_items, 'journal_date', selected_raw_month)
        if raw_query:
            raw_query_lower = raw_query.lower()

            def _matches_raw_query(row):
                field_map = {
                    'code': ('code',),
                    'car_number': ('car_number',),
                    'done_date': ('journal_date',),
                    'receiving_date': ('receiving_date',),
                    'all': ('code', 'name', 'car_number', 'receiving_date', 'journal_date'),
                }
                return any(
                    raw_query_lower in str(row.get(field) or '').lower()
                    for field in field_map.get(raw_filter, field_map['all'])
                )

            if raw_status == 'active':
                raw_active_items = [row for row in raw_active_items if _matches_raw_query(row)]
            else:
                raw_done_items = [row for row in raw_done_items if _matches_raw_query(row)]

        production_date_set = list(production_available_dates)

        stocked_base_material_ids = set()
        location_ids = _get_print_inventory_location_ids(cursor, workplace)
        if location_ids:
            loc_placeholders = ','.join(['?'] * len(location_ids))
            cursor.execute(
                f'''
                SELECT DISTINCT m.id, COALESCE(m.category, '') as category
                FROM materials m
                JOIN material_lots ml
                  ON ml.material_id = m.id
                 AND COALESCE(ml.is_disposed, 0) = 0
                JOIN inv_material_lot_balances b
                  ON b.material_lot_id = ml.id
                 AND COALESCE(b.qty, 0) > 0
                WHERE b.location_id IN ({loc_placeholders})
                ''',
                location_ids,
            )
            for row in cursor.fetchall():
                row = dict(row)
                material_id = int(row['id'] or 0)
                if material_id > 0 and _base_material_category(row.get('category')):
                    stocked_base_material_ids.add(material_id)

        cursor.execute(
            '''
            SELECT
                p.production_date,
                p.status,
                COALESCE(m.code, '') as code,
                COALESCE(m.category, '') as category,
                pmu.material_id,
                COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty
            FROM production_material_usage pmu
            JOIN productions p ON p.id = pmu.production_id
            LEFT JOIN materials m ON m.id = pmu.material_id
            LEFT JOIN production_material_lot_usage pmlu
              ON pmlu.production_usage_id = pmu.id
            WHERE p.workplace = ?
              AND pmu.material_id IS NOT NULL
            ORDER BY p.production_date DESC, pmu.material_id
            ''',
            (workplace,),
        )
        material_map = {'yemat': {}, 'sinan': {}}
        for row in cursor.fetchall():
            row = dict(row)
            if not _normalize_completed_status(row.get('status')):
                continue
            if not _base_material_category(row.get('category')):
                continue
            key = (row.get('production_date') or '').strip()
            material_id = int(row.get('material_id') or 0)
            if not key or material_id <= 0:
                continue
            scope = _material_checksheet_scope(row.get('code'))
            scope_map = material_map.setdefault(scope, {})
            bucket = scope_map.setdefault(key, {'production_date': key, 'item_ids': set(), 'outgoing_total': 0.0})
            bucket['item_ids'].add(material_id)
            bucket['outgoing_total'] += float(row.get('qty') or 0)

        workplace_prefix = (workplace or '').strip()
        cursor.execute(
            '''
            SELECT
                mll.material_id,
                COALESCE(m.code, '') as code,
                COALESCE(m.category, '') as category,
                COALESCE(mll.action, '') as action,
                COALESCE(mll.quantity, 0) as quantity,
                COALESCE(mll.note, '') as note,
                COALESCE(mll.created_at, '') as created_at,
                COALESCE(ml.receiving_date, '') as receiving_date
            FROM material_lot_logs mll
            JOIN materials m ON m.id = mll.material_id
            LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
            WHERE COALESCE(m.workplace, '') = ?
            ORDER BY mll.id
            ''',
            (workplace,),
        )
        for row in cursor.fetchall():
            row = dict(row)
            if not _base_material_category(row.get('category')):
                continue
            material_id = int(row.get('material_id') or 0)
            if material_id <= 0:
                continue
            action = (row.get('action') or '').strip()
            note = (row.get('note') or '').strip()
            qty = float(row.get('quantity') or 0)
            action_date = ''
            outgoing_qty = 0.0
            if action == 'create':
                action_date = (row.get('receiving_date') or '').strip() or _get_print_workday(row.get('created_at'))
            elif action == 'issue_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                action_date = (row.get('receiving_date') or '').strip() or _get_print_workday(row.get('created_at'))
            elif action == 'issue_request_update' and workplace_prefix and note.startswith(workplace_prefix):
                action_date = (row.get('receiving_date') or '').strip() or _get_print_workday(row.get('created_at'))
            elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                action_date = _get_print_workday(row.get('created_at'))
                outgoing_qty = abs(qty)
            elif action == 'adjustment' and note in ('inventory_audit_apply_workplace_plus', 'inventory_audit_apply_workplace_minus'):
                action_date = _get_print_workday(row.get('created_at'))
                if qty < 0:
                    outgoing_qty = abs(qty)
            else:
                continue
            if not action_date:
                continue
            scope = _material_checksheet_scope(row.get('code'))
            scope_map = material_map.setdefault(scope, {})
            bucket = scope_map.setdefault(action_date, {'production_date': action_date, 'item_ids': set(), 'outgoing_total': 0.0})
            bucket['item_ids'].add(material_id)
            bucket['outgoing_total'] += outgoing_qty

        stocked_scope_material_ids = {'yemat': set(), 'sinan': set()}
        if stocked_base_material_ids:
            cursor.execute(
                f'''
                SELECT id, COALESCE(code, '') as code
                FROM materials
                WHERE id IN ({','.join(['?'] * len(stocked_base_material_ids))})
                ''',
                list(stocked_base_material_ids),
            )
            for row in cursor.fetchall():
                scope = _material_checksheet_scope(row['code'])
                stocked_scope_material_ids.setdefault(scope, set()).add(int(row['id'] or 0))

        material_journal_dates_map = {'yemat': [], 'sinan': []}
        for scope in ('yemat', 'sinan'):
            scope_map = material_map.get(scope, {})
            default_visible_ids = stocked_scope_material_ids.get(scope, set())
            scope_dates = sorted(set(production_date_set) | set(scope_map.keys()), reverse=True)
            for production_date in scope_dates:
                bucket = scope_map.get(production_date, {'item_ids': set(), 'outgoing_total': 0.0})
                visible_item_ids = bucket['item_ids'] or default_visible_ids
                if not visible_item_ids:
                    continue
                material_journal_dates_map[scope].append(
                    {
                        'production_date': production_date,
                        'item_count': len(visible_item_ids),
                        'outgoing_total': round(float(bucket.get('outgoing_total') or 0.0), 1),
                        'scope': scope,
                    }
                )
        material_available_dates_map = {
            'yemat': [row['production_date'] for row in material_journal_dates_map['yemat']],
            'sinan': [row['production_date'] for row in material_journal_dates_map['sinan']],
        }
        if selected_material_date and selected_material_date not in material_available_dates_map.get(material_scope, []):
            selected_material_date = ''
        if selected_material_date:
            material_journal_dates_map['yemat'] = [
                row for row in material_journal_dates_map['yemat']
                if row.get('production_date') == selected_material_date
            ]
            material_journal_dates_map['sinan'] = [
                row for row in material_journal_dates_map['sinan']
                if row.get('production_date') == selected_material_date
            ]
        else:
            selected_material_month = selected_material_month or _resolve_default_calendar_month(material_available_dates_map.get(material_scope, []))
            material_journal_dates_map['yemat'] = _filter_rows_to_calendar_month(
                material_journal_dates_map['yemat'],
                'production_date',
                selected_material_month,
            )
            material_journal_dates_map['sinan'] = _filter_rows_to_calendar_month(
                material_journal_dates_map['sinan'],
                'production_date',
                selected_material_month,
            )

        packaging_material_map = _get_packaging_material_map(cursor, workplace)
        packaging_material_ids = list(packaging_material_map.keys())
        packaging_journal_rows = []
        packaging_available_dates = []
        if packaging_material_ids:
            packaging_date_map = {}
            placeholders = ','.join(['?'] * len(packaging_material_ids))
            workplace_prefix = (workplace or '').strip()

            cursor.execute(
                f'''
                SELECT
                    p.production_date,
                    p.status,
                    pmu.material_id,
                    COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty
                FROM production_material_usage pmu
                JOIN productions p ON p.id = pmu.production_id
                LEFT JOIN production_material_lot_usage pmlu
                  ON pmlu.production_usage_id = pmu.id
                WHERE p.workplace = ?
                  AND pmu.material_id IN ({placeholders})
                  AND pmu.material_id IS NOT NULL
                ORDER BY p.production_date DESC, pmu.material_id
                ''',
                [workplace, *packaging_material_ids],
            )
            for row in cursor.fetchall():
                row = dict(row)
                if not _normalize_completed_status(row.get('status')):
                    continue
                action_date = (row.get('production_date') or '').strip()
                material_id = int(row.get('material_id') or 0)
                if not action_date or material_id <= 0:
                    continue
                bucket = packaging_date_map.setdefault(
                    action_date,
                    {'production_date': action_date, 'item_ids': set(), 'incoming_total': 0.0, 'outgoing_total': 0.0},
                )
                bucket['item_ids'].add(material_id)
                bucket['outgoing_total'] += float(row.get('qty') or 0)

            cursor.execute(
                f'''
                SELECT
                    mll.material_id,
                    mll.material_lot_id,
                    COALESCE(mll.action, '') as action,
                    COALESCE(mll.quantity, 0) as quantity,
                    COALESCE(mll.note, '') as note,
                    COALESCE(mll.created_at, '') as created_at,
                    COALESCE(ml.receiving_date, '') as receiving_date,
                    COALESCE(ml.is_disposed, 0) as is_disposed
                FROM material_lot_logs mll
                LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
                WHERE mll.material_id IN ({placeholders})
                ORDER BY mll.id
                ''',
                packaging_material_ids,
            )
            for row in cursor.fetchall():
                row = dict(row)
                material_id = int(row.get('material_id') or 0)
                if material_id <= 0:
                    continue
                action = (row.get('action') or '').strip()
                note = (row.get('note') or '').strip()
                qty = float(row.get('quantity') or 0)
                incoming_qty = 0.0
                outgoing_qty = 0.0
                action_date = ''
                if action == 'create':
                    incoming_qty = abs(qty)
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'issue_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    incoming_qty = abs(qty)
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'issue_request_update' and workplace_prefix and note.startswith(workplace_prefix):
                    incoming_qty = abs(qty)
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action in ('issue_request_cancel', 'delete'):
                    incoming_qty = -abs(qty)
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    outgoing_qty = abs(qty)
                    action_date = _get_print_workday(row.get('created_at'))
                else:
                    continue
                if not action_date:
                    continue
                bucket = packaging_date_map.setdefault(
                    action_date,
                    {'production_date': action_date, 'item_ids': set(), 'incoming_total': 0.0, 'outgoing_total': 0.0},
                )
                bucket['item_ids'].add(material_id)
                bucket['incoming_total'] += incoming_qty
                bucket['outgoing_total'] += outgoing_qty

            packaging_journal_rows = sorted(packaging_date_map.values(), key=lambda item: item.get('production_date') or '', reverse=True)
            for row in packaging_journal_rows:
                row['item_count'] = len(row.get('item_ids') or set())
                row['incoming_total'] = round(float(row.get('incoming_total') or 0.0), 1)
                row['outgoing_total'] = round(float(row.get('outgoing_total') or 0.0), 1)
            packaging_journal_rows = [
                row for row in packaging_journal_rows
                if float(row.get('incoming_total') or 0.0) > 0 or float(row.get('outgoing_total') or 0.0) > 0
            ]
            packaging_available_dates = [row['production_date'] for row in packaging_journal_rows if row.get('production_date')]
            if selected_packaging_date and selected_packaging_date not in packaging_available_dates:
                selected_packaging_date = ''
            if selected_packaging_date:
                packaging_journal_rows = [
                    row for row in packaging_journal_rows
                    if row.get('production_date') == selected_packaging_date
                ]
            else:
                selected_packaging_month = selected_packaging_month or _resolve_default_calendar_month(packaging_available_dates)
                packaging_journal_rows = _filter_rows_to_calendar_month(
                    packaging_journal_rows,
                    'production_date',
                    selected_packaging_month,
                )

        return render_template(
            'journals.html',
            user=session['user'],
            selected_tab=selected_tab,
            raw_status=raw_status,
            date_from=date_from,
            date_to=date_to,
            current_workplace=workplace,
            production_rows=production_rows,
            production_available_dates=production_available_dates,
            selected_production_date=selected_production_date,
            selected_production_month=selected_production_month,
            raw_active_items=raw_active_items,
            raw_done_items=raw_done_items,
            raw_active_available_dates=raw_active_available_dates,
            raw_done_available_dates=raw_done_available_dates,
            selected_raw_date=selected_raw_date,
            selected_raw_month=selected_raw_month,
            raw_scope=raw_scope,
            raw_query=raw_query,
            raw_filter=raw_filter,
            material_scope=material_scope,
            material_journal_dates_yemat=material_journal_dates_map['yemat'],
            material_journal_dates_sinan=material_journal_dates_map['sinan'],
            material_available_dates_yemat=material_available_dates_map['yemat'],
            material_available_dates_sinan=material_available_dates_map['sinan'],
            selected_material_date=selected_material_date,
            selected_material_month=selected_material_month,
            packaging_journal_rows=packaging_journal_rows,
            packaging_available_dates=packaging_available_dates,
            selected_packaging_date=selected_packaging_date,
            selected_packaging_month=selected_packaging_month,
        )
    finally:
        conn.close()


@bp.route('/materials/checksheet-preview')
@login_required
def material_checksheet_preview():
    workplace = get_workplace()
    selected_date = (request.args.get('date') or today_local().isoformat()).strip()
    material_scope = (request.args.get('scope') or 'yemat').strip().lower()
    selected_production_id = int(request.args.get('production_id') or 0) if str(request.args.get('production_id') or '').strip().isdigit() else 0
    if material_scope not in ('yemat', 'sinan'):
        material_scope = 'yemat'
    try:
        report_date = datetime.strptime(selected_date, '%Y-%m-%d').date()
    except Exception:
        report_date = today_local()
        selected_date = report_date.isoformat()

    conn = get_db()
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            SELECT
                p.id as production_id,
                p.status,
                COALESCE(p.entry_mode, '') as entry_mode,
                pmu.id as production_usage_id,
                pmu.material_id as id,
                COALESCE(m.code, '') as code,
                COALESCE(m.name, '') as name,
                COALESCE(m.category, '') as category,
                COALESCE(m.unit, '') as unit,
                COALESCE(s.name, '') as supplier_name,
                COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), '') as override_receiving_date,
                COALESCE(NULLIF(TRIM(pmu.override_manufacture_date), ''), '') as override_manufacture_date,
                COALESCE(NULLIF(TRIM(pmu.override_expiry_date), ''), '') as override_expiry_date,
                COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty
            FROM production_material_usage pmu
            JOIN productions p ON p.id = pmu.production_id
            LEFT JOIN materials m ON m.id = pmu.material_id
            LEFT JOIN suppliers s ON s.id = m.supplier_id
            LEFT JOIN production_material_lot_usage pmlu
              ON pmlu.production_usage_id = pmu.id
            WHERE p.workplace = ?
              AND COALESCE(p.production_date, '') = ?
              AND (? <= 0 OR p.id = ?)
              AND pmu.material_id IS NOT NULL
            ORDER BY pmu.material_id, pmu.id
            ''',
            (workplace, selected_date, selected_production_id, selected_production_id),
        )
        material_rows = [dict(row) for row in cursor.fetchall()]

        register_mode_rows = [
            row for row in material_rows
            if str(row.get('entry_mode') or '').strip().lower() == 'register'
        ]
        direct_usage_mode = bool(register_mode_rows)
        if direct_usage_mode:
            rows = []
            for row in material_rows:
                if not _normalize_completed_status(row.get('status')):
                    continue
                if not _base_material_category(row.get('category')):
                    continue
                if _material_checksheet_scope(row.get('code')) != material_scope:
                    continue
                qty = float(row.get('qty') or 0.0)
                if qty <= 0:
                    continue
                receiving_date = (row.get('override_receiving_date') or '').strip()
                manufacture_date = (row.get('override_manufacture_date') or '').strip()
                expiry_date = (row.get('override_expiry_date') or '').strip()
                expiry_or_mfg = f'(\uc18c) {expiry_date}' if expiry_date else (f'(\uc81c) {manufacture_date}' if manufacture_date else '')
                rows.append(
                    {
                        'code': row.get('code') or '',
                        'name': row.get('name') or '',
                        'category': row.get('category') or '',
                        'unit': row.get('unit') or '',
                        'supplier_name': row.get('supplier_name') or '',
                        'receiving_date': receiving_date,
                        'expiry_or_mfg': expiry_or_mfg,
                        'opening_stock': _round_1(qty),
                        'received_today': _round_1(0),
                        'outgoing_today': _round_1(qty),
                        'closing_stock': _round_1(0),
                        'note': '\ub4f1\ub85d\ubaa8\ub4dc' if str(row.get('entry_mode') or '').strip().lower() == 'register' else '',
                    }
                )

            oil_rows = [row for row in rows if not _is_salt_material_category(row.get('category'))]
            salt_rows = [row for row in rows if _is_salt_material_category(row.get('category'))]
            salt_rows.sort(
                key=lambda row: (
                    1 if not (row.get('receiving_date') or '').strip() else 0,
                    (row.get('receiving_date') or '').strip(),
                    row.get('name') or '',
                )
            )
            min_rows = max(12, len(rows))
            middle_blank_count = max(0, min_rows - len(oil_rows) - len(salt_rows))
            blank_row = {'code': '', 'name': '', 'category': '', 'unit': '', 'supplier_name': '', 'receiving_date': '', 'expiry_or_mfg': '', 'opening_stock': '', 'received_today': '', 'outgoing_today': '', 'closing_stock': '', 'note': ''}
            rows = oil_rows + [dict(blank_row) for _ in range(middle_blank_count)] + salt_rows
            while len(rows) < min_rows:
                rows.append(dict(blank_row))

            author_name = (session.get('user', {}) or {}).get('name') or (session.get('user', {}) or {}).get('username') or ''
            weekday_labels = ['\uc6d4', '\ud654', '\uc218', '\ubaa9', '\uae08', '\ud1a0', '\uc77c']
            period_text = f'{report_date.year}\ub144 {report_date.month}\uc6d4 {report_date.day}\uc77c ({weekday_labels[report_date.weekday()]}\uc694\uc77c)'
            workplace_title = _format_print_workplace(workplace).replace('\uc2e0\uad00 2\uce35', '\uc2e0\uad00_2F').replace('\uc2e0\uad00 1\uce35', '\uc2e0\uad00_1F').replace('2\uce35', '2F').replace('1\uce35', '1F')
            if workplace_title:
                workplace_title = f'{workplace_title} \uc870\ubbf8\uae40 \uc791\uc5c5\uc7a5'
            scope_label = '\uc2e0\uc548' if material_scope == 'sinan' else '\uc608\ub9db'

            return render_template(
                'material_checksheet_preview.html',
                user=session['user'],
                report_date=selected_date,
                period_text=period_text,
                workplace_title=workplace_title,
                author_name=author_name,
                rows=rows[:max(min_rows, len(rows))],
                material_scope=material_scope,
                scope_label=scope_label,
            )

        completed_production_ids = set()
        material_map = {}
        for row in material_rows:
            if not _normalize_completed_status(row.get('status')):
                continue
            if not _base_material_category(row.get('category')):
                continue
            if _material_checksheet_scope(row.get('code')) != material_scope:
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

        cursor.execute(
            '''
            SELECT
                m.id,
                COALESCE(m.code, '') as code,
                COALESCE(m.name, '') as name,
                COALESCE(m.category, '') as category,
                COALESCE(m.unit, '') as unit,
                COALESCE(s.name, '') as supplier_name
            FROM materials m
            LEFT JOIN suppliers s ON s.id = m.supplier_id
            WHERE COALESCE(m.workplace, '') = ?
            ORDER BY m.id
            ''',
            (workplace,),
        )
        material_meta_map = {}
        for row in cursor.fetchall():
            row = dict(row)
            if not _base_material_category(row.get('category')):
                continue
            if _material_checksheet_scope(row.get('code')) != material_scope:
                continue
            material_id = int(row.get('id') or 0)
            if material_id <= 0:
                continue
            material_meta_map[material_id] = {
                'id': material_id,
                'code': row.get('code') or '',
                'name': row.get('name') or '',
                'category': row.get('category') or '',
                'unit': row.get('unit') or '',
                'supplier_name': row.get('supplier_name') or '',
                'outgoing_today': 0.0,
            }

        if material_meta_map:
            placeholders = ','.join(['?'] * len(material_meta_map))
            workplace_prefix = (workplace or '').strip()
            cursor.execute(
                f'''
                SELECT
                    mll.material_id,
                    COALESCE(mll.action, '') as action,
                    COALESCE(mll.quantity, 0) as quantity,
                    COALESCE(mll.note, '') as note,
                    COALESCE(mll.created_at, '') as created_at,
                    COALESCE(ml.receiving_date, '') as receiving_date
                FROM material_lot_logs mll
                LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
                WHERE mll.material_id IN ({placeholders})
                ORDER BY mll.id
                ''',
                list(material_meta_map.keys()),
            )
            for row in cursor.fetchall():
                row = dict(row)
                material_id = int(row.get('material_id') or 0)
                if material_id <= 0 or material_id not in material_meta_map:
                    continue
                action = (row.get('action') or '').strip()
                note = (row.get('note') or '').strip()
                action_date = ''
                if action == 'create':
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'issue_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'issue_request_update' and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _get_print_workday(row.get('created_at'))
                elif action == 'adjustment' and note in ('inventory_audit_apply_workplace_plus', 'inventory_audit_apply_workplace_minus'):
                    action_date = _get_print_workday(row.get('created_at'))
                if action_date == selected_date:
                    material_map.setdefault(material_id, dict(material_meta_map[material_id]))

        location_ids = _get_print_inventory_location_ids(cursor, workplace)
        if location_ids:
            loc_placeholders = ','.join(['?'] * len(location_ids))
            cursor.execute(
                f'''
                SELECT
                    m.id,
                    COALESCE(m.code, '') as code,
                    COALESCE(m.name, '') as name,
                    COALESCE(m.category, '') as category,
                    COALESCE(m.unit, '') as unit,
                    COALESCE(s.name, '') as supplier_name,
                    COALESCE(SUM(b.qty), 0) as workplace_stock
                FROM materials m
                LEFT JOIN suppliers s ON s.id = m.supplier_id
                JOIN material_lots ml
                  ON ml.material_id = m.id
                 AND COALESCE(ml.is_disposed, 0) = 0
                JOIN inv_material_lot_balances b
                  ON b.material_lot_id = ml.id
                 AND COALESCE(b.qty, 0) > 0
                WHERE b.location_id IN ({loc_placeholders})
                GROUP BY m.id, m.code, m.name, m.category, m.unit, s.name
                ORDER BY m.name
                ''',
                location_ids,
            )
            for row in cursor.fetchall():
                row = dict(row)
                if not _base_material_category(row.get('category')):
                    continue
                if _material_checksheet_scope(row.get('code')) != material_scope:
                    continue
                material_id = int(row.get('id') or 0)
                if material_id <= 0:
                    continue
                material_map.setdefault(
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

        materials = list(material_map.values())
        for row in materials:
            row['base_sort_key'] = _get_production_material_sort_key({'category': row.get('category'), 'material_name': row.get('name')})
        materials.sort(key=lambda item: (item.get('base_sort_key') or '', item.get('name') or ''))

        material_ids = [int(row['id']) for row in materials if int(row.get('id') or 0) > 0]
        primary_lot_map = {}
        received_today_map = defaultdict(float)
        non_production_outgoing_today_map = defaultdict(float)
        future_net_delta_map = defaultdict(float)
        future_production_outgoing_map = defaultdict(float)
        workplace_stock_map = {}
        if material_ids:
            placeholders = ','.join(['?'] * len(material_ids))
            workplace_prefix = (workplace or '').strip()

            if location_ids:
                loc_placeholders = ','.join(['?'] * len(location_ids))
                cursor.execute(
                    f'''
                    SELECT
                        ml.material_id,
                        COALESCE(SUM(b.qty), 0) as workplace_stock
                    FROM material_lots ml
                    JOIN inv_material_lot_balances b
                      ON b.material_lot_id = ml.id
                     AND COALESCE(b.qty, 0) > 0
                    WHERE ml.material_id IN ({placeholders})
                      AND COALESCE(ml.is_disposed, 0) = 0
                      AND b.location_id IN ({loc_placeholders})
                    GROUP BY ml.material_id
                    ''',
                    [*material_ids, *location_ids],
                )
                workplace_stock_map = {
                    int(row['material_id']): float(row['workplace_stock'] or 0)
                    for row in cursor.fetchall()
                    if int(row['material_id'] or 0) > 0
                }

                cursor.execute(
                    f'''
                    SELECT
                        ml.material_id,
                        ml.id as material_lot_id,
                        ml.receiving_date,
                        ml.manufacture_date,
                        ml.expiry_date,
                        COALESCE(SUM(b.qty), 0) as workplace_stock
                    FROM material_lots ml
                    JOIN inv_material_lot_balances b
                      ON b.material_lot_id = ml.id
                     AND COALESCE(b.qty, 0) > 0
                    WHERE ml.material_id IN ({placeholders})
                      AND COALESCE(ml.is_disposed, 0) = 0
                      AND b.location_id IN ({loc_placeholders})
                    GROUP BY ml.material_id, ml.id, ml.receiving_date, ml.manufacture_date, ml.expiry_date
                    ORDER BY ml.material_id, ml.receiving_date ASC, ml.id ASC
                    ''',
                    [*material_ids, *location_ids],
                )
                for row in cursor.fetchall():
                    material_id = int(row['material_id'] or 0)
                    if material_id > 0 and material_id not in primary_lot_map:
                        primary_lot_map[material_id] = dict(row)

            cursor.execute(
                f'''
                SELECT
                    ml.material_id,
                    ml.id as material_lot_id,
                    ml.receiving_date,
                    ml.manufacture_date,
                    ml.expiry_date
                FROM material_lots ml
                WHERE ml.material_id IN ({placeholders})
                  AND COALESCE(ml.is_disposed, 0) = 0
                ORDER BY ml.material_id, ml.receiving_date ASC, ml.id ASC
                ''',
                material_ids,
            )
            for row in cursor.fetchall():
                material_id = int(row['material_id'] or 0)
                if material_id > 0 and material_id not in primary_lot_map:
                    primary_lot_map[material_id] = dict(row)

            cursor.execute(
                f'''
                SELECT material_id, action, quantity, note, created_at
                FROM material_lot_logs
                WHERE material_id IN ({placeholders})
                ORDER BY id
                ''',
                material_ids,
            )
            for row in cursor.fetchall():
                row = dict(row)
                material_id = int(row.get('material_id') or 0)
                if material_id <= 0:
                    continue

                action = (row.get('action') or '').strip()
                note = (row.get('note') or '').strip()
                action_date = _get_print_workday(row.get('created_at'))
                qty = float(row.get('quantity') or 0)
                if not action_date:
                    continue

                delta = 0.0
                incoming_qty = 0.0
                outgoing_qty = 0.0

                if action == 'issue_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    delta = abs(qty)
                    incoming_qty = abs(qty)
                elif action == 'issue_request_cancel' and workplace_prefix and note.startswith(workplace_prefix):
                    delta = -abs(qty)
                    incoming_qty = -abs(qty)
                elif action == 'rollback':
                    # Production rollback restores stock, but it is not an actual receipt.
                    # Keep the stock delta for historical balance reconstruction only.
                    delta = abs(qty)
                elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    delta = -abs(qty)
                    outgoing_qty = abs(qty)
                elif action == 'adjustment' and note in ('inventory_audit_apply_workplace_plus', 'inventory_audit_apply_workplace_minus'):
                    delta = qty
                    if qty >= 0:
                        incoming_qty = abs(qty)
                    else:
                        outgoing_qty = abs(qty)
                else:
                    continue

                if action_date == selected_date:
                    received_today_map[material_id] += incoming_qty
                    non_production_outgoing_today_map[material_id] += outgoing_qty
                elif action_date > selected_date:
                    future_net_delta_map[material_id] += delta

            cursor.execute(
                f'''
                SELECT
                    p.status,
                    pmu.material_id,
                    COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty
                FROM production_material_usage pmu
                JOIN productions p ON p.id = pmu.production_id
                LEFT JOIN production_material_lot_usage pmlu
                  ON pmlu.production_usage_id = pmu.id
                WHERE p.workplace = ?
                  AND pmu.material_id IN ({placeholders})
                  AND pmu.material_id IS NOT NULL
                  AND COALESCE(p.production_date, '') > ?
                ORDER BY pmu.material_id, pmu.id
                ''',
                [workplace, *material_ids, selected_date],
            )
            for row in cursor.fetchall():
                row = dict(row)
                if not _normalize_completed_status(row.get('status')):
                    continue
                material_id = int(row['material_id'] or 0)
                if material_id <= 0:
                    continue
                future_production_outgoing_map[material_id] += float(row['qty'] or 0)

        rows = []
        material_info_by_id = {int(item['id']): item for item in materials if int(item.get('id') or 0) > 0}
        lot_row_map = {}

        def ensure_lot_row(material_id, lot_id):
            if material_id not in material_info_by_id or lot_id <= 0:
                return None
            bucket = lot_row_map.get(lot_id)
            if bucket is None:
                item = material_info_by_id[material_id]
                bucket = {
                    'material_id': material_id,
                    'lot_id': lot_id,
                    'code': item.get('code') or '',
                    'name': item.get('name') or '',
                    'category': item.get('category') or '',
                    'unit': item.get('unit') or '',
                    'supplier_name': item.get('supplier_name') or '',
                    'receiving_date': '',
                    'manufacture_date': '',
                    'expiry_date': '',
                    'current_stock': 0.0,
                    'received_today': 0.0,
                    'nonprod_out_today': 0.0,
                    'prod_out_today': 0.0,
                    'future_log_net': 0.0,
                    'future_prod_out': 0.0,
                }
                lot_row_map[lot_id] = bucket
            return bucket

        if material_ids:
            placeholders = ','.join(['?'] * len(material_ids))
            cursor.execute(
                f'''
                SELECT
                    ml.id as material_lot_id,
                    ml.material_id,
                    ml.receiving_date,
                    ml.manufacture_date,
                    ml.expiry_date
                FROM material_lots ml
                WHERE ml.material_id IN ({placeholders})
                  AND COALESCE(ml.is_disposed, 0) = 0
                ORDER BY ml.material_id, ml.receiving_date ASC, ml.id ASC
                ''',
                material_ids,
            )
            for row in cursor.fetchall():
                row = dict(row)
                bucket = ensure_lot_row(int(row.get('material_id') or 0), int(row.get('material_lot_id') or 0))
                if not bucket:
                    continue
                bucket['receiving_date'] = (row.get('receiving_date') or '').strip()
                bucket['manufacture_date'] = (row.get('manufacture_date') or '').strip()
                bucket['expiry_date'] = (row.get('expiry_date') or '').strip()

            if location_ids:
                loc_placeholders = ','.join(['?'] * len(location_ids))
                cursor.execute(
                    f'''
                    SELECT
                        ml.material_id,
                        ml.id as material_lot_id,
                        COALESCE(SUM(b.qty), 0) as workplace_stock
                    FROM material_lots ml
                    JOIN inv_material_lot_balances b
                      ON b.material_lot_id = ml.id
                    WHERE ml.material_id IN ({placeholders})
                      AND COALESCE(ml.is_disposed, 0) = 0
                      AND b.location_id IN ({loc_placeholders})
                    GROUP BY ml.material_id, ml.id
                    ''',
                    [*material_ids, *location_ids],
                )
                for row in cursor.fetchall():
                    bucket = ensure_lot_row(int(row['material_id'] or 0), int(row['material_lot_id'] or 0))
                    if not bucket:
                        continue
                    bucket['current_stock'] = float(row['workplace_stock'] or 0.0)

            cursor.execute(
                f'''
                SELECT
                    mll.material_id,
                    mll.material_lot_id,
                    COALESCE(mll.action, '') as action,
                    COALESCE(mll.quantity, 0) as quantity,
                    COALESCE(mll.note, '') as note,
                    COALESCE(mll.created_at, '') as created_at
                FROM material_lot_logs mll
                WHERE mll.material_id IN ({placeholders})
                  AND mll.material_lot_id IS NOT NULL
                ORDER BY mll.id
                ''',
                material_ids,
            )
            for row in cursor.fetchall():
                row = dict(row)
                bucket = ensure_lot_row(int(row.get('material_id') or 0), int(row.get('material_lot_id') or 0))
                if not bucket:
                    continue
                action = (row.get('action') or '').strip()
                note = (row.get('note') or '').strip()
                qty = float(row.get('quantity') or 0.0)

                incoming_qty = 0.0
                outgoing_qty = 0.0
                action_date = ''
                if action == 'create':
                    if (material_info_by_id.get(int(row.get('material_id') or 0), {}).get('workplace') or '').strip() != workplace:
                        continue
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                    incoming_qty = abs(qty)
                elif action in {'issue_request_complete', 'issue_request_update'} and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                    incoming_qty = abs(qty)
                elif action == 'issue_request_cancel' and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _get_print_workday(row.get('created_at'))
                    incoming_qty = -abs(qty)
                elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _get_print_workday(row.get('created_at'))
                    outgoing_qty = abs(qty)
                elif action == 'export_request_cancel' and workplace_prefix and note.startswith(workplace_prefix):
                    action_date = _get_print_workday(row.get('created_at'))
                    outgoing_qty = -abs(qty)
                elif action == 'adjustment' and note in ('inventory_audit_apply_workplace_plus', 'inventory_audit_apply_workplace_minus'):
                    action_date = _get_print_workday(row.get('created_at'))
                    if qty >= 0:
                        incoming_qty = qty
                    else:
                        outgoing_qty = abs(qty)
                else:
                    continue

                if not action_date:
                    continue
                if action_date == selected_date:
                    bucket['received_today'] += incoming_qty
                    bucket['nonprod_out_today'] += outgoing_qty
                elif action_date > selected_date:
                    bucket['future_log_net'] += (incoming_qty - outgoing_qty)

            production_query = '''
                SELECT
                    p.status,
                    pmu.material_id,
                    pmlu.material_lot_id,
                    p.production_date,
                    COALESCE(pmlu.quantity, 0) as qty
                FROM production_material_lot_usage pmlu
                JOIN production_material_usage pmu ON pmu.id = pmlu.production_usage_id
                JOIN productions p ON p.id = pmu.production_id
                WHERE p.workplace = ?
                  AND pmu.material_id IN ({placeholders})
            '''
            production_params = [workplace, *material_ids]
            if location_ids:
                loc_placeholders = ','.join(['?'] * len(location_ids))
                production_query += f' AND pmlu.location_id IN ({loc_placeholders})'
                production_params.extend(location_ids)
            production_query += ' ORDER BY pmlu.material_id, pmlu.id'
            cursor.execute(production_query.format(placeholders=placeholders), production_params)
            for row in cursor.fetchall():
                row = dict(row)
                if not _normalize_completed_status(row.get('status')):
                    continue
                bucket = ensure_lot_row(int(row.get('material_id') or 0), int(row.get('material_lot_id') or 0))
                if not bucket:
                    continue
                prod_date = (row.get('production_date') or '').strip()
                qty = float(row.get('qty') or 0.0)
                if prod_date == selected_date:
                    bucket['prod_out_today'] += qty
                elif prod_date > selected_date:
                    bucket['future_prod_out'] += qty

        lot_items = sorted(
            lot_row_map.values(),
            key=lambda item: (
                _get_production_material_sort_key({'category': item.get('category'), 'material_name': item.get('name')}) or '',
                item.get('name') or '',
                item.get('receiving_date') or '',
                int(item.get('lot_id') or 0),
            ),
        )
        for item in lot_items:
            closing_stock = float(item.get('current_stock') or 0.0) - float(item.get('future_log_net') or 0.0) + float(item.get('future_prod_out') or 0.0)
            received_today = float(item.get('received_today') or 0.0)
            outgoing_today = float(item.get('nonprod_out_today') or 0.0) + float(item.get('prod_out_today') or 0.0)
            opening_stock = closing_stock - received_today + outgoing_today
            if abs(closing_stock) < 1e-6:
                closing_stock = 0.0
            if abs(opening_stock) < 1e-6:
                opening_stock = 0.0
            if closing_stock <= 0 and received_today <= 0 and outgoing_today <= 0:
                continue

            expiry_date = (item.get('expiry_date') or '').strip()
            manufacture_date = (item.get('manufacture_date') or '').strip()
            expiry_or_mfg = f'(\uc18c) {expiry_date}' if expiry_date else (f'(\uc81c) {manufacture_date}' if manufacture_date else '')
            rows.append({
                'code': item.get('code') or '',
                'name': item.get('name') or '',
                'category': item.get('category') or '',
                'unit': item.get('unit') or '',
                'supplier_name': item.get('supplier_name') or '',
                'receiving_date': (item.get('receiving_date') or '').strip(),
                'expiry_or_mfg': expiry_or_mfg,
                'opening_stock': _round_1(opening_stock),
                'received_today': _round_1(received_today),
                'outgoing_today': _round_1(outgoing_today),
                'closing_stock': _round_1(closing_stock),
                'note': '',
            })

        oil_rows = [row for row in rows if not _is_salt_material_category(row.get('category'))]
        salt_rows = [row for row in rows if _is_salt_material_category(row.get('category'))]
        salt_rows.sort(
            key=lambda row: (
                1 if not (row.get('receiving_date') or '').strip() else 0,
                (row.get('receiving_date') or '').strip(),
                row.get('name') or '',
            )
        )

        min_rows = max(12, len(rows))
        middle_blank_count = max(0, min_rows - len(oil_rows) - len(salt_rows))
        rows = oil_rows + (
            [
                {
                    'code': '',
                    'name': '',
                    'category': '',
                    'unit': '',
                    'supplier_name': '',
                    'receiving_date': '',
                    'expiry_or_mfg': '',
                    'opening_stock': '',
                    'received_today': '',
                    'outgoing_today': '',
                    'closing_stock': '',
                    'note': '',
                }
                for _ in range(middle_blank_count)
            ]
        ) + salt_rows

        while len(rows) < min_rows:
            rows.append({'code': '', 'name': '', 'category': '', 'unit': '', 'supplier_name': '', 'receiving_date': '', 'expiry_or_mfg': '', 'opening_stock': '', 'received_today': '', 'outgoing_today': '', 'closing_stock': '', 'note': ''})

        author_name = (session.get('user', {}) or {}).get('name') or (session.get('user', {}) or {}).get('username') or ''
        weekday_labels = ['\uc6d4', '\ud654', '\uc218', '\ubaa9', '\uae08', '\ud1a0', '\uc77c']
        period_text = f'{report_date.year}\ub144 {report_date.month}\uc6d4 {report_date.day}\uc77c ({weekday_labels[report_date.weekday()]}\uc694\uc77c)'
        workplace_title = _format_print_workplace(workplace).replace('\uc2e0\uad00 2\uce35', '\uc2e0\uad00_2F').replace('\uc2e0\uad00 1\uce35', '\uc2e0\uad00_1F').replace('2\uce35', '2F').replace('1\uce35', '1F')
        if workplace_title:
            workplace_title = f'{workplace_title} 조미김 작업장'
        scope_label = '신안' if material_scope == 'sinan' else '예맛'

        return render_template(
            'material_checksheet_preview.html',
            user=session['user'],
            report_date=selected_date,
            period_text=period_text,
            workplace_title=workplace_title,
            author_name=author_name,
            rows=rows[:max(min_rows, len(rows))],
            material_scope=material_scope,
            scope_label=scope_label,
        )
    finally:
        conn.close()


@bp.route('/materials/packaging-checksheet-preview')
@login_required
def packaging_checksheet_preview():
    workplace = get_workplace()
    selected_production_id = int(request.args.get('production_id') or 0) if str(request.args.get('production_id') or '').strip().isdigit() else 0
    selected_date = (request.args.get('date') or today_local().isoformat()).strip()
    try:
        report_date = datetime.strptime(selected_date, '%Y-%m-%d').date()
    except Exception:
        report_date = today_local()
        selected_date = report_date.isoformat()

    conn = get_db()
    cursor = conn.cursor()
    try:
        if selected_production_id > 0:
            cursor.execute(
                '''
                SELECT id, COALESCE(production_date, '') as production_date, COALESCE(workplace, '') as workplace
                FROM productions
                WHERE id = ?
                ''',
                (selected_production_id,),
            )
            production_row = cursor.fetchone()
            if production_row:
                production_row = dict(production_row)
                production_workplace = (production_row.get('workplace') or '').strip()
                if production_workplace:
                    workplace = production_workplace
                production_date = (production_row.get('production_date') or '').strip()
                if production_date:
                    selected_date = production_date
                    try:
                        report_date = datetime.strptime(selected_date, '%Y-%m-%d').date()
                    except Exception:
                        pass

        packaging_material_map = _get_packaging_material_map(cursor, workplace)
        packaging_material_ids = list(packaging_material_map.keys())
        incoming_rows = []
        outgoing_rows = []

        if packaging_material_ids:
            placeholders = ','.join(['?'] * len(packaging_material_ids))
            workplace_prefix = (workplace or '').strip()

            cursor.execute(
                f'''
                SELECT
                    mll.material_id,
                    mll.material_lot_id,
                    COALESCE(mll.action, '') as action,
                    COALESCE(mll.quantity, 0) as quantity,
                    COALESCE(mll.note, '') as note,
                    COALESCE(mll.created_at, '') as created_at,
                    COALESCE(ml.receiving_date, '') as receiving_date,
                    COALESCE(ml.manufacture_date, '') as manufacture_date,
                    COALESCE(ml.expiry_date, '') as expiry_date
                FROM material_lot_logs mll
                LEFT JOIN material_lots ml ON ml.id = mll.material_lot_id
                WHERE mll.material_id IN ({placeholders})
                ORDER BY mll.id
                ''',
                packaging_material_ids,
            )
            incoming_map = {}
            outgoing_log_map = {}
            for row in cursor.fetchall():
                row = dict(row)
                material_id = int(row.get('material_id') or 0)
                if material_id <= 0:
                    continue
                action = (row.get('action') or '').strip()
                note = (row.get('note') or '').strip()
                qty = float(row.get('quantity') or 0)
                received_qty = 0.0
                outgoing_qty = 0.0
                note_text = ''
                action_date = ''
                if action == 'create':
                    received_qty = abs(qty)
                    note_text = '신규 로트 입고'
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'issue_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    received_qty = abs(qty)
                    note_text = '불출 입고'
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'issue_request_update' and workplace_prefix and note.startswith(workplace_prefix):
                    received_qty = abs(qty)
                    note_text = '불출 입고'
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action in ('issue_request_cancel', 'delete'):
                    received_qty = -abs(qty)
                    note_text = '불출 입고'
                    action_date = _resolve_packaging_incoming_action_date(row, action)
                elif action == 'export_request_complete' and workplace_prefix and note.startswith(workplace_prefix):
                    outgoing_qty = abs(qty)
                    note_text = _format_packaging_export_note(note, workplace_prefix)
                    action_date = _get_print_workday(row.get('created_at'))
                else:
                    continue
                if action_date != selected_date:
                    continue

                base_item = packaging_material_map.get(material_id, {})
                expiry_date = (row.get('expiry_date') or '').strip()
                manufacture_date = (row.get('manufacture_date') or '').strip()
                expiry_or_mfg = expiry_date or manufacture_date
                if received_qty:
                    key = (material_id, (row.get('receiving_date') or '').strip(), expiry_or_mfg, note_text)
                    bucket = incoming_map.setdefault(
                        key,
                        {
                            'name': base_item.get('name') or '',
                            'supplier_name': base_item.get('supplier_name') or '',
                            'quantity': 0.0,
                            'receiving_date': (row.get('receiving_date') or '').strip() or selected_date,
                            'expiry_or_mfg': expiry_or_mfg,
                            'note': note_text,
                            'unit': base_item.get('unit') or '',
                        },
                    )
                    bucket['quantity'] += received_qty
                if outgoing_qty:
                    key = (material_id, note_text, (row.get('receiving_date') or '').strip(), expiry_or_mfg)
                    bucket = outgoing_log_map.setdefault(
                        key,
                        {
                            'name': base_item.get('name') or '',
                            'target_name': '반출',
                            'quantity': 0.0,
                            'receiving_date': (row.get('receiving_date') or '').strip(),
                            'expiry_or_mfg': expiry_or_mfg,
                            'note': note_text,
                            'unit': base_item.get('unit') or '',
                        },
                    )
                    bucket['quantity'] += outgoing_qty

            incoming_map = {
                key: value
                for key, value in incoming_map.items()
                if round(float(value.get('quantity') or 0), 4) > 0
            }

            cursor.execute(
                f'''
                SELECT
                    p2.status,
                    pmu.material_id,
                    COALESCE(p.name, '') as product_name,
                    COALESCE(pmlu.quantity, pmu.actual_quantity, 0) as qty,
                    COALESCE(ml.receiving_date, '') as receiving_date,
                    COALESCE(ml.manufacture_date, '') as manufacture_date,
                    COALESCE(ml.expiry_date, '') as expiry_date
                FROM production_material_usage pmu
                JOIN productions p2 ON p2.id = pmu.production_id
                LEFT JOIN products p ON p.id = p2.product_id
                LEFT JOIN production_material_lot_usage pmlu ON pmlu.production_usage_id = pmu.id
                LEFT JOIN material_lots ml ON ml.id = pmlu.material_lot_id
                WHERE p2.workplace = ?
                  AND COALESCE(p2.production_date, '') = ?
                  AND (? <= 0 OR p2.id = ?)
                  AND pmu.material_id IN ({placeholders})
                  AND pmu.material_id IS NOT NULL
                ORDER BY pmu.material_id, p.name
                ''',
                [workplace, selected_date, selected_production_id, selected_production_id, *packaging_material_ids],
            )
            production_outgoing_map = {}
            for row in cursor.fetchall():
                row = dict(row)
                if not _normalize_completed_status(row.get('status')):
                    continue
                material_id = int(row.get('material_id') or 0)
                if material_id <= 0:
                    continue
                qty = float(row.get('qty') or 0)
                expiry_date = (row.get('expiry_date') or '').strip()
                manufacture_date = (row.get('manufacture_date') or '').strip()
                expiry_or_mfg = expiry_date or manufacture_date
                key = (material_id, (row.get('receiving_date') or '').strip(), expiry_or_mfg)
                base_item = packaging_material_map.get(material_id, {})
                bucket = production_outgoing_map.setdefault(
                    key,
                    {
                        'name': base_item.get('name') or '',
                        'target_names': [base_item.get('supplier_name') or '생산 출고'],
                        'quantity': 0.0,
                        'receiving_date': (row.get('receiving_date') or '').strip(),
                        'expiry_or_mfg': expiry_or_mfg,
                        'note': '',
                        'unit': base_item.get('unit') or '',
                    },
                )
                bucket['quantity'] += qty
                if base_item.get('supplier_name'):
                    bucket['target_names'] = [base_item.get('supplier_name')]

            incoming_rows = sorted(incoming_map.values(), key=lambda item: ((item.get('name') or ''), (item.get('receiving_date') or '')))
            for row in incoming_rows:
                row['quantity'] = _round_1(row.get('quantity') or 0)

            for row in production_outgoing_map.values():
                outgoing_rows.append(
                    {
                        'name': row.get('name') or '',
                        'target_name': ', '.join(row.get('target_names') or []) or '생산 출고',
                        'quantity': _round_1(row.get('quantity') or 0),
                        'receiving_date': row.get('receiving_date') or '',
                        'expiry_or_mfg': row.get('expiry_or_mfg') or '',
                        'note': '',
                        'unit': row.get('unit') or '',
                    }
                )
            for row in outgoing_log_map.values():
                outgoing_rows.append(
                    {
                        'name': row.get('name') or '',
                        'target_name': row.get('target_name') or '반출',
                        'quantity': _round_1(row.get('quantity') or 0),
                        'receiving_date': row.get('receiving_date') or '',
                        'expiry_or_mfg': row.get('expiry_or_mfg') or '',
                        'note': row.get('note') or '',
                        'unit': row.get('unit') or '',
                    }
                )
            outgoing_rows.sort(key=lambda item: ((item.get('name') or ''), (item.get('target_name') or '')))

        incoming_limit_single_page = 9
        outgoing_limit_single_page = 12
        use_single_page_layout = (
            len(incoming_rows) <= incoming_limit_single_page
            and len(outgoing_rows) <= outgoing_limit_single_page
        )

        if use_single_page_layout:
            incoming_rows = _pad_print_rows(incoming_rows, 10, _blank_packaging_incoming_row)
            outgoing_rows = _pad_print_rows(outgoing_rows, 14, _blank_packaging_outgoing_row)
        else:
            incoming_rows = _pad_print_rows(incoming_rows, 19, _blank_packaging_incoming_row)
            outgoing_rows = _pad_print_rows(outgoing_rows, 19, _blank_packaging_outgoing_row)

        author_name = (session.get('user', {}) or {}).get('name') or (session.get('user', {}) or {}).get('username') or ''
        weekday_labels = ['월', '화', '수', '목', '금', '토', '일']
        period_text = f'{report_date.year}년 {report_date.month}월 {report_date.day}일 ({weekday_labels[report_date.weekday()]}요일)'
        workplace_title = _format_print_workplace(workplace)

        return render_template(
            'packaging_checksheet_preview.html',
            user=session['user'],
            report_date=selected_date,
            period_text=period_text,
            workplace_title=workplace_title,
            author_name=author_name,
            incoming_rows=incoming_rows,
            outgoing_rows=outgoing_rows,
            use_single_page_layout=use_single_page_layout,
        )
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
               COALESCE(p.category, '') AS product_category,
               COALESCE(p.set_item_type, '') AS set_item_type,
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
    expiry_rows = _build_production_expiry_rows(production, display_expiry)

    # 원재료(원초) 사용 내역
    cursor.execute(
        '''
        SELECT
            pmu.*,
            COALESCE(NULLIF(TRIM(pmu.override_car_number), ''), rm.car_number) as car_number,
            COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), rm.receiving_date) as receiving_date
        FROM production_material_usage pmu
        LEFT JOIN raw_materials rm ON pmu.raw_material_id = rm.id
        WHERE pmu.production_id = ? AND pmu.raw_material_id IS NOT NULL
        ORDER BY COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), rm.receiving_date) ASC, rm.id ASC
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
            COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), ml.receiving_date) as lot_receiving_date,
            COALESCE(NULLIF(TRIM(pmu.override_manufacture_date), ''), ml.manufacture_date) as lot_manufacture_date,
            COALESCE(NULLIF(TRIM(pmu.override_expiry_date), ''), ml.expiry_date) as lot_expiry_date,
            ml.lot_seq
        FROM production_material_usage pmu
        LEFT JOIN materials m ON pmu.material_id = m.id
        LEFT JOIN production_material_lot_usage pmlu
          ON pmlu.production_usage_id = pmu.id
        LEFT JOIN material_lots ml
          ON ml.id = pmlu.material_lot_id
        WHERE pmu.production_id = ? AND pmu.material_id IS NOT NULL
        ORDER BY m.category, m.name, COALESCE(NULLIF(TRIM(pmu.override_receiving_date), ''), ml.receiving_date, ''), COALESCE(ml.lot_seq, 0), pmu.id
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
        item['print_display_date'] = _format_packaging_print_date(item)
        if _exclude_from_production_print(item):
            continue
        item['base_sort_key'] = _get_production_material_sort_key(item)
        grouped_materials[_get_production_material_section(item)].append(item)

    # A set finished product consumes semi-finished products.  They are kept in a
    # separate FIFO usage table because they are product inventory rather than a
    # conventional material lot, then rendered in the material section of the journal.
    is_set_finished_product = (
        str(production['product_category'] or '').strip() == '세트'
        and str(production['set_item_type'] or '').strip().lower() == 'finished'
    )
    if is_set_finished_product:
        component_usage_rows = cursor.execute(
            '''
            SELECT
                pclu.id AS component_lot_usage_id,
                pmu.id AS usage_id,
                cp.code AS material_code,
                cp.name AS material_name,
                pclu.quantity AS lot_used_quantity,
                pclu.quantity AS actual_quantity,
                pclu.receiving_date AS lot_receiving_date,
                pclu.expiry_date AS lot_expiry_date
            FROM production_component_lot_usage pclu
            JOIN production_material_usage pmu ON pmu.id = pclu.production_usage_id
            LEFT JOIN products cp ON cp.id = pclu.component_product_id
            WHERE pmu.production_id = ?
            ORDER BY pclu.receiving_date ASC, pclu.component_production_id ASC, pclu.id ASC
            ''',
            (production_id,),
        ).fetchall()
        recorded_usage_ids = set()
        for row in component_usage_rows:
            item = dict(row)
            recorded_usage_ids.add(int(item.get('usage_id') or 0))
            item.update(
                {
                    'material_id': None,
                    'category': '부재료',
                    'lot_manufacture_date': '',
                    'base_sort_key': '00_set_component',
                }
            )
            grouped_materials['base'].append(item)

        # Existing completed set productions predate the FIFO usage table.  Keep their
        # journals useful by resolving the available semi-finished outputs in FIFO order
        # at print time, while newly saved production records use the persisted rows above.
        legacy_component_rows = cursor.execute(
            '''
            SELECT pmu.id AS usage_id, pmu.component_product_id,
                   COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) AS quantity,
                   cp.code AS material_code, cp.name AS material_name
            FROM production_material_usage pmu
            LEFT JOIN products cp ON cp.id = pmu.component_product_id
            WHERE pmu.production_id = ?
              AND pmu.component_product_id IS NOT NULL
              AND COALESCE(pmu.actual_quantity, pmu.expected_quantity, 0) > 0
            ORDER BY pmu.id
            ''',
            (production_id,),
        ).fetchall()
        for legacy_row in legacy_component_rows:
            legacy = dict(legacy_row)
            if int(legacy['usage_id'] or 0) in recorded_usage_ids:
                continue
            remaining_quantity = float(legacy['quantity'] or 0)
            source_rows = cursor.execute(
                '''
                SELECT id, production_date, status, actual_boxes,
                       expiry_date, expiry_date_2, expiry_date_3,
                       expiry_boxes_1, expiry_boxes_2, expiry_boxes_3,
                       sample_excluded_boxes_1, sample_excluded_boxes_2, sample_excluded_boxes_3
                FROM productions
                WHERE product_id = ?
                  AND workplace = ?
                  AND COALESCE(actual_boxes, 0) > 0
                  AND COALESCE(production_date, '') <= ?
                ORDER BY production_date ASC, id ASC
                ''',
                (legacy['component_product_id'], workplace, production['production_date']),
            ).fetchall()
            fallback_items = []
            for source_row in source_rows:
                source = dict(source_row)
                if _normalize_production_status(source.get('status')) != '완료':
                    continue
                source['box_quantity'] = 1
                expiry_rows = _build_production_expiry_rows(source)
                lots = []
                for expiry_row in expiry_rows:
                    try:
                        lot_quantity = float(expiry_row.get('actual_boxes') or 0)
                    except (TypeError, ValueError):
                        lot_quantity = 0.0
                    if lot_quantity > 0:
                        lots.append((str(expiry_row.get('expiry_date') or ''), lot_quantity))
                if not lots:
                    lots = [(str(source.get('expiry_date') or ''), float(source.get('actual_boxes') or 0))]
                for expiry_date, lot_quantity in lots:
                    if remaining_quantity <= 1e-9:
                        break
                    consumed_quantity = min(remaining_quantity, lot_quantity)
                    if consumed_quantity <= 1e-9:
                        continue
                    fallback_items.append((str(source.get('production_date') or ''), expiry_date, consumed_quantity))
                    remaining_quantity -= consumed_quantity
                if remaining_quantity <= 1e-9:
                    break
            if remaining_quantity > 1e-9:
                fallback_items.append(('', '', remaining_quantity))
            for receiving_date, expiry_date, consumed_quantity in fallback_items:
                grouped_materials['base'].append(
                    {
                        'usage_id': legacy['usage_id'],
                        'material_id': None,
                        'material_code': legacy.get('material_code') or '',
                        'material_name': legacy.get('material_name') or '',
                        'category': '부재료',
                        'lot_used_quantity': consumed_quantity,
                        'actual_quantity': consumed_quantity,
                        'lot_receiving_date': receiving_date,
                        'lot_expiry_date': expiry_date,
                        'lot_manufacture_date': '',
                        'base_sort_key': '00_set_component',
                    }
                )

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
        expiry_rows=expiry_rows,
        same_day_index=same_day_index,
        same_day_total=same_day_total,
    )
