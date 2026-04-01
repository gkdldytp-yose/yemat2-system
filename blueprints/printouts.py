from flask import Blueprint, render_template, session, redirect, url_for
from datetime import datetime, timedelta
from collections import defaultdict
import math
import calendar

from core import get_db, login_required, get_workplace

bp = Blueprint('printouts', __name__)


def _packaging_order(row):
    category = (row.get('category') or '').strip()
    name = (row.get('material_name') or '').strip()
    text = f"{category} {name}"
    if '내포' in text:
        return 0
    if '외포' in text:
        return 1
    if '박스' in text:
        return 2
    if '실리카' in text:
        return 3
    if '트레이' in text:
        return 4
    return 5


def _round_1(value):
    if value is None:
        return None
    return round(float(value), 1)


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
            m.name as material_name,
            m.category,
            m.unit,
            pmlu.quantity as lot_used_quantity,
            ml.lot as lot_no,
            ml.receiving_date as lot_receiving_date,
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

    packaging_categories = {'포장재', '내포', '외포', '실리카', '실리카겔', '트레이', '박스'}
    packaging_keywords = ['내포', '외포', '박스', '트레이', '실리카']
    packaging = []
    others = []
    for row in material_usage_rows:
        item = dict(row)
        cat = (item.get('category') or '')
        name = (item.get('material_name') or '')
        is_pack = cat in packaging_categories or any(k in name for k in packaging_keywords)

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

        if is_pack:
            packaging.append(item)
        else:
            others.append(item)

    packaging.sort(
        key=lambda item: (
            _packaging_order(item),
            item.get('material_name') or '',
            item.get('lot_receiving_date') or '',
            item.get('lot_seq') or 0,
            item.get('usage_id') or 0,
        )
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
        material_usages=others,
        packaging_usages=packaging,
        date_str=date_str,
        weekday=weekday,
        workplace=workplace,
        display_expiry=display_expiry,
    )
