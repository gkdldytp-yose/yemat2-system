import sqlite3
import pandas as pd
from pathlib import Path

DB_PATH = '/Users/jaehyang/Desktop/yemat1/yemat.db'
EXCEL_PATH = '/Users/jaehyang/Downloads/부자재 db.xlsx'
SHEET = '품번코드마스터'

CATEGORY_MAP = {
    'P-포장재': '포장재',
    'Z-소모품': '소모품',
    'S-실리카겔': '실리카',
    'T-트레이': '트레이',
}


def main():
    if not Path(EXCEL_PATH).exists():
        raise FileNotFoundError(EXCEL_PATH)

    df = pd.read_excel(EXCEL_PATH, sheet_name=SHEET, header=2)
    df = df[df['구분'].astype(str).str.contains('부재료', na=False)].copy()

    # normalize
    df['code'] = df['원.부자재코드'].astype(str).str.strip()
    df['name'] = df['원.부자재명'].astype(str).str.strip()
    df['unit'] = df['단위'].astype(str).str.strip()
    df['unit_price'] = pd.to_numeric(df['단가'], errors='coerce').fillna(0)
    df['moq'] = df['MOQ'].fillna('').astype(str).str.strip()
    df['lead_time'] = df['리드타임'].fillna('').astype(str).str.strip()
    df['category_raw'] = df['내용'].astype(str).str.strip()
    df['category'] = df['category_raw'].map(CATEGORY_MAP).fillna(df['category_raw'])

    # drop empty codes
    df = df[df['code'] != '']

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    for _, row in df.iterrows():
        code = row['code']
        name = row['name']
        category = row['category']
        unit = row['unit'] if row['unit'] != 'nan' else ''
        unit_price = float(row['unit_price']) if pd.notna(row['unit_price']) else 0
        moq = row['moq'] if row['moq'] != 'nan' else ''
        lead_time = row['lead_time'] if row['lead_time'] != 'nan' else ''

        cur.execute('SELECT id, workplace FROM materials WHERE code = ?', (code,))
        existing = cur.fetchone()
        if existing:
            # keep existing workplace if already assigned
            workplace = existing['workplace']
            cur.execute(
                '''
                UPDATE materials
                SET name = ?, category = ?, unit = ?, unit_price = ?, moq = ?, lead_time = ?, supplier_id = ?
                WHERE id = ?
                ''',
                (name, category, unit, unit_price, moq, lead_time, None, existing['id']),
            )
            updated += 1
        else:
            cur.execute(
                '''
                INSERT INTO materials (code, name, category, unit, unit_price, moq, lead_time, supplier_id, workplace)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''',
                (code, name, category, unit, unit_price, moq, lead_time, None, None),
            )
            inserted += 1

    conn.commit()
    conn.close()

    print(f'inserted={inserted}, updated={updated}, skipped={skipped}')


if __name__ == '__main__':
    main()
