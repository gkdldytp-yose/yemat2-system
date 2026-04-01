import sqlite3
from pathlib import Path

DB_PATH = '/Users/jaehyang/Desktop/yemat1/yemat.db'
TXT_PATH = '/Users/jaehyang/Desktop/yemat1/tmp_products_bom.txt'


def parse_category(raw):
    if not raw:
        return None
    raw = raw.strip()
    if not raw:
        return None
    if '.' in raw:
        return raw.split('.', 1)[1].strip()
    return raw


def infer_workplace(name: str):
    if not name:
        return None
    if '자반' in name:
        return '1동 자반'
    if '도시락' in name:
        return '2동 신관 1층'
    if '식탁' in name:
        return '1동 조미'
    return None


def main():
    if not Path(TXT_PATH).exists():
        raise FileNotFoundError(TXT_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    inserted = 0
    updated = 0
    skipped = 0

    with open(TXT_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip('\n')
            if not line.strip():
                continue
            cols = line.split('\t')
            if len(cols) < 2:
                continue
            code = cols[0].strip()
            name = cols[1].strip()
            if not code or not name:
                continue
            # 제품만 등록 (A- 또는 C-)
            if not (code.startswith('A-') or code.startswith('C-')):
                skipped += 1
                continue

            category_raw = cols[2].strip() if len(cols) > 2 else ''
            category = parse_category(category_raw) or '기타'
            workplace = infer_workplace(name)

            cur.execute('SELECT id, workplace FROM products WHERE code = ?', (code,))
            row = cur.fetchone()
            if row:
                cur.execute(
                    '''
                    UPDATE products
                    SET name = ?, category = ?, workplace = ?
                    WHERE id = ?
                    ''',
                    (name, category, workplace if workplace else None, row['id']),
                )
                updated += 1
            else:
                cur.execute(
                    '''
                    INSERT INTO products (name, code, category, workplace)
                    VALUES (?, ?, ?, ?)
                    ''',
                    (name, code, category, workplace if workplace else None),
                )
                inserted += 1

    conn.commit()
    conn.close()

    print(f'inserted={inserted}, updated={updated}, skipped={skipped}')


if __name__ == '__main__':
    main()
