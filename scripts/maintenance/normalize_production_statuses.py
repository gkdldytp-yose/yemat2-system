"""Normalize recognized legacy production status values.

Run without arguments for a read-only report.  Use ``--apply`` only after a
verified backup; unknown values are intentionally preserved for manual review.
"""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
DB_PATH = ROOT / 'yemat.db'
CANONICAL_BY_LEGACY_HEX = {
    '3FEABEA8ECA6BA': '\uC644\uB8CC',
    '3FEB8D89ECA099': '\uC608\uC815',
}


def report_and_normalize(*, apply=False, db_path=DB_PATH):
    conn = sqlite3.connect(str(db_path))
    try:
        report = {}
        for table in ('productions', 'production_schedules'):
            rows = conn.execute(
                f'SELECT HEX(status) AS value_hex, COUNT(*) AS count FROM {table} GROUP BY HEX(status)'
            ).fetchall()
            report[table] = {value_hex or 'NULL': count for value_hex, count in rows}
            if apply:
                for legacy_hex, canonical in CANONICAL_BY_LEGACY_HEX.items():
                    conn.execute(
                        f'UPDATE {table} SET status = ? WHERE HEX(status) = ?',
                        (canonical, legacy_hex),
                    )
        if apply:
            # ``??`` is not a status label, but a paired production/schedule
            # with no actual output can be safely restored to planned.  Any
            # other unknown value remains untouched for manual review.
            conn.execute(
                "UPDATE productions SET status = ? "
                "WHERE HEX(status) = '3F3F' AND COALESCE(actual_boxes, 0) <= 0",
                ('\uC608\uC815',),
            )
            conn.execute(
                "UPDATE production_schedules SET status = ? "
                "WHERE HEX(status) = '3F3F' AND production_id IN ("
                "SELECT id FROM productions WHERE status = ? AND COALESCE(actual_boxes, 0) <= 0)",
                ('\uC608\uC815', '\uC608\uC815'),
            )
        if apply:
            conn.commit()
        return report
    finally:
        conn.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true', help='write recognized legacy values')
    args = parser.parse_args()
    results = report_and_normalize(apply=args.apply)
    print('production status normalization:', 'applied' if args.apply else 'report only')
    for table, values in results.items():
        print(table, values)
