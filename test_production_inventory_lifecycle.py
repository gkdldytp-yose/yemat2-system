import sqlite3
import unittest

from blueprints.production import _consume_material_fifo, _sync_material_stock_with_lots
from core import ensure_production_status_triggers


class ProductionInventoryLifecycleTestCase(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(':memory:')
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript('''
            CREATE TABLE productions (id INTEGER PRIMARY KEY, status TEXT);
            CREATE TABLE production_schedules (id INTEGER PRIMARY KEY, status TEXT);
            CREATE TABLE materials (id INTEGER PRIMARY KEY, current_stock REAL);
            CREATE TABLE material_lots (
                id INTEGER PRIMARY KEY, material_id INTEGER, lot TEXT,
                receiving_date TEXT, lot_seq INTEGER, current_quantity REAL,
                quantity REAL, is_disposed INTEGER DEFAULT 0
            );
            CREATE TABLE production_material_lot_usage (
                id INTEGER PRIMARY KEY, production_id INTEGER, production_usage_id INTEGER,
                material_id INTEGER, material_lot_id INTEGER, quantity REAL, location_id INTEGER
            );
            CREATE TABLE material_lot_logs (
                id INTEGER PRIMARY KEY, material_lot_id INTEGER, material_id INTEGER,
                action TEXT, quantity REAL, note TEXT
            );
        ''')
        ensure_production_status_triggers(self.conn)

    def tearDown(self):
        self.conn.close()

    def test_completed_production_deducts_fifo_lots_updates_stock_and_keeps_history(self):
        self.conn.execute("INSERT INTO productions VALUES (1, CAST(X'3FEABEA8ECA6BA' AS TEXT))")
        self.conn.execute("INSERT INTO production_schedules VALUES (1, CAST(X'3FEABEA8ECA6BA' AS TEXT))")
        self.conn.execute('INSERT INTO materials VALUES (1, 10)')
        self.conn.executemany(
            'INSERT INTO material_lots VALUES (?, 1, ?, ?, ?, ?, ?, 0)',
            [(1, 'LOT-OLD', '2026-01-01', 1, 3, 3), (2, 'LOT-NEW', '2026-01-02', 2, 7, 7)],
        )

        consumed = _consume_material_fifo(self.conn.cursor(), 1, 11, 1, 8)
        _sync_material_stock_with_lots(self.conn, 1)

        self.assertEqual(self.conn.execute('SELECT status FROM productions WHERE id=1').fetchone()[0], '\uC644\uB8CC')
        self.assertEqual(self.conn.execute('SELECT status FROM production_schedules WHERE id=1').fetchone()[0], '\uC644\uB8CC')
        self.assertEqual(consumed, [
            {'lot_id': 1, 'quantity': 3.0, 'lot': 'LOT-OLD'},
            {'lot_id': 2, 'quantity': 5.0, 'lot': 'LOT-NEW'},
        ])
        self.assertEqual(self.conn.execute('SELECT current_quantity FROM material_lots WHERE id=1').fetchone()[0], 0)
        self.assertEqual(self.conn.execute('SELECT current_quantity FROM material_lots WHERE id=2').fetchone()[0], 2)
        self.assertEqual(self.conn.execute('SELECT current_stock FROM materials WHERE id=1').fetchone()[0], 2)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM production_material_lot_usage').fetchone()[0], 2)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM material_lot_logs WHERE action='consume'").fetchone()[0], 2)

    def test_stock_sync_counts_workplace_balances_without_warehouse(self):
        self.conn.executescript("""
            CREATE TABLE inv_locations (id INTEGER PRIMARY KEY, loc_type TEXT);
            CREATE TABLE inv_material_lot_balances (location_id INTEGER, material_lot_id INTEGER, qty REAL);
            INSERT INTO materials VALUES (1, 0);
            INSERT INTO material_lots VALUES (1, 1, 'LOT', '2026-01-01', 1, 15, 15, 0);
            INSERT INTO inv_locations VALUES (1, 'WORKPLACE'), (2, 'WAREHOUSE');
            INSERT INTO inv_material_lot_balances VALUES (1, 1, 5), (2, 1, 10);
        """)
        _sync_material_stock_with_lots(self.conn, 1)
        self.assertEqual(self.conn.execute('SELECT current_stock FROM materials WHERE id=1').fetchone()[0], 5)

    def test_unknown_workplace_does_not_consume_another_location(self):
        self.conn.execute('CREATE TABLE inv_locations (id INTEGER, name TEXT, workplace_code TEXT)')
        with self.assertRaises(ValueError):
            _consume_material_fifo(self.conn.cursor(), 1, 1, 1, 2, 'unknown')


if __name__ == '__main__':
    unittest.main()
