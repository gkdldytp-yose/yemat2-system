import sqlite3
import unittest

from core import _ensure_material_detail_indexes


class MaterialDetailIndexesTestCase(unittest.TestCase):
    def test_existing_databases_gain_indexes_and_history_lookup_uses_them(self):
        conn = sqlite3.connect(':memory:')
        self.addCleanup(conn.close)
        conn.executescript('''
            CREATE TABLE audit_logs (id INTEGER PRIMARY KEY, entity TEXT, entity_id INTEGER);
            CREATE TABLE production_material_usage (id INTEGER PRIMARY KEY, material_id INTEGER, production_id INTEGER);
            CREATE TABLE production_material_lot_usage (id INTEGER PRIMARY KEY, production_usage_id INTEGER, material_id INTEGER);
        ''')
        _ensure_material_detail_indexes(conn)
        _ensure_material_detail_indexes(conn)
        cases = [
            ("SELECT id FROM audit_logs WHERE entity = ? AND entity_id = ? ORDER BY id DESC LIMIT 1", ('production', 1), 'idx_audit_entity_entity_id_id'),
            ('SELECT id FROM production_material_usage WHERE material_id = ?', (1,), 'idx_pmu_material_id'),
            ('SELECT id FROM production_material_usage WHERE production_id = ?', (1,), 'idx_pmu_production_id'),
            ('SELECT id FROM production_material_lot_usage WHERE production_usage_id = ? AND material_id = ?', (1, 1), 'idx_pmlu_usage_material'),
        ]
        for query, params, index in cases:
            with self.subTest(index=index):
                plan = ' '.join(str(row) for row in conn.execute('EXPLAIN QUERY PLAN ' + query, params))
                self.assertIn(index, plan)

    def test_missing_optional_tables_are_skipped(self):
        conn = sqlite3.connect(':memory:')
        self.addCleanup(conn.close)
        _ensure_material_detail_indexes(conn)


if __name__ == '__main__':
    unittest.main()
