import unittest
from uuid import uuid4

from app import create_app
from blueprints.admin import _query_integrated_audit_logs
from core import db_transaction


class AuditLogBehaviorTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()

    def tearDown(self):
        prefixes = ('audit_auto_', 'audit_filter_')
        with db_transaction() as conn:
            for prefix in prefixes:
                conn.execute('DELETE FROM audit_logs WHERE COALESCE(username, "") LIKE ?', (f'{prefix}%',))

    def _login_as(self, username, *, is_admin=True):
        with self.client.session_transaction() as session:
            session['user'] = {
                'id': 1,
                'username': username,
                'name': username,
                'is_admin': is_admin,
                'role': 'admin' if is_admin else 'production',
                'workplaces': ['2동 신관 2층'],
            }
            session['workplace'] = '2동 신관 2층'

    def test_hidden_audit_log_excludes_auth_session_rows(self):
        username = f'audit_filter_{uuid4().hex[:10]}'
        with db_transaction() as conn:
            conn.execute(
                '''
                INSERT INTO audit_logs (action, entity, entity_id, data, username, name, workplace, ip, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ''',
                ('login', 'auth_session', 1, '{}', username, username, '2동 신관 2층', '127.0.0.1'),
            )
            conn.execute(
                '''
                INSERT INTO audit_logs (action, entity, entity_id, data, username, name, workplace, ip, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ''',
                ('get', 'main.index', None, '{}', username, username, '2동 신관 2층', '127.0.0.1'),
            )
            rows = _query_integrated_audit_logs(conn.cursor(), username=username, limit=50)

        self.assertTrue(any((row.get('entity') or '') == 'main.index' for row in rows))
        self.assertFalse(any((row.get('entity') or '') == 'auth_session' for row in rows))

    def test_authenticated_request_creates_auto_audit_log(self):
        username = f'audit_auto_{uuid4().hex[:10]}'
        self._login_as(username)

        response = self.client.get('/')
        self.assertEqual(response.status_code, 200)

        with db_transaction() as conn:
            row = conn.execute(
                '''
                SELECT action, entity, data
                FROM audit_logs
                WHERE username = ?
                  AND entity = ?
                ORDER BY id DESC
                LIMIT 1
                ''',
                (username, 'main.index'),
            ).fetchone()

        self.assertIsNotNone(row)
        self.assertEqual(row['action'], 'get')
        self.assertIn('"status_code": 200', row['data'] or '')


if __name__ == '__main__':
    unittest.main()
