import unittest
from uuid import uuid4

from app import create_app
from core import db_transaction


class WorkDaysAdminAccessTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()
        self.username = f'workday_test_{uuid4().hex[:10]}'

    def tearDown(self):
        with db_transaction() as conn:
            conn.execute('DELETE FROM audit_logs WHERE username = ?', (self.username,))
            conn.execute("DELETE FROM audit_logs WHERE username = 'tester'")

    def _login_as(self, *, is_admin):
        with self.client.session_transaction() as session:
            session['user'] = {
                'id': 1,
                'username': self.username,
                'name': self.username,
                'is_admin': is_admin,
                'workplaces': ['2동 신관 2층'],
                'role': 'production' if not is_admin else 'admin',
            }
            session['workplace'] = '2동 신관 2층'

    def test_non_admin_cannot_open_work_days(self):
        self._login_as(is_admin=False)
        response = self.client.get('/work-days')
        self.assertEqual(response.status_code, 302)
        self.assertIn('/', response.headers.get('Location', ''))

    def test_non_admin_cannot_manage_work_days(self):
        self._login_as(is_admin=False)
        response = self.client.post(
            '/work-days/manage',
            data={'date': '2026-06-30', 'type': 'work', 'overtime_hours': '0', 'note': ''},
        )
        self.assertEqual(response.status_code, 302)
        self.assertIn('/', response.headers.get('Location', ''))

    def test_non_admin_cannot_delete_work_days(self):
        self._login_as(is_admin=False)
        response = self.client.post('/work-days/delete', data={'date': '2026-06-30'})
        self.assertEqual(response.status_code, 302)
        self.assertIn('/', response.headers.get('Location', ''))


if __name__ == '__main__':
    unittest.main()
