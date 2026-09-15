import unittest

from app import create_app


class TodayWorkPageTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()
        with self.client.session_transaction() as session:
            session['user'] = {
                'id': 1,
                'username': 'admin',
                'is_admin': True,
                'role': 'admin',
                'workplaces': ['2동 신관 2층'],
            }
            session['workplace'] = '2동 신관 2층'

    def test_today_work_page_loads_for_the_selected_workplace(self):
        response = self.client.get('/today-work')
        self.assertEqual(response.status_code, 200)
        self.assertIn('오늘 작업'.encode('utf-8'), response.data)
        self.assertIn('오늘 생산'.encode('utf-8'), response.data)


if __name__ == '__main__':
    unittest.main()
