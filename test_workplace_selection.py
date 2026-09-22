import unittest

from app import create_app


class WorkplaceSelectionTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()

    def _set_multi_workplace_user(self, selected_workplace=None):
        with self.client.session_transaction() as session:
            session['user'] = {
                'id': 1,
                'username': 'workplace_selection_test',
                'name': 'Test User',
                'is_admin': False,
                'role': 'production',
                'workplaces': ['A', 'B'],
            }
            if selected_workplace is not None:
                session['workplace'] = selected_workplace

    def test_menu_is_blocked_until_multi_workplace_user_selects_workplace(self):
        self._set_multi_workplace_user()

        response = self.client.get('/products', follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'], '/select-workplace?required=1')

    def test_invalid_saved_workplace_is_cleared_and_user_is_prompted_to_select(self):
        self._set_multi_workplace_user(selected_workplace='invalid-workplace')

        response = self.client.get('/materials', follow_redirects=False)

        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers['Location'], '/select-workplace?required=1')
        with self.client.session_transaction() as session:
            self.assertNotIn('workplace', session)

    def test_workplace_selection_page_is_available_without_selection(self):
        self._set_multi_workplace_user()

        response = self.client.get('/select-workplace')

        self.assertEqual(response.status_code, 200)


if __name__ == '__main__':
    unittest.main()
