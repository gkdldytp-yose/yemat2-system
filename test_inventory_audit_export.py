import unittest

from app import create_app


class InventoryAuditExportTestCase(unittest.TestCase):
    def setUp(self):
        self.app = create_app()
        self.app.config['TESTING'] = True
        self.client = self.app.test_client()

        with self.client.session_transaction() as session:
            session['user'] = {
                'id': 1,
                'username': 'admin',
                'is_admin': True,
                'workplaces': ['2동 신관 2층'],
            }
            session['workplace'] = '2동 신관 2층'

    def test_inventory_audit_export_returns_csv_file(self):
        response = self.client.get(
            '/integrated-management/inventory-audit/export'
            '?inventory_type=all'
            '&inventory_wp=2%EB%8F%99+%EC%8B%A0%EA%B4%80+2%EC%B8%B5'
            '&inventory_search_field=all'
            '&inventory_category='
            '&inventory_product_id='
            '&inventory_q='
        )

        self.assertEqual(response.status_code, 200)
        self.assertIn('attachment;', response.headers.get('Content-Disposition', ''))
        self.assertIn('inventory_audit_', response.headers.get('Content-Disposition', ''))
        self.assertTrue(response.data.startswith(b'\xef\xbb\xbf'))


if __name__ == '__main__':
    unittest.main()
