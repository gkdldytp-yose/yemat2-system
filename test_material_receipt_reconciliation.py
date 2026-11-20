import unittest

from core import reconcile_material_receipts


class ReceiptReconciliationTest(unittest.TestCase):
    def test_repeated_edits_and_cancellation_preserve_other_receipts(self):
        receipts = [
            {'log_id': 1, 'material_lot_id': 1, 'action': 'issue_request_complete', 'received_quantity': 100},
            {'log_id': 2, 'material_lot_id': 1, 'action': 'issue_request_complete', 'received_quantity': 20},
            {'log_id': 4, 'material_lot_id': 1, 'action': 'issue_request_update', 'received_quantity': 60},
            {'log_id': 6, 'material_lot_id': 2, 'action': 'issue_request_update', 'received_quantity': 40},
        ]
        cancellations = [
            {'log_id': 3, 'material_lot_id': 1, 'quantity': 100},
            {'log_id': 5, 'material_lot_id': 1, 'quantity': 60},
        ]
        result = reconcile_material_receipts(receipts, cancellations)
        self.assertEqual([(row['log_id'], row['received_quantity']) for row in result], [(2, 20), (6, 40)])
        self.assertEqual(receipts[0]['received_quantity'], 100)

    def test_partial_cancellation(self):
        receipts = [{'log_id': 1, 'material_lot_id': 1, 'action': 'issue_request_complete', 'received_quantity': 100}]
        cancellations = [{'log_id': 2, 'material_lot_id': 1, 'quantity': 30}]
        self.assertEqual(reconcile_material_receipts(receipts, cancellations)[0]['received_quantity'], 70)


if __name__ == '__main__':
    unittest.main()
