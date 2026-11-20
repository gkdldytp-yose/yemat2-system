import unittest

from blueprints.production import _build_material_requirement_product_box_map, _hide_unused_removed_bom_usage


class ProductionRequirementsTestCase(unittest.TestCase):
    def test_unfinished_schedules_are_summed_and_completed_schedules_are_excluded(self):
        schedules = [
            {'product_id': 140, 'planned_boxes': 350, 'status': '예정'},
            {'product_id': 140, 'planned_boxes': 350, 'status': '진행중'},
            {'product_id': 140, 'planned_boxes': 350, 'status': '예정'},
            {'product_id': 140, 'planned_boxes': 999, 'status': '완료'},
        ]

        self.assertEqual(_build_material_requirement_product_box_map(schedules), {140: 1050.0})

    def test_removed_bom_hides_unused_estimates_but_keeps_actual_history(self):
        rows = [
            {'id': 1, 'material_id': 10, 'actual_quantity': None},
            {'id': 2, 'material_id': 11, 'actual_quantity': 3},
            {'id': 3, 'material_id': 12, 'actual_quantity': 0},
            {'id': 4, 'raw_material_id': 7, 'actual_quantity': 5},
        ]
        result = _hide_unused_removed_bom_usage(rows, [1, 2])
        self.assertEqual([row['id'] for row in result], [2, 3, 4])


if __name__ == '__main__':
    unittest.main()
