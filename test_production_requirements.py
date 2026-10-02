import unittest

from blueprints.production import _build_material_requirement_product_box_map


class ProductionRequirementsTestCase(unittest.TestCase):
    def test_unfinished_schedules_are_summed_and_completed_schedules_are_excluded(self):
        schedules = [
            {'product_id': 140, 'planned_boxes': 350, 'status': '예정'},
            {'product_id': 140, 'planned_boxes': 350, 'status': '진행중'},
            {'product_id': 140, 'planned_boxes': 350, 'status': '예정'},
            {'product_id': 140, 'planned_boxes': 999, 'status': '완료'},
        ]

        self.assertEqual(_build_material_requirement_product_box_map(schedules), {140: 1050.0})


if __name__ == '__main__':
    unittest.main()
