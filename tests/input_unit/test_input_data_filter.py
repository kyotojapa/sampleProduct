import unittest

from input_unit.input_data_filter import has_valid_object_id


class TestInputDataFilter(unittest.TestCase):

    def test_has_valid_object_id(self):
        self.assertTrue(has_valid_object_id({
            "snapshot": {
                "obj": "<SlaMtr!2840>[fvkau3-sla]"
            }
        }))

        self.assertTrue(has_valid_object_id({
            "snapshot": {
                "obj": "<IscSchedGrp!2880>[fvkau3]"
            }
        }))

        self.assertFalse(has_valid_object_id({
            "snapshot": {
                "obj": "<IscSchedGrp!2880>[fvkau3>prod2-bgp-ota]"
            }
        }))

        self.assertFalse(has_valid_object_id({
            "snapshot": {
                "obj": "<IscSchedGrp!2880>[fvkau3-prod2-bgp-ota]"
            }
        }))
