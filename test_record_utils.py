import unittest
from record_utils import RecordUtils


class TestRecordUtils(unittest.TestCase):

    def test_find_largest_ids_with_valid_data(self):

        data_lines = [
            "101 50",
            "102 30",
            "103 70",  # largest value2
            "104 20",
            "105 60",  # largest value3
            "106 40",
            "107 90",  # largest value1
        ]

        top_x = 3
        expected_result = [107, 103, 105]
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)

    def test_find_largest_ids_with_negative_values(self):
        data_lines = [
            "101 -50",
            "102 -30",  # largest value2
            "103 -70",
            "104 -20",  # largest value1
            "105 -60",
            "106 -40",  # largest value3
            "107 -90",
        ]
        top_x = 3
        # Negative values are also considered in the heap
        expected_result = [104, 102, 106]
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)

    def test_find_largest_ids_with_invalid_data(self):
        data_lines = [
            "101 50",
            "102",     # Invalid line with missing value
            "103 70",
            "ABC 80",  # Invalid line with non-integer id
            "105 60",
            "106 DE",  # Invalid line with non-integer value
            "    90",  # Invalid line with missing id
        ]
        top_x = 3
        # Ignoring invalid lines, only considering valid ones
        expected_result = [103, 105, 101]
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)

    def test_find_largest_ids_with_fewer_records_than_top_x(self):
        data_lines = [
            "101 50",
            "102 30",
            "103 70",
            "104 20",
            "105 60",
            "106 40",
        ]
        top_x = 10  # More than the number of records available
        expected_result = [103, 105, 101, 106, 102, 104]
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)

    def test_find_largest_ids_with_duplicate_values(self):
        data_lines = [
            "101 50",
            "102 30",
            "103 70",  # largest value1
            "104 20",
            "105 60",  # largest value3
            "106 40",
            "107 70",  # largest value2 (Duplicate value with record 103)
        ]
        top_x = 3
        # Ignoring duplicates, considering only unique values
        expected_result = [103, 107, 105]
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)

    def test_find_largest_ids_with_empty_input(self):
        data_lines = []
        top_x = 3
        expected_result = []  # No records, so the result should be an empty list
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)

    def test_find_largest_ids_with_zero_top_x(self):
        data_lines = [
            "101 50",
            "102 30",
            "103 70",
            "104 20",
            "105 60",
            "106 40",
            "107 90",
        ]
        top_x = 0
        expected_result = []  # 0 top x, so the result should be an empty list
        result = RecordUtils.find_largest_ids_parallel(data_lines, top_x)
        self.assertEqual(result, expected_result)
