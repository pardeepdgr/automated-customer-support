import unittest
from problem.max_sum_subsequence import find_max_sum_subsequence


class MyTestCase(unittest.TestCase):
    def test_max_sum_subsequence_with_alternate_max_order(self):
        self.assertEqual(find_max_sum_subsequence([6, 7, 1, 3, 8, 2, 4]), 6 + 1 + 8 + 4)

    def test_max_sum_subsequence_with_random_max_order(self):
        self.assertEqual(find_max_sum_subsequence([5, 3, 4, 11, 2]), 5 + 11)


if __name__ == '__main__':
    unittest.main()
