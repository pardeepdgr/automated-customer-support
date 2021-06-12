import unittest
from problem.biggest_two_digit_fragment import find_biggest_two_digit_fragment


class MyTestCase(unittest.TestCase):

    def test_biggest_two_digit_fragment(self):
        self.assertEqual(find_biggest_two_digit_fragment("9099298"), 99)

    def test_biggest_two_digit_fragment_when_single_digit_passed(self):
        self.assertEqual(find_biggest_two_digit_fragment("1"), 1)


if __name__ == '__main__':
    unittest.main()
