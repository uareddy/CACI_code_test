from unittest import TestCase

import creditData


class TestCreditcarddata(TestCase):

    def test_credit_card_checker(self):
        test_input_visa = '4829769584081989'
        test_input_wrong = '9829769584081989'
        results_visa = creditData.credit_card_checker(test_input_visa)
        expected_results_visa = 'Visa'
        self.assertEqual(results_visa, expected_results_visa)
        results_wrong = creditData.credit_card_checker(test_input_wrong)
        expected_results_wrong = 'Wrong_Format'
        self.assertEqual(results_wrong, expected_results_wrong)
    def test_mask(self):
        test_input_mask = '4829769584081989'
        n = 9
        result_mask = creditData.mask(test_input_mask,n)
        expected_results_mask = '4829769*********'
        self.assertEqual(result_mask, expected_results_mask)
    def test_size_byte(self):
        test_input_size = 'AZ'
        result_size = creditData.size_byte(test_input_size)
        expected_results_size = '51'
        self.assertEqual(result_size, expected_results_size)

