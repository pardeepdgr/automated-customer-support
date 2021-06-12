def find_biggest_two_digit_fragment(digits: str):
    number_of_digits = len(digits)
    if number_of_digits < 2:
        return int(digits)

    digit_fragments = []
    for index, digit in enumerate(digits):
        if index < number_of_digits - 1:
            digit_fragments.append(int(digits[index:index + 2]))
    return max(digit_fragments)
