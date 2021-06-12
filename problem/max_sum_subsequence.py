def find_max_sum_subsequence(houses: list[int]):
    number_of_houses = len(houses)
    max_sum_subsequence = list()

    max_sum_subsequence.insert(0, houses[0])
    max_sum_subsequence.insert(1, max(houses[0], houses[1]))

    for index in range(2, number_of_houses):
        max_sum_subsequence.insert(index, max(houses[index] + max_sum_subsequence[index-2], max_sum_subsequence[index-1]))

    return max_sum_subsequence[number_of_houses-1]
