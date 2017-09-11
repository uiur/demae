from demae.util import split_size


def test_split_size():
    assert split_size(list(range(6)), 3) == [[0, 1], [2, 3], [4, 5]]
    assert split_size(list(range(7)), 3) == [[0, 1], [2, 3], [4, 5, 6]]
    assert split_size([], 3) == [[], [], []]
