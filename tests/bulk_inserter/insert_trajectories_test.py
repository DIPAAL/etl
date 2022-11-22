import pandas as pd

from etl.insert.insert_trajectories import TrajectoryInserter


def test_it_should_generate_random_unique_numbers():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})

    # mock the random.sample function
    def mock_sample(population, k):
        return [1, 2, 3, 4, 5]

    random_series = TrajectoryInserter.generate_unique_random_series(df, 10, mock_sample)

    assert len(random_series) == len(df)
    assert len(random_series.unique()) == len(df)


def test_it_resolves_conflicts_initially():
    df = pd.DataFrame({'a': [1, 2, 3, 4, 5]})

    # mock the random.sample function
    def mock_sample(population, k):
        # the first call requests 5 numbers, the second call request 1 number.
        if k == 5:
            return [1, 2, 3, 4, 4]
        return [5]

    random_series = TrajectoryInserter.generate_unique_random_series(df, 5, mock_sample)

    assert len(random_series) == len(df)
    assert len(random_series.unique()) == len(df)
