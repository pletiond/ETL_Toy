from ETL_Toy.data.data import Data
from ETL_Toy.jobs.steps import Sort_Data


def init():
    sort_data = Sort_Data('tmp', '')
    data = Data()
    data.columns_names = ['ID', 'SALARY']
    data.add_row(['1', 999.9])
    data.add_row(['3', 99.9])
    data.add_row(['2', 9.9])
    sort_data.data = data
    return sort_data


def test_sort_ID():
    sort_data = init()
    sort_data.sorting_column = 'ID'
    sort_data.process()

    assert [['1', '999.9'], ['2', '9.9'], ['3', '99.9']] == sort_data.data.data.tolist()


def test_sort_SALARY():
    sort_data = init()
    sort_data.sorting_column = 'SALARY'
    sort_data.process()

    assert [['2', '9.9'], ['3', '99.9'], ['1', '999.9']] == sort_data.data.data.tolist()
