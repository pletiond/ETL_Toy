from ETL_Toy.data.data import Data
from ETL_Toy.jobs.steps import Filter_Columns


def init():
    filter_col = Filter_Columns('tmp')
    data = Data()
    data.columns_names = ['ID', 'NAME', 'SURNAME']
    data.add_row(['1', 'Andy', 'Pleticha'])
    data.add_row(['2', 'Peter', 'Random'])
    data.add_row(['3', 'George', 'Bush'])
    filter_col.data = data
    return filter_col


def test_only_allowed():
    filter_col = init()
    filter_col.set_columns(['ID'])
    filter_col.process()
    assert [['1'], ['2'], ['3']] == list(filter_col.data.data)


def test_only_remove():
    filter_col = init()
    filter_col.remove_columns(['SURNAME'])
    filter_col.process()
    assert [['1', 'Andy'], ['2', 'Peter'], ['3', 'George']] == filter_col.data.data.tolist()
