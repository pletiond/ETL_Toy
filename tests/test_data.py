from ETL_Toy.data.data import Data


def test_add_row():
    data = Data()
    data.columns_names = ['ID', 'NAME']
    data.add_row(['1', 'Andy'])
    data.add_row(['2', 'Peter'])
    assert len(data.data.tolist()) == 2
    assert data.data.tolist() == [['1', 'Andy'], ['2', 'Peter']]


def test_remove_column():
    data = Data()
    data.columns_names = ['ID', 'NAME']
    data.add_row(['1', 'Andy'])
    data.add_row(['2', 'Peter'])
    data.remove_column('NAME')

    assert data.data.tolist() == [['1'], ['2']]
