from ETL_Toy.jobs.string_operations import String_operations
from ETL_Toy.data.data import Data


def init():
    so = String_operations('tmp')
    data = Data()
    data.columns_names = ['STREET', 'NAME', 'SURNAME']
    data.add_row(['nEw', 'AnDy', 'PLEticha'])
    data.add_row(['tMp', 'pEtEr', 'ranDOM'])
    data.add_row(['rAnDoM', 'geoRge', 'bUSh'])
    so.data = data
    return so


def test_lower():
    so = init()
    so.do_lower(['STREET'])
    so.process()

    assert [['new', 'AnDy', 'PLEticha'], ['tmp', 'pEtEr', 'ranDOM'],
            ['random', 'geoRge', 'bUSh']] == so.data.data.tolist()


def test_upper():
    so = init()
    so.do_upper(['NAME'])
    so.process()

    assert [['nEw', 'ANDY', 'PLEticha'], ['tMp', 'PETER', 'ranDOM'],
            ['rAnDoM', 'GEORGE', 'bUSh']] == so.data.data.tolist()


def test_capitalize():
    so = init()
    so.do_capitalize(['SURNAME'])
    so.process()

    assert [['nEw', 'AnDy', 'Pleticha'], ['tMp', 'pEtEr', 'Random'],
            ['rAnDoM', 'geoRge', 'Bush']] == so.data.data.tolist()
