from ETL_Toy.data.data import Data
from ETL_Toy.jobs.steps import Mapping


def init():
    mapping = Mapping('tmp')
    data = Data()
    data.columns_names = ['NAME', 'SEX', 'FACULTY']
    data.add_row(['Peter', '1', '10'])
    data.add_row(['George', '1', '5'])
    data.add_row(['Kate', '0', '10'])
    mapping.data = data
    return mapping


def test_mapping_1():
    mapping = init()
    mapping.new_mapping('SEX', '1', 'MALE')
    mapping.new_mapping('SEX', '0', 'FEMALE')
    mapping.process()
    assert [['Peter', 'MALE', '10'], ['George', 'MALE', '5'], ['Kate', 'FEMALE', '10']] == mapping.data.data.tolist()


def test_mapping_2():
    mapping = init()
    mapping.new_mapping('FACULTY', '10', 'FIT')
    mapping.new_mapping('FACULTY', '5', 'FEL')
    mapping.new_mapping('FACULTY', '1', 'FS')
    mapping.new_mapping('FACULTY', '2', 'ARCH')
    mapping.process()
    assert not [[]] == mapping.data.data.tolist()
    assert [['Peter', '1', 'FIT'], ['George', '1', 'FEL'], ['Kate', '0', 'FIT']] == mapping.data.data.tolist()
