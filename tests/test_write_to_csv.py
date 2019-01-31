import os

from ETL_Toy.data.data import Data
from ETL_Toy.jobs.steps import Write_To_CSV


def init():
    data = Data()
    write_to_csv = Write_To_CSV('tmp', 'OUTPUT_CSV', ';')
    data.columns_names = ['ID', 'NAME']
    data.add_row(['1', 'Andy'])
    data.add_row(['2', 'Peter'])
    write_to_csv.data = data
    dirname = os.path.dirname(__file__)
    write_to_csv.data_targets = os.path.join(dirname, 'fixtures\data_targets.config')

    return write_to_csv


def read_file(file):
    with open(file, 'r') as f:
        lines = f.readlines()
        output = []
        for line in lines:
            tmp = line.replace('\n', '')
            output.append(tmp)
        return output


def test_write():
    write_to_csv = init()
    write_to_csv.process()
    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, 'fixtures/csv_example_output.csv')
    assert read_file(path) == ['ID;NAME', '1;Andy', '2;Peter']
