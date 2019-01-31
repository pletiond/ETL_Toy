from ETL_Toy.jobs.read_csv import Read_csv
from ETL_Toy.data.data import Data
import os


def test_read_correct():
    read_csv = Read_csv('tmp', 'INPUT_CSV', ';')
    dirname = os.path.dirname(__file__)
    read_csv.data_targets = os.path.join(dirname, 'fixtures\data_targets.config')
    read_csv.data = Data()
    read_csv.process()

    assert [['Adam', 'Novák', '11', '1'], ['Petr', 'Novotný', '31', '1'], ['Ondra', 'Pleticha', '24', '1'],
            ['Zuzana', 'Hánová', '23', '0']] == read_csv.data.data.tolist()


def test_read_incorrect():
    read_csv = Read_csv('tmp', 'INPUT_CSV', '|')
    dirname = os.path.dirname(__file__)
    read_csv.data_targets = os.path.join(dirname, 'fixtures\data_targets.config')
    read_csv.data = Data()
    read_csv.process()

    assert not [['Adam', 'Novák', '11', '1'], ['Petr', 'Novotný', '31', '1'], ['Ondra', 'Pleticha', '24', '1'],
                ['Zuzana', 'Hánová', '23', '0']] == read_csv.data.data.tolist()
