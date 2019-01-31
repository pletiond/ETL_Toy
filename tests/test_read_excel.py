from ETL_Toy.jobs.read_excel import Read_excel
from ETL_Toy.data.data import Data
import os


def test_read():
    read_excel = Read_excel('tmp', 'INPUT_EXCEL')
    dirname = os.path.dirname(__file__)
    read_excel.data_targets = os.path.join(dirname, 'fixtures\data_targets.config')
    read_excel.data = Data()
    read_excel.process()

    assert [['Adam', 'Novák', '11', '1'], ['Petr', 'Novotný', '31', '1'], ['Ondra', 'Pleticha', '24', '1'],
            ['Zuzana', 'Hánová', '23', '0']] == read_excel.data.data.tolist()
