from ETL_Toy.jobs.transformation import Transformation
from ETL_Toy.jobs.sort_data import Sort_Data
from ETL_Toy.jobs.filter_columns import Filter_columns
from ETL_Toy.jobs.string_operations import String_operations
import os


def test_save_load():
    trans = Transformation()
    trans.add_step(Sort_Data('tmp1', ''))
    trans.add_step(Filter_columns('tmp2'))
    trans.add_step(String_operations('tmp3'))

    dirname = os.path.dirname(__file__)
    path = os.path.join(dirname, 'fixtures/test_save.obj')
    trans.save(path)

    trans_loaded = Transformation.load(path)

    assert type(trans_loaded) == type(trans)
    assert len(trans_loaded.jobs) == 3
    assert trans_loaded.jobs[0].name == 'tmp1'
    assert trans_loaded.jobs[1].name == 'tmp2'
    assert trans_loaded.jobs[2].name == 'tmp3'
