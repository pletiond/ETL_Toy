from ETL_Toy.jobs.transformation import Transformation
from ETL_Toy.jobs.dummy import Dummy
from ETL_Toy.jobs.read_csv import Read_csv
from ETL_Toy.jobs.write_to_csv import Write_To_CSV
from ETL_Toy.jobs.filter_columns import Filter_columns
from ETL_Toy.jobs.mapping import Mapping
from ETL_Toy.jobs.string_operations import String_operations
from ETL_Toy.jobs.read_excel import Read_excel
from ETL_Toy.jobs.write_to_excel import Write_To_Excel

import click
transformation = Transformation()

# read_csv = Read_csv('reader1', 'INPUT_CSV', ';')
# transformation.add_step(read_csv)

excel_reader = Read_excel('excel1', 'INPUT_EXCEL')
transformation.add_step(excel_reader)

mapping = Mapping('mapping1')
mapping.new_mapping('sex', 1, 'M')
mapping.new_mapping('sex', 0, 'Ž')
transformation.add_step(mapping)

so = String_operations('so1')

so.do_lower(['prijimeni'])
so.do_capitalize(['prijimeni'])
transformation.add_step(so)

# write_to_csv = Write_To_CSV('writer1', 'OUTPUT_CSV', '|')
# transformation.add_step(write_to_csv)
write_excel = Write_To_Excel('excel_write', 'OUTPUT_EXCEL')
transformation.add_step(write_excel)

# transformation.run()

transformation.save('C:\\Disk\\CVUT-FIT\\PYT\\mi-pyt-sem\\tests\\output.obj')


# output =transformation.load('C:\\Disk\\CVUT-FIT\\PYT\\mi-pyt-sem\\tests\\output.obj')

# output.run()

# TODO click + data sources

@click.command()
@click.option('--data_targets', '-d', default=None, show_default=True, help='File with data targets')
@click.option('--log', '-l', help='Target file for logging')
@click.argument('TARGET', nargs=-1)
def main(data_targets, log, target):
    if not len(target) == 1:
        raise Exception('Not valid number of targets')

    target_object = transformation.load(target[0])
    target_object.run(log, data_targets)


if __name__ == '__main__':
    main()
