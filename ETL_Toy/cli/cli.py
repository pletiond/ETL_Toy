from ETL_Toy.jobs.transformation import Transformation
from ETL_Toy.jobs.dummy import Dummy
from ETL_Toy.jobs.read_csv import Read_csv
from ETL_Toy.jobs.write_to_csv import Write_to_csv
from ETL_Toy.jobs.filter_columns import Filter_columns
from ETL_Toy.jobs.mapping import Mapping
from ETL_Toy.jobs.string_operations import String_operations

transformation = Transformation()

read_csv = Read_csv('reader1', 'C:\\Disk\\CVUT-FIT\\PYT\\mi-pyt-sem\\tests\\csv_example.csv', ';')
transformation.add_step(read_csv)

mapping = Mapping('mapping1')
mapping.new_mapping('sex', 1, 'M')
mapping.new_mapping('sex', 0, 'Ž')
transformation.add_step(mapping)

so = String_operations('so1')

so.do_lower(['prijimeni'])
so.do_capitalize(['prijimeni'])
transformation.add_step(so)

write_to_csv = Write_to_csv('writer1', 'C:\\Disk\\CVUT-FIT\\PYT\\mi-pyt-sem\\tests\\csv_example_output.csv', '|')
transformation.add_step(write_to_csv)

transformation.run()
