import logging


class Read_csv:

    def __init__(self, name, file, delimiter):
        self.data = None
        self.file = file
        self.delimiter = delimiter
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.column_cnt = 0

    def process(self):
        self.logger.info(f'Starting new read_csv job - {self.name}!')
        first = True
        with open(self.file, 'r', encoding='utf8') as f:
            for line in f.readlines():
                if first:
                    self.parse_header(line)
                    first = False
                else:
                    self.parse_line(line)

        self.logger.info(
            f'Read_csv job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def parse_header(self, header):
        for column in str(header).replace('\n', '').rstrip().split(self.delimiter):
            self.data.add_column_name(column)
            self.column_cnt += 1

    def parse_line(self, line):
        parsed_line = str(line).replace('\n', '').rstrip().split(self.delimiter)
        if not len(parsed_line) == self.column_cnt:
            print('Len error')
            raise Exception('TO DO')

        self.data.add_row(parsed_line)
