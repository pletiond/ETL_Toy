import logging
import configparser
from openpyxl import load_workbook


class Read_excel:

    def __init__(self, name, data_target_name):
        self.data = None
        self.data_target_name = data_target_name
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.data_targets = None
        self.column_cnt = 0

    def process(self):
        self.logger.info(f'Starting new read_excel job - {self.name}!')
        file, sheet = self._get_file_and_sheet()
        wb = load_workbook(filename=file, read_only=True)
        ws = wb[sheet]
        header = True
        for row in ws.rows:
            if header:
                self._parse_header(row)
                header = False
            else:
                self._parse_row(row)

        self.logger.info(
            f'Read_excel job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _get_file_and_sheet(self):

        config = configparser.ConfigParser()
        config.read(self.data_targets)
        if not self.data_target_name in config.sections():
            raise Exception('Invalid data target')
        if not 'file' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        if not 'sheet' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        return config[self.data_target_name]['file'], config[self.data_target_name]['sheet']

    def _parse_header(self, row):
        for cell in row:
            self.data.add_column_name(cell.value)
            self.column_cnt += 1

    def _parse_row(self, row):
        line = []
        for cell in row:
            line.append(cell.value)
        if not len(line) == self.column_cnt:
            raise Exception('Different row lenght')
        else:
            self.data.add_row(line)
