import configparser
import logging


class Read_csv:
    """
    Read file and load data
    """
    def __init__(self, name, data_target_name, delimiter):
        """
        :param name: Step name
        :param data_target_name: Name of data target
        :param delimiter: CSV delimiter
        """
        self.data = None
        self.data_target_name = data_target_name
        self.delimiter = delimiter
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.column_cnt = 0
        self.data_targets = None

    def process(self):
        self.logger.info(f'Starting new read_csv job - {self.name}!')

        if len(self.data.columns_names) > 0:
            self.logger.info(f'{self.name} not empty input data!')
            raise Exception(f'{self.name} not empty input data!')

        first = True
        file = self._get_file()
        with open(file, 'r', encoding='utf8') as f:
            for line in f.readlines():
                if first:
                    self._parse_header(line)
                    first = False
                else:
                    self._parse_line(line)

        self.logger.info(
            f'Read_csv job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _parse_header(self, header):
        """
        Parse  header and add column names to data class
        :param header: Header line
        """
        for column in str(header).replace('\n', '').rstrip().split(self.delimiter):
            self.data.add_column_name(column)
            self.column_cnt += 1

    def _parse_line(self, line):
        """
        Parse line with values
        :param line: Value line
        """
        parsed_line = str(line).replace('\n', '').rstrip().split(self.delimiter)
        if not len(parsed_line) == self.column_cnt:
            self.logger.info(f'{self.name} - header and row lenght not match!')
            raise Exception(f'{self.name} - header and row lenght not match!')

        self.data.add_row(parsed_line)

    def _get_file(self):
        """
        Using data target name find path to csv file
        """
        config = configparser.ConfigParser()
        config.read(self.data_targets)
        if not self.data_target_name in config.sections():
            self.logger.info(f'{self.name} - Invalid data target name')
            raise Exception('Invalid data target name')
        if not 'file' in config[self.data_target_name]:
            self.logger.info(f'{self.name} - Invalid data target type')
            raise Exception('Invalid data target type')
        return config[self.data_target_name]['file']
