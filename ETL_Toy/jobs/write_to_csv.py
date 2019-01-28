import logging
import configparser


class Write_To_CSV:

    def __init__(self, name, data_target_name, delimiter):
        self.name = name
        self.data_target_name = data_target_name
        self.data_targets = None
        self.delimiter = delimiter
        self.data = None
        self.logger = logging.getLogger(__name__)

    def process(self):
        self.logger.info(f'Starting new write_to_csv job - {self.name}!')
        file = self._get_file()
        with open(file, 'w+', encoding='utf8') as f:
            header = str(self.delimiter).join(self.data.columns_names)
            f.write(header + '\n')
            for line in self.data.data:
                f.write(str(self.delimiter).join(line) + '\n')

        self.logger.info(
            f'Write_to_csv job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _get_file(self):
        config = configparser.ConfigParser()
        config.read(self.data_targets)
        if not self.data_target_name in config.sections():
            raise Exception('Invalid data target')
        if not 'file' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        return config[self.data_target_name]['file']
