import logging


class Write_to_csv:

    def __init__(self, name, file, delimiter):
        self.name = name
        self.file = file
        self.delimiter = delimiter
        self.data = None
        self.logger = logging.getLogger(__name__)

    def process(self):
        self.logger.info(f'Starting new write_to_csv job - {self.name}!')
        with open(self.file, 'w+', encoding='utf8') as f:
            header = str(self.delimiter).join(self.data.columns_names)
            f.write(header + '\n')
            for line in self.data.data:
                f.write(str(self.delimiter).join(line) + '\n')

        self.logger.info(
            f'Write_to_csv job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')
