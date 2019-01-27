import logging


class Dummy:

    def __init__(self, name):
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.data = None

    def process(self):
        self.logger.info(f'Starting new dummy job - {self.name}!')
        self.logger.info(
            f'Dummy job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')
