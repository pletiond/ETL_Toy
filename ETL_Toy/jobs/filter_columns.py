import logging


class Filter_columns:

    def __init__(self, name):
        self.name = name
        self.allowed = None
        self.remove = None
        self.data = None
        self.logger = logging.getLogger(__name__)

    def set_columns(self, names):
        self.allowed = names

    def remove_columns(self, names):
        self.remove = names

    def process(self):
        self.logger.info(f'Starting new filter_columns job - {self.name}!')

        if self.allowed:
            self._select_columns()
        elif self.remove:
            self._only_remove()

        self.logger.info(
            f'Filter_columns job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _only_remove(self):
        print('ONLY REMOVE!!!')
        for column in self.remove:
            self.data.remove_column(column)

    def _select_columns(self):
        all_columns = list(self.data.columns_names)
        for column in all_columns:
            print(column)
            if column in self.allowed:
                continue
            self.data.remove_column(column)
