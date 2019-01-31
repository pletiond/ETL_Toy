import logging


class Filter_columns:
    """
    This step filter column by whitelist or blacklist
    """
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
        """
        Remove selected columns
        :return: False if column doesnt exist
        """
        for column in self.remove:
            res = self.data.remove_column(column)
            if not res:
                self.logger.info(f'{self.name} - Column doesnt exist!')
                raise Exception('{self.name} - Column doesnt exist!')


    def _select_columns(self):
        """
        Select columns by whitelist, remove others
        """
        all_columns = list(self.data.columns_names)
        for column in all_columns:
            if column in self.allowed:
                continue
            self.data.remove_column(column)

