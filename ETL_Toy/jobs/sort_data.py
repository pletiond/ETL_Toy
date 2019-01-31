import logging


class Sort_Data():
    """
    Sort dataset by selected column
    """
    def __init__(self, name, column):
        """
        :param name: Step name
        :param column: Sorting column
        """
        self.data = None
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.data_targets = None
        self.sorting_column = column

    def process(self):
        self.logger.info(f'Starting new sorting job - {self.name}!')

        if not self.sorting_column in self.data.columns_names:
            self.logger.info(f'{self.name} column doesnt exist!')
            raise Exception(f'{self.name} column doesnt exist!')

        self.data.sort_by_column(self.sorting_column)

        self.logger.info(
            f'Sort_data job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')
