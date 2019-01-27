import logging
import numpy


class String_operations:

    def __init__(self, name):
        self.name = name
        self.data = None
        self.logger = logging.getLogger(__name__)
        self.transpone = None
        self.transpone_final = None
        self.upper = []
        self.lower = []
        self.capitalize = []

    def do_upper(self, columns):
        self.upper = columns

    def do_lower(self, columns):
        self.lower = columns

    def do_capitalize(self, columns):
        self.capitalize = columns

    def process(self):
        self.logger.info(f'Starting new string_operations job - {self.name}!')
        self.transpone = self.data.data.T
        self.transpone_final = self.data.data.T

        self._process_lower()
        self._process_upper()
        self._process_capitalize()

        self.data.data = self.transpone_final.T
        self.logger.info(
            f'String_operations job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _process_lower(self):
        for column in self.lower:
            index = self.data.columns_names.index(column)
            self.transpone_final[index] = numpy.char.lower(numpy.array(self.transpone[index]))

    def _process_upper(self):
        for column in self.upper:
            index = self.data.columns_names.index(column)
            self.transpone_final[index] = numpy.char.upper(numpy.array(self.transpone[index]))

    def _process_capitalize(self):
        for column in self.capitalize:
            index = self.data.columns_names.index(column)
            self.transpone_final[index] = numpy.char.capitalize(numpy.array(self.transpone[index]))
