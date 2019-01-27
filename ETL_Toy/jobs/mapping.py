import logging
import numpy


class Mapping:

    def __init__(self, name):
        self.name = name
        self.data = None
        self.logger = logging.getLogger(__name__)
        self.rules = dict()
        self.transpone = None
        self.transpone_final = None

    def new_mapping(self, column, old, new):
        if not column in self.rules:
            self.rules[column] = {}
        self.rules[column][old] = new

    def process(self):
        self.logger.info(f'Starting new mapping job - {self.name}!')

        self.transpone = self.data.data.T
        self.transpone_final = self.data.data.T

        for column, rules in self.rules.items():
            self._apply_rules(column, rules)

        self.data.data = self.transpone_final.T
        print(self.transpone_final)
        self.logger.info(
            f'mapping job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _apply_rules(self, column, rules):
        for i in range(len(self.data.columns_names)):
            if not column == self.data.columns_names[i]:
                continue

            orig = self.transpone[i]

            for old, new in rules.items():
                for k in range(len(self.transpone[i])):
                    if str(orig[k]) == str(old):
                        orig[k] = new

            self.transpone_final[i] = orig
