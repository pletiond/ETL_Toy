import logging
import logging.config
import os
import pickle

import click

from ETL_Toy.data.data import Data


class Transformation:
    jobs = []

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.data_targets = None

    def add_step(self, step):
        self.jobs.append(step)

    def save(self, path):
        filehander = open(path, "wb")
        pickle.dump(self, filehander)
        filehander.close()

    @staticmethod
    def load(path):
        file = open(path, "rb")
        object_file = pickle.load(file)
        return object_file

    def run(self, log_path, data_targets=None):
        self.data_targets = data_targets
        self._set_logging(log_path)
        logging.info('ETL_Toy started!')
        curr_data = Data()
        try:

            for step in self.jobs:
                step.data = curr_data
                step.data_targets = self.data_targets
                step.process()
                curr_data = step.data

        except Exception as e:
            print(e.with_traceback())
            logging.info('ETL_Toy finished with error!')
            mess = click.style('ETL_Toy finished with error! More information in log file.', fg='red')
            click.echo(mess)
            exit(1)

        logging.info('ETL_Toy finished successfully!')
        mess = click.style('ETL_Toy finished successfully!', fg='green')
        click.echo(mess)

    def _set_logging(self, log_path):
        dirname = os.getcwd()
        path = os.path.join(dirname, log_path)
        print(path)
        logging.basicConfig(filename=path, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
