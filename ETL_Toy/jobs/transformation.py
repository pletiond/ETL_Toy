import logging
import logging.config
import os
import click
import dill

from ETL_Toy.data.data import Data


class Transformation(object):
    """
    Spine of the application, contains all steps
    """
    jobs = []

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.data_targets = None


    def add_step(self, step):
        self.jobs.append(step)


    def save(self, path):
        """
        Save current transformation into file

        :param path: Path for saving

        """
        out = []
        out.append(self)
        out += self.jobs
        with open(path, 'wb') as f:
            dill.dump(out, f, recurse=True)


    @staticmethod
    def load(path):
        """
        Load saved transformation

        :param path: Path for  loading
        :return: loaded class

        """

        with open(path, 'rb') as pkl:
            f = dill.load(pkl)

            main = f[0]
            main.jobs = []
            for step in f[1:]:
                main.add_step(step)
            return main

    def run(self, log_path, data_targets=None):
        """
        Run transformation - all steps in order

        :param log_path: Path to log file
        :param data_targets: Path to data targets file

        """
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
        """
        Set logging  in selected file. Logging time,type of message, module and message
        :param log_path: Path to log file
        :return:
        """
        dirname = os.getcwd()
        path = os.path.join(dirname, log_path)
        logging.basicConfig(filename=path, level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
