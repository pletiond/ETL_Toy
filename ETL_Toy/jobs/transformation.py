import logging
import logging.config
from ETL_Toy.data.data import Data


class Transformation:
    jobs = []

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def add_step(self, step):
        self.jobs.append(step)

    def save(self, path):
        ...

    def load(self, path):
        ...

    def run(self, log_path='../..'):
        self._set_logging(log_path)
        logging.info('ETL_Toy started!')
        curr_data = Data()
        try:

            for step in self.jobs:
                print(f'Step_name= {step.name}')
                step.data = curr_data
                step.process()
                curr_data = step.data
                print(curr_data.data)

        except Exception as e:
            # print(e.with_traceback())
            print('ERROR')
            raise e

        logging.info('ETL_Toy finished successfully!')

    def _set_logging(self, log_path):
        if not log_path == '':
            log_path += '/'

        logging.basicConfig(filename='../../ETL_Toy.log', level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
