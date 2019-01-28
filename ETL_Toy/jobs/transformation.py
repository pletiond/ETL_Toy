import logging
import logging.config
import pickle
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

    def run(self, log_path='../..', data_targets=None):
        self.data_targets = data_targets
        self._set_logging(log_path)
        logging.info('ETL_Toy started!')
        curr_data = Data()
        try:

            for step in self.jobs:
                print(f'Step_name= {step.name}')
                step.data = curr_data
                step.data_targets = self.data_targets
                step.process()
                curr_data = step.data

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
