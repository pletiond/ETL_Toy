import configparser
import logging

import psycopg2


class Write_To_Postgresql:
    """
    Insert data to selected dabatase table
    """

    def __init__(self, name, data_target_name, table, schema='public'):
        """
        :param name: Step name
        :param data_target_name: Data target name
        :param table: Selected table
        :param schema: Selcted table
        """
        self.data = None
        self.data_target_name = data_target_name
        self.logger = logging.getLogger(__name__)
        self.name = name
        self.data_targets = None
        self.conn = None
        self.host = None
        self.port = None
        self.user = None
        self.password = None
        self.schema = schema
        self.table = table

    def process(self):
        """
        Connect to database
        :return:
        """
        self.logger.info(f'Starting new read_postgresql job - {self.name}!')
        self._load_credentials()
        try:
            self.conn = psycopg2.connect(
                f'dbname={self.dbname} user={self.user} port={self.port} host={self.host} password={self.password}')
        except:
            self.logger.info(f'{self.name} - Unable to connect to database.')
            raise Exception('Unable to connect to database.')

        self._insert_data()

        self.logger.info(
            f'Read_postgresql job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _insert_data(self):
        """
        Insert all row into table and commit it
        """
        cur = self.conn.cursor()
        for row in self.data.data:
            cur.execute(
                f'INSERT INTO {self.schema}.{self.table} ({self._prepare_header()}) VALUES ( {self._prepare_row(row)} )')

        self.conn.commit()
        cur.close()
        self.conn.close()

    def _prepare_row(self, row):
        """
        Format row in sql format
        :param row:
        :return: formatted row
        """
        output = ''
        for value in row:
            output += '\'' + str(value) + '\','
        return output[:-1]

    def _prepare_header(self):
        """
        Format header  in sql format
        :return: formatted header
        """
        return ','.join(self.data.columns_names)

    def _load_credentials(self):
        """
        Using data target name load host, dbname, port and user
        """
        config = configparser.ConfigParser()
        config.read(self.data_targets)
        if not self.data_target_name in config.sections():
            raise Exception('Invalid data target name')
        if not 'dbname' in config[self.data_target_name]:
            self.logger.info(f'{self.name} - missing dbname value!')
            raise Exception('Invalid data target type')
        if not 'host' in config[self.data_target_name]:
            self.logger.info(f'{self.name} - missing host value!')
            raise Exception('Invalid data target type')
        if not 'port' in config[self.data_target_name]:
            self.logger.info(f'{self.name} - missing port value!')
            raise Exception('Invalid data target type')
        if not 'user' in config[self.data_target_name]:
            self.logger.info(f'{self.name} - missing user value!')
            raise Exception('Invalid data target type')
        if not 'password' in config[self.data_target_name]:
            self.logger.info(f'{self.name} - missing password value!')
            raise Exception('Invalid data target type')

        self.dbname = config[self.data_target_name]['dbname']
        self.host = config[self.data_target_name]['host']
        self.port = config[self.data_target_name]['port']
        self.user = config[self.data_target_name]['user']
        self.password = config[self.data_target_name]['password']
