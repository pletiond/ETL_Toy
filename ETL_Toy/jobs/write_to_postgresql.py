import logging
import configparser
import psycopg2


class Write_To_Postgresql:

    def __init__(self, name, data_target_name, table, schema='public'):
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
        self.logger.info(f'Starting new read_postgresql job - {self.name}!')
        self._load_credentials()
        try:
            self.conn = psycopg2.connect(
                f'dbname={self.dbname} user={self.user} port={self.port} host={self.host} password={self.password}')
        except:
            raise Exception('Unable to connect to database.')

        self._insert_data()

        self.logger.info(
            f'Read_postgresql job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _insert_data(self):
        cur = self.conn.cursor()
        for row in self.data.data:
            cur.execute(
                f'INSERT INTO {self.schema}.{self.table} ({self._prepare_header()}) VALUES ( {self._prepare_row(row)} )')

        self.conn.commit()
        cur.close()
        self.conn.close()

    def _prepare_row(self, row):
        output = ''
        for value in row:
            output += '\'' + str(value) + '\','
        print(output[:-1])
        return output[:-1]

    def _prepare_header(self):
        print(','.join(self.data.columns_names))
        return ','.join(self.data.columns_names)

    def _load_credentials(self):
        config = configparser.ConfigParser()
        config.read(self.data_targets)
        if not self.data_target_name in config.sections():
            raise Exception('Invalid data target name')
        if not 'dbname' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        if not 'host' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        if not 'port' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        if not 'user' in config[self.data_target_name]:
            raise Exception('Invalid data target type')
        if not 'password' in config[self.data_target_name]:
            raise Exception('Invalid data target type')

        self.dbname = config[self.data_target_name]['dbname']
        self.host = config[self.data_target_name]['host']
        self.port = config[self.data_target_name]['port']
        self.user = config[self.data_target_name]['user']
        self.password = config[self.data_target_name]['password']
