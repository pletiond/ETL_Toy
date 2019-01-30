import logging
import psycopg2
import configparser


class Read_Postgresql:

    def __init__(self, name, data_target_name, table, schema='public', columns='*'):
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
        self.columns = columns

    def process(self):
        self.logger.info(f'Starting new read_postgresql job - {self.name}!')
        self._load_credentials()
        try:
            self.conn = psycopg2.connect(
                f'dbname={self.dbname} user={self.user} port={self.port} host={self.host} password={self.password}')
        except:
            raise Exception('Unable to connect to database.')

        self._load_data()

        self.logger.info(
            f'Read_postgresql job - {self.name} ended - {len(self.data.data)} lines, {len(self.data.columns_names)} columns.')

    def _load_data(self):
        cur = self.conn.cursor()
        cur.execute(f'SELECT {self.columns} FROM {self.schema}.{self.table}')
        colnames = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        print(colnames)
        for colname in colnames:
            self.data.add_column_name(colname, '')
        while row is not None:
            self.data.add_row(row)
            row = cur.fetchone()

        cur.close()
        self.conn.close()

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
