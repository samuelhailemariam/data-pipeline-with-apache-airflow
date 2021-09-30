from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 sql='',
                 mode='append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} dimention table.')
            redshift_hook.run(f'DELETE FROM {self.table};')
            self.log.info('Deletion complete.')

        sql = """
            INSERT INTO {table}
            {sql}
        """.format(table=self.table, sql=self.sql)

        self.log.info(f'Executing INSERT INTO command on {self.table} dimentin table')
        redshift_hook.run(sql)
        self.log.info("Loading complete.")