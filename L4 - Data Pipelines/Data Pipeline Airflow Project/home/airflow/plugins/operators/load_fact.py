from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 songplay_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.songplay_sql = songplay_sql

    def execute(self, context):
        self.log.info('LoadFactOperator implementation Started')
        
        # Create a redshift postgres connection hook
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        print(redshift)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Insert to songplays Started")
        redshift.run(self.songplay_sql)
        self.log.info("Insert to songplays Finished")
                
