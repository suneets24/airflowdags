## DAG for creating a staging table for identifying player_type for spenders 
## Create Table Statement - Create table as_s2.s2_spenders_active_cohort_staging (title_id int, network_id int, client_user_id bigint) partitioned by (raw_date date) stored as ORC;

import airflow
from datetime import timedelta, datetime, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor
from quboleWrapper import qubole_wrapper, export_to_rdms

query_type = 'dev_presto'

# Set expected runtime in seconds, setting to 0 is 7200 seconds
expected_runtime = 0

# The group that owns this DAG
owner = "Analytic Services"

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 21),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_spenders_active_cohort_staging',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(4, 00),
                             task_id='start_time_task',
                             dag=dag
                             )


def qubole_operator(task_id, sql, retries, retry_delay, dag):
    return PythonOperator(
        task_id=task_id,
        python_callable=qubole_wrapper,
        provide_context=True,
        retries=retries,
        retry_delay=retry_delay,
        # schedule_interval=None,
        pool='presto_default_pool',
        op_kwargs={'db_type': query_type,
                   'raw_sql': sql,
                   'expected_runtime': expected_runtime,
                   'dag_id': dag.dag_id,
                   'task_id': task_id
                   },
        templates_dict={'ds': '{{ ds }}'},
        dag=dag)

insert_active_cohorts_sql = '''Insert overwrite as_s2.s2_spenders_active_cohort_staging 
with player_cohorts as	
(
	SELECT DISTINCT a.context_headers_title_id_s 
	, a.network_id 
	, a.client_user_id_l 
	, b.g_rev 
	, b.player_type
	, a.dt 
	FROM ads_ww2.fact_session_data a 
	JOIN 
	( SELECT network_id, context_headers_user_id_s,g_rev 
	, case when player_ntile <= 1 then 'Active Superwhale' 
			when player_ntile <= 20 then 'Active Whale' 
			when player_ntile <= 50 then 'Active Dolphin' 
			else 'Active Minnow' end as player_type 

	FROM 
	(
		SELECT a.network_id, a.context_headers_user_id_s, g_rev, ntile(100) over (order by g_rev desc) as player_ntile 
		FROM 
			( SELECT context_headers_user_id_s 
			, network_id 
			, SUM(price_usd) as g_rev 
			FROM  as_s2.ww2_mtx_consumables_staging 
			WHERE dt <= date '{{DS_DATE_ADD(0)}}'
			GROUP BY 1,2 
			) a 
		JOIN 
		   (SELECT DISTINCT network_id, client_user_id_l 
			FROM ads_ww2.fact_session_data 
			WHERE dt between date '{{DS_DATE_ADD(-6)}}' AND date '{{DS_DATE_ADD(0)}}'
			    ) b 
		ON a.network_id = b.network_id 
		AND a.context_headers_user_id_s = cast(b.client_user_id_l as varchar)
		
		) 
	) b 
	ON a.network_id = b.network_id 
	AND cast(a.client_user_id_l as VARCHAR) = b.context_headers_user_id_s 
	WHERE a.dt = date '{{DS_DATE_ADD(0)}}'
) 

select * from player_cohorts'''
insert_active_cohorts_task = qubole_operator('s2_spenders_active_cohort_staging',
                                              insert_active_cohorts_sql, 4, timedelta(seconds=300), dag) 

# Wire up the DAG , Setting Dependency of the tasks
insert_active_cohorts_task.set_upstream(start_time_task)