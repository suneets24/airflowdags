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
    'start_date': datetime(2017, 11, 8),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_currency_balance_dashboard',
                  default_args=default_args
                  )


				  
# Start running at this time
start_time_task = TimeSensor(target_time=time(6, 30),
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

		
insert_currency_balance_sql = """Insert overwrite as_s2.s2_currency_balances_dashboard 
with temp_currecny_balance as 
(
select a.*, coalesce(b.player_type, 'Active Non-Spender') as player_type  
from 
(
select distinct context_headers_title_id_s
  , context_data_mmp_transaction_id_s
,context_headers_source_s
,context_headers_env_s
,context_headers_uuid_s
,context_headers__event_type_s
,context_headers_user_id_s
,context_headers_timestamp_s
,currency_id_l
,balance_new_l
,balance_old_l 
,dt
from ads_ww2.fact_currency_balances_view 
where dt <= date '{{DS_DATE_ADD(0)}}'
and currency_id_l = 6 
) a 

join ads_ww2.fact_session_data c 
on a.context_headers_title_id_s = c.context_headers_title_id_s 
and a.context_headers_user_id_s = cast(c.client_user_id_l as varchar)

left join (select * from as_s2.s2_spenders_active_cohort_staging where raw_date = date '{{DS_DATE_ADD(0)}}' ) b 
on a.context_headers_title_id_s = b.title_id_s 
and a.context_headers_user_id_s = cast(b.client_user_id as varchar) 
-- Filter on Session Data and Spending cohort data
where c.dt = date '{{DS_DATE_ADD(0)}}'
)


-- Get currency balance for DAUs on that day 

select player_type, currency_id, avg(new_balance) as balance 
, count(distinct row(context_headers_title_id_s, context_headers_user_id_s)) as num_users 
,  date '{{DS_DATE_ADD(0)}}' as raw_date
from 
(
select player_type, context_headers_user_id_s, context_headers_title_id_s 

-- Only considered Armory Credits, Same can be expanded to get balance for all other currenceis 

, case when currency_id_l = 1 then 'XP' 
                 when currency_id_l = 2 then 'COD Points'
				 when currency_id_l = 3 then 'Clan Token'
				 when currency_id_l = 4 then 'Prestige Token'
				 when currency_id_l = 5 then 'Supply Key'
				 when currency_id_l = 6 then 'Armory Credit'
				 when currency_id_l = 7 then 'Social XP'
				 when currency_id_l = 8 then 'Double XP'
				 when currency_id_l = 9 then 'Double Division XP'
				 when currency_id_l = 10 then 'Double Weapon XP'
				 when currency_id_l = 11 then 'Double Loot'
				 when currency_id_l = 12 then 'Double XP Daily Time' end as currency_id 
			 
	, balance_new_l as new_balance 
	, row_number() over (partition by context_headers_title_id_s, context_headers_user_id_s, currency_id_l order by ts desc) as transact_rank
from 
(
select distinct dt, player_type  
, context_headers_title_id_s 
, context_headers_user_id_s 
, currency_id_l, balance_new_l , balance_old_l 
, to_unixtime(from_iso8601_timestamp(context_headers_timestamp_s)) as ts 
from temp_currecny_balance 
)
)
-- Get latest transaction till that day 
where transact_rank = 1 
group by 1,2,5
""" 

#### Please let me know if the task name is fine
insert_currency_balance_task = qubole_operator('daily_end_of_day_currency_balance',
                                              insert_currency_balance_sql, 2, timedelta(seconds=600), dag) 

# Wire up the DAG , Setting Dependency of the tasks
insert_currency_balance_task.set_upstream(start_time_task)
