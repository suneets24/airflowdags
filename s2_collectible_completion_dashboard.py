import airflow
from datetime import timedelta, datetime, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor 
from airflow.operators.sensors import ExternalTaskSensor
from quboleWrapper import qubole_wrapper, export_to_rdms

query_type = 'dev_presto'

# Set expected runtime in seconds, setting to 0 is 7200 seconds
expected_runtime = 0

# The group that owns this DAG
owner = "Analytic Services"

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2017, 12, 15),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_collectible_completion_dashboard_cohort',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(7, 00),
                             task_id='start_time_task',
                             dag=dag
                             )

current_date = (datetime.now()).date()
stats_date = current_date - timedelta(days=1)

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

		
insert_collectible_completion_sql = """Insert overwrite table as_s2.s2_collectible_completion_dashboard 
with collectible_items as
(
select collectionrewardid
		, regexp_replace(collection_name, 'MPUI_COLLECTION_', '') as collection_name
		, loot_id,loot_group
		, name
		, reference
		, description
		, rarity 
		, collection_type
		, case when productionlevel in ('Gold', 'TU1') then 'Launch Collection' 
               when productionlevel in ('MTX1') then 'Winter Collection' else productionlevel end as productionlevel

from as_s2.loot_v4_ext a 
where productionlevel in ('Gold', 'TU1', 'MTX1')
and collectionrewardid <> loot_id
and collectionid > 0 
AND trim(isloot) <> ''
and category in ('emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon') 
group by 1,2,3,4,5,6,7,8,9,10
), 

loot_cross as
(
select collectionrewardid 
, loot_id 
		, collection_name
		, productionlevel
		, collection_type
		, sum(1) over (partition by collectionrewardid) as pool_size
from collectible_items
group by 1,2,3,4,5
), 

player_cohorts as	
(
	SELECT DISTINCT a.context_headers_title_id_s, a.network_id 
	, a.client_user_id_l 
	, case when a.dt < date('2017-11-21') then 'Active Non-Spender' else coalesce(b.player_type, 'Active Non-Spender') end as player_type 
	, a.dt 
	FROM ads_ww2.fact_session_data a 
	left JOIN 
	(select * from as_s2.s2_spenders_active_cohort_staging where raw_date = date '{{DS_DATE_ADD(0)}}') b 
	ON a.network_id = b.network_id 
	AND a.client_user_id_l = b.client_user_id 
	WHERE a.dt = date '{{DS_DATE_ADD(0)}}'
),

temp_inventory_data as 
(
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_awardproduct_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_consumeawards_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, 0, 0 , dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_eventinfo_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_durableprocess_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_durablerevoke_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_pawnitems_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
union all 
select context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
from ads_ww2.fact_mkt_purchaseskus_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
)

select y.player_type
--		, y.collection_type
		, y.productionlevel
		, z.unique_users
		, sum(case when y.num_items = y.pool_size then 1 else 0 end) as num_items 
		, z.dt as raw_date 
from 

(
select player_type, context_headers_user_id_s, collection_name, collection_type, productionlevel, pool_size,  sum(num_items) as num_items 
from 

(

select a.context_headers_user_id_s, c.player_type, b.collectionrewardid, b.collection_name, b.pool_size, b.collection_type, b.productionlevel, count(distinct a.item_id_l) as num_items 
, sum(quantity_new_l - quantity_old_l) as quantity_tot 

from 
temp_inventory_data a 
join loot_cross b 
    on a.item_id_l = b.loot_id 
join player_cohorts	 c 
    on a.context_headers_title_id_s = c.context_headers_title_id_s 
    and a.context_headers_user_id_s = cast(c.client_user_id_l as varchar)
group by 1,2,3,4,5,6,7

) 

group by 1,2,3,4,5,6

) y 


join 
( 
select dt, player_type, sum(unique_users) as unique_users 
from 
(
select dt, context_headers_title_id_s, player_type, count(distinct client_user_id_l) as unique_users 
from player_cohorts 
group by 1,2,3  
    )
group by 1,2  
    ) Z 
on y.player_type = z.player_type
group by 1,2,3,5 """ 

insert_collectible_completion_task = qubole_operator('s2_collectible_completion_dashboard',
                                              insert_collectible_completion_sql, 2, timedelta(seconds=600), dag) 

# Wire up the DAG , Setting Dependency of the tasks
##player_cohort_dependency_task.set_upstream(start_time_task)
insert_collectible_completion_task.set_upstream(start_time_task)
