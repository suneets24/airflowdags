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
    'start_date': datetime(2017, 12, 22),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_collectible_completion_item_level',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(7, 00),
                             task_id='start_time_task',
                             dag=dag
                             )

##current_date = (datetime.now()).date()
##stats_date = current_date - timedelta(days=1)

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

		
insert_collectible_completion_item_sql = """Insert overwrite as_s2.s2_collectible_item_acquisition_dashboard 

-- Filter Collecctible Items From Lootrest data 

with collectible_items as
(
select collectionrewardid
		, regexp_replace(regexp_replace(collection_name, 'MPUI_COLLECTION_', ''), 'LOOT_MTX1_COLLECTION_', '') as collection_name
		, loot_id
		, loot_group
		, name
		, reference
		, description
		, rarity 
		, collection_type 
		, case when productionlevel in ('Gold', 'TU1') then 'Launch Collection' 
               when productionlevel in ('MTX1') then 'Winter Collection' else productionlevel end as productionlevel
        , category 
from as_s2.loot_v4_ext a 
where productionlevel in ('Gold', 'TU1', 'MTX1')
and collectionrewardid <> loot_id
and collectionid > 0 
AND trim(isloot) <> ''
and category in ('emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon') 
group by 1,2,3,4,5,6,7,8,9,10,11
), 


-- Get Player Cohort 
player_cohorts as	
(
	SELECT DISTINCT a.context_headers_title_id_s, a.network_id 
	, a.client_user_id_l 
	, case when a.dt < date('2017-11-21') then 'Active Non-Spender' else coalesce(b.player_type, 'Active Non-Spender') end as player_type 
	, a.dt 
	FROM ads_ww2.fact_session_data a 
	left JOIN 
	( select * from as_s2.s2_spenders_active_cohort_staging 
	where raw_date = date '{{DS_DATE_ADD(0)}}' -- Confirm this table name
	) b 
	ON a.network_id = b.network_id 
	AND a.client_user_id_l = b.client_user_id 
	WHERE a.dt = date '{{DS_DATE_ADD(0)}}'
),

-- Combine all the separate Inventory table 
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
        , y.collection_name
		, y.collection_type
		, y.productionlevel
		, 1 as pool_size
		, z.unique_users 
		, count(distinct row(context_headers_title_id_s, context_headers_user_id_s))  as num_items 
		, count(distinct row(context_headers_title_id_s, context_headers_user_id_s))*pow(z.unique_users, -1) as avg_of_items
		, y.category
		, y.reference as item_name1
		, y.description as item_name2
		, z.dt as raw_date 
from 


(

select distinct a.context_headers_title_id_s 
, a.context_headers_user_id_s , a.item_id_l 
, c.player_type 
, b.reference 
, b.description 
, b.collectionrewardid 
, b.collection_name 
, b.category 
, b.collection_type 
, b.productionlevel 
from 
temp_inventory_data a 
join collectible_items b 
    on a.item_id_l = b.loot_id 
join player_cohorts	 c 
    on a.context_headers_title_id_s = c.context_headers_title_id_s 
    and a.context_headers_user_id_s = cast(c.client_user_id_l as varchar)
) y 


join 
( 
select dt, player_type, count(distinct row(context_headers_title_id_s, client_user_id_l)) as unique_users 
from player_cohorts 
group by 1,2
) z 
on y.player_type = z.player_type 
group by 1,2,3,4,5,6,9,10,11,12""" 

insert_player_collectible_completion_sql = """Insert overwrite as_s2.s2_collectible_item_acquisition_dashboard 

-- Filter Collecctible Items From Lootrest data 

with collectible_items as
(
select collectionrewardid
		, regexp_replace(regexp_replace(collection_name, 'MPUI_COLLECTION_', ''), 'LOOT_MTX1_COLLECTION_', '') as collection_name
		, loot_id
		, loot_group
		, name
		, reference
		, description
		, rarity 
		, collection_type 
		, case when productionlevel in ('Gold', 'TU1') then 'Launch Collection' 
               when productionlevel in ('MTX1') then 'Winter Collection' else productionlevel end as productionlevel
        , category 
from as_s2.loot_v4_ext a 
where productionlevel in ('Gold', 'TU1', 'MTX1')
and collectionrewardid <> loot_id
and collectionid > 0 
AND trim(isloot) <> ''
and category in ('emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon') 
group by 1,2,3,4,5,6,7,8,9,10,11
), 


-- Get Player Cohort 
player_cohorts as	
(
	SELECT DISTINCT a.context_headers_title_id_s, a.network_id 
	, a.client_user_id_l 
	, case when a.dt < date('2017-11-21') then 'Active Non-Spender' else coalesce(b.player_type, 'Active Non-Spender') end as player_type 
	, a.dt 
	FROM ads_ww2.fact_session_data a 
	left JOIN 
	( select * from as_s2.s2_spenders_active_cohort_staging 
	where raw_date = date '{{DS_DATE_ADD(0)}}' -- Confirm this table name
	) b 
	ON a.network_id = b.network_id 
	AND a.client_user_id_l = b.client_user_id 
	WHERE a.dt = date '{{DS_DATE_ADD(0)}}'
),

-- Combine all the separate Inventory table 
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
        , y.collection_name
		, y.collection_type
		, y.productionlevel
		, 1 as pool_size
		, z.unique_users 
		, count(distinct row(context_headers_title_id_s, context_headers_user_id_s))  as num_items 
		, count(distinct row(context_headers_title_id_s, context_headers_user_id_s))*pow(z.unique_users, -1) as avg_of_items
		, y.category
		, y.reference as item_name1
		, y.description as item_name2
		, z.dt as raw_date 
from 


(

select distinct a.context_headers_title_id_s 
, a.context_headers_user_id_s , a.item_id_l 
, c.player_type 
, b.reference 
, b.description 
, b.collectionrewardid 
, b.collection_name 
, b.category 
, b.collection_type 
, b.productionlevel 
from 
temp_inventory_data a 
join collectible_items b 
    on a.item_id_l = b.loot_id 
join player_cohorts	 c 
    on a.context_headers_title_id_s = c.context_headers_title_id_s 
    and a.context_headers_user_id_s = cast(c.client_user_id_l as varchar)
) y 


join 
( 
select dt, player_type, count(distinct row(context_headers_title_id_s, client_user_id_l)) as unique_users 
from player_cohorts 
group by 1,2
) z 
on y.player_type = z.player_type 
group by 1,2,3,4,5,6,9,10,11,12"""

insert_collectible_completion_item_task = qubole_operator('insert_collectible_completion_item_task',
                                              insert_collectible_completion_item_sql, 2, timedelta(seconds=600), dag) 

											  
insert_player_collectible_completion_task = qubole_operator('insert_player_collectible_completion_task',
                                              insert_player_collectible_completion_sql, 2, timedelta(seconds=600), dag) 
# Wire up the DAG , Setting Dependency of the tasks

insert_collectible_completion_item_task.set_upstream(start_time_task)
insert_player_collectible_completion_task.set_upstream(insert_collectible_completion_item_task)

