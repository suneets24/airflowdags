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
    'start_date': datetime(2017, 12, 18),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_dupe_drop_rate_dashboard',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(6, 45),
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
        pool='hive_default_pool',
        op_kwargs={'db_type': query_type,
                   'raw_sql': sql,
                   'expected_runtime': expected_runtime,
                   'dag_id': dag.dag_id,
                   'task_id': task_id
                   },
        templates_dict={'ds': '{{ ds }}'},
        dag=dag)


def export_to_rdms_operator(task_id, table_name, retries, retry_delay, dag):
    return PythonOperator(
        task_id=task_id,
        python_callable=export_to_rdms,
        provide_context=True,
        retries=retries,
        retry_delay=retry_delay,
        pool='hive_default_pool',
        op_kwargs={'table_name': table_name,
                   'expected_runtime': expected_runtime,
                   'dag_id': dag.dag_id,
                   'task_id': task_id
                   },
        templates_dict=None,
        dag=dag)

## Define function to pass different parameter for string evaluations 

def evaluate_queries(query, eval_param,times_var):
    times_var = times_var + 1
    pass_param = []
    for i in range(1,times_var):
        pass_param.append('eval_param')
    pass_param = ','.join(str(x) for x in pass_param)
    query_evaluated = query %(eval(pass_param))
    return query_evaluated

insert_item_drop_rate_cohort_crate_sql = """Insert overwrite table as_s2.s2_dupe_drop_rate_dashboard
-- Consolidate all the inventory tables

with temp_inventory_items as
(
select a.context_headers_title_id_s 
, a.context_headers_user_id_s, item_id_l 
, a.context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l
, to_unixtime(from_iso8601_timestamp(context_headers_timestamp_s)) as context_headers_timestamp_s 
, case when date '{{DS_DATE_ADD(0)}}' < date('2017-11-21') then 'Active Non-Spender' else coalesce(c.player_type, 'Active Non-Spender') end as player_type
, b.category
,b.reference
, a.dt
from 
--ads_ww2.fact_datachanges_inventory_items_view 
(
select context_headers_title_id_s, context_headers_user_id_s, item_id_l 
, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_awardproduct_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_consumeawards_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s, 0, 0 
  , context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_eventinfo_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_consumeinventoryitems_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
,context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_durableprocess_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 
--select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s, quantity_old_l, quantity_new_l,context_headers_timestamp_s, dt
--from ads_ww2.fact_mkt_durablerevoke_data_userdatachanges_inventoryitems 
--where dt <= date '{{DS_DATE_ADD(0)}}'
--union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_pawnitems_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'

union all 

select context_headers_title_id_s, context_headers_user_id_s, item_id_l, context_data_mmp_transaction_id_s 
, case when quantity_old_l is null then 0 else quantity_old_l end as quantity_old_l 
, case when quantity_new_l is null then 0 else quantity_new_l end as quantity_new_l 
, context_headers_timestamp_s, dt
from ads_ww2.fact_mkt_purchaseskus_data_userdatachanges_inventoryitems 
where dt <= date '{{DS_DATE_ADD(0)}}'
) a 

-- Filter crates and items 
left join as_s2.loot_v4_ext b 
on a.item_id_l = b.loot_id 

-- Map Player Type 
left join 
(
select distinct * from as_s2.s2_spenders_active_cohort_staging 
where raw_date = date '{{DS_DATE_ADD(0)}}'
) c 
on a.context_headers_title_id_s = c.title_id_s 
and a.context_headers_user_id_s = cast(c.client_user_id as varchar) 

-- Filter Inventory 
where a.dt = date '{{DS_DATE_ADD(0)}}' 
and trim(context_data_mmp_transaction_id_s) not in ('0', '') -- Exclude Transaction Id as 0 
-- Filter Categories and Productionlevel 
and ( b.category in ( 'emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon', 'consumable') or a.item_id_l in (1,2,5,6,75,12583025,12583026,12583027,12583028))
-- Remove productionlevel filter as any and filter on lootrest data will drop armory credits ids
--and b.productionlevel in ('Gold', 'TU1','MTX1')
),

crate_opens as 
(
select distinct a.* from temp_inventory_items a where item_id_l in (1,2,5,6,75) and quantity_new_l < quantity_old_l
),

-- Separate Queries for Dupes, New Items, Armory Credits and XP. Must be combined

crate_dupes_final as
(
select a.item_id_l, a.context_headers_title_id_s
, a.context_headers_user_id_s
, a.player_type
, a.context_data_mmp_transaction_id_s
, sum(case when coalesce(b.item_id_l, 0) = 0 then 0 else 1 end) as num_items
,'Dupes' as rate_identifier
from crate_opens  a 
left join 
    ( 
	 select distinct context_headers_title_id_s as title_id , context_headers_user_id_s as inv_user_id 
	 , context_data_mmp_transaction_id_s as transactionid , item_id_l 
	 from temp_inventory_items 
	 -- Quantity >1 are duplicate items 
	 -- Excluding crates and consumables as they can have quantity_new_l > 1 
	 where (quantity_new_l > 1 and item_id_l not in (1,2,5,6,75) and category in ('emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon')) 
	 ) b
on  a.context_headers_title_id_s = b.title_id
and a.context_headers_user_id_s = b.inv_user_id 
and a.context_data_mmp_transaction_id_s = b.transactionid
group by 1,2,3,4,5
),

crate_new_items_final as
(
select a.item_id_l, a.context_headers_title_id_s
, a.context_headers_user_id_s
, a.player_type
, a.context_data_mmp_transaction_id_s
, sum(case when coalesce(b.item_id_l, 0) = 0 then 0 else 1 end) as num_items
, 'New Items'  as rate_identifier
from crate_opens  a 
left join 
    ( 
	 select distinct context_headers_title_id_s as title_id , context_headers_user_id_s as inv_user_id 
	 , context_data_mmp_transaction_id_s as transactionid , item_id_l 
	 from temp_inventory_items 
	 where (quantity_new_l = 1 and item_id_l not in (1,2,5,6,75) and category in ('emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon')) 
	 ) b
on  a.context_headers_title_id_s = b.title_id
and a.context_headers_user_id_s = b.inv_user_id 
and a.context_data_mmp_transaction_id_s = b.transactionid
group by 1,2,3,4,5
),

crate_ac_final as
(
select a.item_id_l, a.context_headers_title_id_s
, a.context_headers_user_id_s
, a.player_type
, a.context_data_mmp_transaction_id_s
, sum(case when coalesce(b.item_id_l, 0) = 0 then 0 else 1 end) as num_items
, 'Armory Credits'  as rate_identifier
from crate_opens a 
left join 
    ( 
	 select distinct context_headers_title_id_s as title_id , context_headers_user_id_s as inv_user_id 
	 , context_data_mmp_transaction_id_s as transactionid , item_id_l 
	 from temp_inventory_items  
	 where item_id_l in (12583025,12583026,12583027,12583028) 
	 ) b
on  a.context_headers_title_id_s = b.title_id
and a.context_headers_user_id_s = b.inv_user_id 
and a.context_data_mmp_transaction_id_s = b.transactionid
group by 1,2,3,4,5
),

crate_xp_final as
(
select a.item_id_l, a.context_headers_title_id_s
, a.context_headers_user_id_s
, a.player_type
, a.context_data_mmp_transaction_id_s
, sum(case when coalesce(b.item_id_l, 0) = 0 then 0 else 1 end) as num_items
, 'XP' as rate_identifier
from crate_opens a 
left join 
    ( 
	 select distinct context_headers_title_id_s as title_id , context_headers_user_id_s as inv_user_id 
	 , context_data_mmp_transaction_id_s as transactionid , item_id_l 
	 from temp_inventory_items  
	 where category in ('consumable') and reference not like '%%_counter%%' and reference not like '%%token%%'
	 ) b
on  a.context_headers_title_id_s = b.title_id
and a.context_headers_user_id_s = b.inv_user_id 
and a.context_data_mmp_transaction_id_s = b.transactionid
group by 1,2,3,4,5
)

--Agggregate results at Player Type level 
select rate_identifier
,(case when item_id_l=1 then 'MP Common'
when item_id_l = 2 then 'MP Rare'
when item_id_l = 5 then 'ZM Common' 
when item_id_l = 6 then 'ZM Rare'
when item_id_l = 75 then 'SD Winter'				
end)  as crate_num
,player_type, avg(num_items) as avg_of_items
, count(distinct context_data_mmp_transaction_id_s) as total_crates
, count(distinct context_headers_user_id_s) as total_users
, sum(case when num_items = 0 then 1 else 0 end ) as no_item_rates
, sum(case when num_items = 1 then 1 else 0 end ) as one_item_rates
, sum(case when num_items = 2 then 1 else 0 end ) as two_item_rates
, sum(case when num_items = 3 then 1 else 0 end ) as three_item_rates 
, sum(case when num_items = 4 then 1 else 0 end ) as four_item_rates 
, sum(case when num_items = 5 then 1 else 0 end ) as five_item_rates 
, sum(case when num_items = 6 then 1 else 0 end ) as six_item_rates 
, date '{{DS_DATE_ADD(0)}}' as day_date
from 
((select * from crate_dupes_final) union all
(select * from crate_new_items_final) union all
(select * from crate_ac_final) union all
(select * from crate_xp_final))
group by 1,2,3""" 
##%(stats_date, stats_date, stats_date, stats_date, stats_date) 

insert_item_drop_rate_cohort_crate_task = qubole_operator('daily_dupe_rate_cohort_crate_two_days',
                                              insert_item_drop_rate_cohort_crate_sql, 3, timedelta(seconds=600), dag) 
 

# Wire up the DAG , Setting Dependency of the tasks
insert_item_drop_rate_cohort_crate_task.set_upstream(start_time_task)


