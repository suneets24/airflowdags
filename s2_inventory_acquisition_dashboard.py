import airflow
from datetime import timedelta, datetime, time
from airflow.operators.python_operator import PythonOperator
from airflow.operators.sensors import TimeSensor
from quboleWrapper import qubole_wrapper, export_to_rdms

query_type = 'dev_presto'

# Set expected runtime in seconds, setting to 0 is 7200 seconds
expected_runtime = 0

# The GROUP that owns this DAG
owner = "Analytic Services"

default_args = {
    'owner': owner,
    'depends_on_past': False,
    'start_date': datetime(2017, 11, 21),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_inventory_acquisition_dashboard',
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

insert_daily_inventory_acquisition_sql = '''Insert overwrite table as_shared.s2_inventory_acquisition_dashboard 

-- Read loot table 

with loot_table as 
(
SELECT name, reference, description, rarity, 
case when productionlevel in ('Gold', 'TU1') then 'Launch' else productionlevel end as productionlevel 
, category, rarity_s, loot_id, loot_group , BaseWeaponReference as weapon_base, (case when collectionrewardid = loot_id then 2
    when collectionid > 0 then 1 else 0 end) as is_collectible
FROM as_s2.loot_v4_ext a 
join as_s2.s2_items_from_crates b 
on a.loot_id = b.item_id_l 
WHERE productionlevel in ('Gold', 'TU1', 'MTX1') 
AND trim(isloot) <> ''
and category in ('emote', 'grip', 'uniforms', 'weapon', 'playercard_title', 'playercard_icon') 
--and reference not in ('playercard_social_3', 'playercard_zm_challenge_11', 'playercard_zm_challenge_01', 'playercard_social_4')
--and (b.item_id_l is not null or collectionrewardid = loot_id)
GROUP by 1,2,3,4,5,6,7,8,9,10,11
),

-- Read pool data input from Econ Sim 

simulation_loot_pool as 
(
SELECT 
  case when category like '%emote%' then 'emote' 
       when category like '%grip%' then 'grip' 
	   when category like '%uniform%' then 'uniforms' 
	   when category like '%weapon%' then 'weapon' 
	   when category like '%emblem%' then 'playercard_icon' 
	   when category like '%card%' then 'playercard_title' 
	   when category like '%helmet%' then 'helmet' end as category 
     
, case when rarity = 'common' then 0 
       when rarity = 'rare' then 1 
	   when rarity = 'legendary' then 2 
	   when rarity = 'epic' then 3 
	   when rarity = 'heroic' then 4 end as rarity 
, case when release_day = 0 then 'Launch' 
when release_day = 35 then 'MTX1' end as productionlevel 
, case when category like '%nc%' then 0 else 1 end as is_collectible 
, count as pool_size 
FROM as_S2.s2_loot_pool_sim 
WHERE category not in ('currency' , 'consumable')
and count > 0

),

-- Create a table with all the category, rarity and their pool size 

loot_cross as 
(
SELECT a.category, a.rarity, a.productionlevel, 
a.is_collectible, coalesce(b.pool_size, a.pool_size,0) as pool_size  
FROM 
(
SELECT category, rarity, productionlevel, is_collectible 
--, condition 
, count(DISTINCT loot_id) as pool_size
FROM loot_table 
GROUP by 1,2,3,4
) a 
left JOIN simulation_loot_pool b 
on a.category = b.category 
and a.rarity = b.rarity 
and a.productionlevel = b.productionlevel 
and a.is_collectible = b.is_collectible 
)
,

-- Define Active Player Cohorts based on the lifetime spending 

player_cohorts as	
(
	SELECT DISTINCT a.context_headers_title_id_s, a.network_id 
	, a.client_user_id_l 
	, coalesce(b.player_type, 'Active Non-Spender') as player_type 
	, a.dt 
	FROM ads_ww2.fact_session_data a 
	left JOIN 
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
),

-- Combine all the separate inventory tables to create one temporary table 

temp_inventory_data as 
(
SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_awardproduct_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all
 
SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_consumeawards_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all 

SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, 0, 0 , dt
FROM ads_ww2.fact_mkt_consumeinventoryitems_data_eventinfo_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all 

SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_consumeinventoryitems_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all 

SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_durableprocess_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all 

SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_durablerevoke_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all 

SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_pawnitems_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'

union all 

SELECT context_headers_title_id_s, context_headers_user_id_s, item_id_l, quantity_old_l, quantity_new_l , dt
FROM ads_ww2.fact_mkt_purchaseskus_data_userdatachanges_inventoryitems 
WHERE dt <= date '{{DS_DATE_ADD(0)}}'
)

SELECT y.player_type, x.category, x.rarity, x.is_collectible, x.productionlevel, x.pool_size 
, z.unique_users 
, y.num_items 
, y.num_items*pow(z.unique_users, -1) 
, z.dt as raw_date 

FROM loot_cross x 
JOIN 
(
SELECT  player_type, category, productionlevel, rarity, is_collectible, sum(num_items) as num_items 
FROM 
(
SELECT c.player_type, a.context_headers_user_id_s, b.category, b.productionlevel, b.rarity, b.is_collectible, count(DISTINCT a.item_id_l) as num_items, sum(quantity_new_l - quantity_old_l ) as quantity_tot 

FROM temp_inventory_data a 

JOIN loot_table b 
ON a.item_id_l = b.loot_id 

JOIN player_cohorts c 
ON a.context_headers_title_id_s = c.context_headers_title_id_s 
AND a.context_headers_user_id_s = cast(c.client_user_id_l as varchar)
GROUP by 1,2,3,4,5,6
) 
GROUP by 1,2,3,4,5
) y 

on x.category = y.category 
and x.rarity = y.rarity 
and x.is_collectible = y.is_collectible
and x.productionlevel = y.productionlevel

JOIN 
    (SELECT dt, player_type, sum(unique_users) as unique_users 
	FROM 
	(SELECT dt, player_type, context_headers_title_id_s, count(DISTINCT client_user_id_l) as unique_users 
	FROM player_cohorts 
	GROUP by 1,2,3) 
    GROUP by 1,2 ) Z 
on y.player_type = z.player_type  ''' 

insert_daily_inventory_acquisition_task = qubole_operator('insert_daily_inventory_acquisition',
                                              insert_daily_inventory_acquisition_sql, 2, timedelta(seconds=600), dag)
# Wire up the DAG
insert_daily_inventory_acquisition_task.set_upstream(start_time_task)
