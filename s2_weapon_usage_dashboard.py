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
    'start_date': datetime(2017, 12, 06),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='s2_weapon_usage_dashboard',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(5, 30),
                             task_id='start_time_task',
                             dag=dag
                             )

current_date = (datetime.now()).date()
stats_date = current_date - timedelta(days=2)
stats_date2 = current_date - timedelta(days=1)

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


insert_daily_weapons_usage_sql = """ Insert overwrite table as_shared.s2_weapon_usage_dashboard 
with temp_match as 
(select distinct context_headers_title_id_s, context_data_match_common_matchid_s, match_common_map_s, match_common_gametype_s, 
	context_headers_event_id_s, match_common_utc_start_time_i, match_common_life_count_i,
	match_common_player_count_i, match_common_has_bots_b 
	FROM ads_ww2.fact_mp_match_data
	WHERE dt =  date '{{DS_DATE_ADD(-1)}}'
	AND context_data_match_common_matchid_s IS NOT NULL
	AND match_common_is_private_match_b = FALSE 
	AND context_headers_title_id_s in ('5597', '5598','5599')
	AND match_common_gametype_s in ('war', 'dom', 'raid', 'war hc', 'dom hc')
),


player_match as 
(
select distinct context_headers_title_id_s, context_data_match_common_matchid_s, context_data_players_index, client_gamer_tag_s, context_data_players_client_user_id_l, start_rank_i, start_prestige_i 
from ads_ww2.fact_mp_match_data_players 
where dt = date '{{DS_DATE_ADD(-1)}}'
and context_data_match_common_matchid_s in (select context_data_match_common_matchid_s from temp_match)
),

loot_table as 
(
select name, reference, description, rarity, productionlevel, category, rarity_s, loot_id, loot_group , BaseWeaponReference as weapon_base 
from as_s2.loot_v4_ext a 
where upper(category) = 'WEAPON'
group by 1,2,3,4,5,6,7,8,9,10
),


weapon_usage as 
 (
  select dt as raw_date 

 , case when title_id in ('5598') then 'XBOX'

		when title_id in ('5599') then 'PS4' 

		when title_id in ('5597') then 'PC' end as platform

		, regexp_replace(weapon_base, '_mp', '') as weapon_base

		, weapon_class 

		, game_type

		, rank 

		, prestige 

		, sum(kills) as kills

		, sum(deaths) as deaths

		, sum(duration)/60.0 as duration

		, count(*) as times_used 

		

FROM  

 (
select distinct a.context_headers_title_id_s as title_id
        , a.dt 
        , a.context_data_match_common_matchid_s as match_id
        , a.loadout_index_i as loadoutid 
        , a.weapon_guid_l 
        , d.loot_group as weapon_class
        , d.reference as weapon_description 
        , d.weapon_base 
        , b.match_common_gametype_s game_type 
        , c.client_gamer_tag_s as gamer_tag
        , c.start_rank_i as rank
        , c.start_prestige_i as prestige
        , a.kills_i as kills
        , a.deaths_i as deaths 
        , a.time_in_use_seconds_i as duration 
        , a.context_data_players_score_i as score
        , b.match_common_map_s as map_description
    from ads_ww2.fact_mp_match_data_players_weaponstats a 
    join temp_match b 
    on a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 
    join player_match c 
    on a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s 
    and a.context_data_players_index = c.context_data_players_index 
    join loot_table d 
    on a.weapon_guid_l = d.loot_id 
    where a.dt = date '{{DS_DATE_ADD(-1)}}'
    and a.time_in_use_seconds_i > 0 
    ) 
group by 1,2,3,4,5,6,7
),

cross_table as 
(
select * 
FROM (select distinct rank from weapon_usage where rank <= 54) c1
CROSS JOIN (select distinct game_type from weapon_usage) c2 
CROSS JOIN (select distinct platform from weapon_usage) c3
CROSS JOIN (SELECT distinct raw_date, weapon_base, weapon_class from weapon_usage) c4 
CROSS JOIN (select distinct prestige from weapon_usage where prestige between 0 and 11) c5
)


select d.monday_date as week_start_dt 
, c.game_type 
, c.platform 
, regexp_replace(c.weapon_base , '_mp', '') as weapon_base
, c.weapon_class 
, c.rank 
, c.prestige 
, coalesce(a.kills,0) as kills_equipped 
, coalesce(a.deaths,0) as deaths_equipped 
, coalesce(a.duration,0) as duration_equipped 
, coalesce(a.times_used,0) as times_used_equipped
, coalesce(b.kills,0) as kills_loadout
, coalesce(b.deaths,0) as deaths_loadout 
, coalesce(b.duration,0) as duration_loadout 
, coalesce(b.times_used,0) as times_used_loadout 
, sum(coalesce(a.kills,0)) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige) as kills_total_weapon_name
, sum(coalesce(a.duration,0)) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige) as duration_total_weapon_name 
, sum(coalesce(b.duration,0)) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige) as duration_loadout_total_weapon_name 
, count(c.weapon_base) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige, c.weapon_class) as total_num_weapon_base 
, c.raw_date 
from cross_table c
left join 

(
select raw_date 
, game_type 
, case when title_id in ('5598') then 'XBOX' 
            when title_id in ('5599') then 'PS4' 
			when title_id in ('5597') then 'PC' end as platform 
, regexp_replace(weapon_base , '_mp', '') as weapon_base 
, weapon_class
, rank
, prestige
, sum(kills) as kills 
, sum(deaths) as deaths 
, sum(score) as score 
, sum(duration)/60.0 as duration 
, sum(times_used) as times_used  

from 


(
(
select distinct b.context_headers_title_id_s as title_id 
, b.match_common_map_s as map_description 
, b.match_common_gametype_s as game_type 
, a.victim_weapon_s as weapon_name
, a.attacker_weapon_s as attacker_weapon
, d.loot_group as weapon_class 
, d.weapon_base 
, c.start_rank_i as rank 
, c.start_prestige_i as prestige 
, c.client_gamer_tag_s as gamer_tag
, a.context_headers_event_id_s 
, a.context_data_match_common_matchid_s
, a.player_index_i 
, a.attacker_i 
, a.spawn_time_ms_i 
, a.duration_ms_i 
, a.score_earned_i as score 
, (a.death_time_ms_i - a.spawn_time_ms_i)/1000.0 as duration 
, b.match_common_utc_start_time_i 
, 0 as kills 
, 1 as deaths 
, 1 as times_used 
, a.dt as raw_date 
from ads_ww2.fact_mp_match_data_lives a 

join temp_match b 
	
on a.context_headers_event_id_s = b.context_headers_event_id_s 
and a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 

join player_match c 

on a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s
and a.player_index_i = c.context_data_players_index

left join loot_table d -- Weapon Description Mapping 
on a.victim_weapon_guid_l = d.loot_id

-- Lives Data Filters 
where a.dt = date '{{DS_DATE_ADD(-1)}}'
and a.duration_ms_i > 0 
--and (a.spawn_pos_ai[1] > 0 or a.spawn_pos_ai[2] > 0 or a.spawn_pos_ai[3] > 0) 
and a.means_of_death_s <> 'none' 
and a.victim_weapon_s <> 'none' 


) 

union all


( 
select distinct b.context_headers_title_id_s as title_id 
, b.match_common_map_s as map_description 
, b.match_common_gametype_s as game_type 
, a.attacker_weapon_s as weapon_name
, a.victim_weapon_s as victim_weapon
, d.loot_group as weapon_class 
, d.weapon_base 
, c.start_rank_i as rank 
, c.start_prestige_i as prestige 
, c.client_gamer_tag_s as gamer_tag
, a.context_headers_event_id_s 
, a.context_data_match_common_matchid_s
, a.player_index_i 
, a.attacker_i 
, a.spawn_time_ms_i 
, a.duration_ms_i 
, 0 as score 
, 0.0 as duration 
, b.match_common_utc_start_time_i 
, 1 as kills 
, 0 as deaths 
, 0 as times_used 
, a.dt as raw_date 
from ads_ww2.fact_mp_match_data_lives a 

join temp_match b 
	
on a.context_headers_event_id_s = b.context_headers_event_id_s 
and a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 

join player_match c 

on a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s
and a.attacker_i = c.context_data_players_index

left join loot_table d -- Weapon Description Mapping Table 
on a.attacker_weapon_guid_l = d.loot_id

-- Lives Data Filters 
where a.dt = date '{{DS_DATE_ADD(-1)}}'
and a.duration_ms_i > 0 
--and (a.spawn_pos_ai[1] > 0 or a.spawn_pos_ai[2] > 0 or a.spawn_pos_ai[3] > 0) 
and a.means_of_death_s <> 'none' 
and a.attacker_weapon_s <> 'none' 

)  
)
group by 1,2,3,4,5,6,7
) a 
on c.raw_date = a.raw_date 
and c.game_type = a.game_type 
and c.platform = a.platform 
and c.weapon_base = a.weapon_base 
and c.weapon_class = a.weapon_class 
and c.rank = a.rank 
and c.prestige = a.prestige 

left join weapon_usage b 

on c.raw_date = b.raw_date 
and c.game_type = b.game_type 
and c.platform = b.platform 
and c.weapon_base = b.weapon_base 
and c.weapon_class = b.weapon_class 
and c.rank = b.rank 
and c.prestige = b.prestige 

left join as_shared.dim_date_id_date_monday_dev d 
on c.raw_date = d.raw_date
--),"""

insert_daily_weapons_usage_task = qubole_operator('insert_daily_weapons_usage',
                                              insert_daily_weapons_usage_sql, 2, timedelta(seconds=600), dag)
insert_daily_weapons_usage_a_day_back_sql = """ Insert overwrite table as_shared.s2_weapon_usage_dashboard 
with temp_match as 
(select distinct context_headers_title_id_s, context_data_match_common_matchid_s, match_common_map_s, match_common_gametype_s, 
	context_headers_event_id_s, match_common_utc_start_time_i, match_common_life_count_i,
	match_common_player_count_i, match_common_has_bots_b 
	FROM ads_ww2.fact_mp_match_data
	WHERE dt = date '{{DS_DATE_ADD(0)}}'
	AND context_data_match_common_matchid_s IS NOT NULL
	AND match_common_is_private_match_b = FALSE 
	AND context_headers_title_id_s in ('5597', '5598','5599')
	AND match_common_gametype_s in ('war', 'dom', 'raid', 'war hc', 'dom hc')
),


player_match as 
(
select distinct context_headers_title_id_s, context_data_match_common_matchid_s, context_data_players_index, client_gamer_tag_s, context_data_players_client_user_id_l, start_rank_i, start_prestige_i 
from ads_ww2.fact_mp_match_data_players 
where dt = date '{{DS_DATE_ADD(0)}}'
and context_data_match_common_matchid_s in (select context_data_match_common_matchid_s from temp_match)
),

loot_table as 
(
select name, reference, description, rarity, productionlevel, category, rarity_s, loot_id, loot_group , BaseWeaponReference as weapon_base 
from as_s2.loot_v4_ext a 
where upper(category) = 'WEAPON'
group by 1,2,3,4,5,6,7,8,9,10
),


weapon_usage as 
 (
  select dt as raw_date 

 , case when title_id in ('5598') then 'XBOX'

		when title_id in ('5599') then 'PS4' 

		when title_id in ('5597') then 'PC' end as platform

		, regexp_replace(weapon_base, '_mp', '') as weapon_base

		, weapon_class 

		, game_type

		, rank 

		, prestige 

		, sum(kills) as kills

		, sum(deaths) as deaths

		, sum(duration)/60.0 as duration

		, count(*) as times_used 

		

FROM  

 (
select distinct a.context_headers_title_id_s as title_id
        , a.dt 
        , a.context_data_match_common_matchid_s as match_id
        , a.loadout_index_i as loadoutid 
        , a.weapon_guid_l 
        , d.loot_group as weapon_class
        , d.reference as weapon_description 
        , d.weapon_base 
        , b.match_common_gametype_s game_type 
        , c.client_gamer_tag_s as gamer_tag
        , c.start_rank_i as rank
        , c.start_prestige_i as prestige
        , a.kills_i as kills
        , a.deaths_i as deaths 
        , a.time_in_use_seconds_i as duration 
        , a.context_data_players_score_i as score
        , b.match_common_map_s as map_description
    from ads_ww2.fact_mp_match_data_players_weaponstats a 
    join temp_match b 
    on a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 
    join player_match c 
    on a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s 
    and a.context_data_players_index = c.context_data_players_index 
    join loot_table d 
    on a.weapon_guid_l = d.loot_id 
    where a.dt = date '{{DS_DATE_ADD(0)}}'
    and a.time_in_use_seconds_i > 0 
    ) 
group by 1,2,3,4,5,6,7
),

cross_table as 
(
select * 
FROM (select distinct rank from weapon_usage where rank <= 54) c1
CROSS JOIN (select distinct game_type from weapon_usage) c2 
CROSS JOIN (select distinct platform from weapon_usage) c3
CROSS JOIN (SELECT distinct raw_date, weapon_base, weapon_class from weapon_usage) c4 
CROSS JOIN (select distinct prestige from weapon_usage where prestige between 0 and 11) c5
)


select d.monday_date as week_start_dt 
, c.game_type 
, c.platform 
, regexp_replace(c.weapon_base , '_mp', '') as weapon_base
, c.weapon_class 
, c.rank 
, c.prestige 
, coalesce(a.kills,0) as kills_equipped 
, coalesce(a.deaths,0) as deaths_equipped 
, coalesce(a.duration,0) as duration_equipped 
, coalesce(a.times_used,0) as times_used_equipped
, coalesce(b.kills,0) as kills_loadout
, coalesce(b.deaths,0) as deaths_loadout 
, coalesce(b.duration,0) as duration_loadout 
, coalesce(b.times_used,0) as times_used_loadout 
, sum(coalesce(a.kills,0)) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige) as kills_total_weapon_name
, sum(coalesce(a.duration,0)) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige) as duration_total_weapon_name 
, sum(coalesce(b.duration,0)) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige) as duration_loadout_total_weapon_name 
, count(c.weapon_base) over (partition by c.raw_date , c.game_type , c.platform , c.rank, c.prestige, c.weapon_class) as total_num_weapon_base 
, c.raw_date 
from cross_table c
left join 

(
select raw_date 
, game_type 
, case when title_id in ('5598') then 'XBOX' 
            when title_id in ('5599') then 'PS4' 
			when title_id in ('5597') then 'PC' end as platform 
, regexp_replace(weapon_base , '_mp', '') as weapon_base 
, weapon_class
, rank
, prestige
, sum(kills) as kills 
, sum(deaths) as deaths 
, sum(score) as score 
, sum(duration)/60.0 as duration 
, sum(times_used) as times_used  

from 


(
(
select distinct b.context_headers_title_id_s as title_id 
, b.match_common_map_s as map_description 
, b.match_common_gametype_s as game_type 
, a.victim_weapon_s as weapon_name
, a.attacker_weapon_s as attacker_weapon
, d.loot_group as weapon_class 
, d.weapon_base 
, c.start_rank_i as rank 
, c.start_prestige_i as prestige 
, c.client_gamer_tag_s as gamer_tag
, a.context_headers_event_id_s 
, a.context_data_match_common_matchid_s
, a.player_index_i 
, a.attacker_i 
, a.spawn_time_ms_i 
, a.duration_ms_i 
, a.score_earned_i as score 
, (a.death_time_ms_i - a.spawn_time_ms_i)/1000.0 as duration 
, b.match_common_utc_start_time_i 
, 0 as kills 
, 1 as deaths 
, 1 as times_used 
, a.dt as raw_date 
from ads_ww2.fact_mp_match_data_lives a 

join temp_match b 
	
on a.context_headers_event_id_s = b.context_headers_event_id_s 
and a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 

join player_match c 

on a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s
and a.player_index_i = c.context_data_players_index

left join loot_table d -- Weapon Description Mapping 
on a.victim_weapon_guid_l = d.loot_id

-- Lives Data Filters 
where a.dt = date '{{DS_DATE_ADD(0)}}'
and a.duration_ms_i > 0 
--and (a.spawn_pos_ai[1] > 0 or a.spawn_pos_ai[2] > 0 or a.spawn_pos_ai[3] > 0) 
and a.means_of_death_s <> 'none' 
and a.victim_weapon_s <> 'none' 


) 

union all


( 
select distinct b.context_headers_title_id_s as title_id 
, b.match_common_map_s as map_description 
, b.match_common_gametype_s as game_type 
, a.attacker_weapon_s as weapon_name
, a.victim_weapon_s as victim_weapon
, d.loot_group as weapon_class 
, d.weapon_base 
, c.start_rank_i as rank 
, c.start_prestige_i as prestige 
, c.client_gamer_tag_s as gamer_tag
, a.context_headers_event_id_s 
, a.context_data_match_common_matchid_s
, a.player_index_i 
, a.attacker_i 
, a.spawn_time_ms_i 
, a.duration_ms_i 
, 0 as score 
, 0.0 as duration 
, b.match_common_utc_start_time_i 
, 1 as kills 
, 0 as deaths 
, 0 as times_used 
, a.dt as raw_date 
from ads_ww2.fact_mp_match_data_lives a 

join temp_match b 
	
on a.context_headers_event_id_s = b.context_headers_event_id_s 
and a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 

join player_match c 

on a.context_data_match_common_matchid_s = c.context_data_match_common_matchid_s
and a.attacker_i = c.context_data_players_index

left join loot_table d -- Weapon Description Mapping Table 
on a.attacker_weapon_guid_l = d.loot_id

-- Lives Data Filters 
where a.dt = date '{{DS_DATE_ADD(0)}}'
and a.duration_ms_i > 0 
--and (a.spawn_pos_ai[1] > 0 or a.spawn_pos_ai[2] > 0 or a.spawn_pos_ai[3] > 0) 
and a.means_of_death_s <> 'none' 
and a.attacker_weapon_s <> 'none' 

)  
)
group by 1,2,3,4,5,6,7
) a 
on c.raw_date = a.raw_date 
and c.game_type = a.game_type 
and c.platform = a.platform 
and c.weapon_base = a.weapon_base 
and c.weapon_class = a.weapon_class 
and c.rank = a.rank 
and c.prestige = a.prestige 

left join weapon_usage b 

on c.raw_date = b.raw_date 
and c.game_type = b.game_type 
and c.platform = b.platform 
and c.weapon_base = b.weapon_base 
and c.weapon_class = b.weapon_class 
and c.rank = b.rank 
and c.prestige = b.prestige 

left join as_shared.dim_date_id_date_monday_dev d 
on c.raw_date = d.raw_date
--),""" 

insert_daily_weapons_usage_a_day_back_task = qubole_operator('insert_daily_weapons_usage_a_day_back',
                                              insert_daily_weapons_usage_a_day_back_sql, 2, timedelta(seconds=600), dag)


# Wire up the DAG
insert_daily_weapons_usage_task.set_upstream(start_time_task)
insert_daily_weapons_usage_a_day_back_task.set_upstream(insert_daily_weapons_usage_task)
