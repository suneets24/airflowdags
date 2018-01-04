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
    'start_date': datetime(2017, 11, 9),
    'schedule_interval': '@daily'
}

dag = airflow.DAG(dag_id='WWII_GameType_Maps_and_Playlist_Usage',
                  default_args=default_args
                  )

# Start running at this time
start_time_task = TimeSensor(target_time=time(6, 00),
                             task_id='start_time_task',
                             dag=dag
                             )

current_date = (datetime.now()).date()
stats_date2 = current_date - timedelta(days=2)
stats_date = current_date - timedelta(days=1)

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

insert_daily_gametype_maps_playlist_usage_sql = """Insert overwrite table as_shared.s2_gametype_maps_playlist_dashboard 
with first_round as
(
  select distinct  a.context_headers_title_id_s 
  ,a.match_common_gametype_s 
  , a.match_common_map_s 
  , a.context_data_match_common_playlist_id_i 
  , a.context_data_match_common_matchid_s 
  , a.axisgamemodedata_ai 
  , a.axisobj_i 
  , a.match_common_utc_start_time_i 
  , a.match_common_utc_end_time_i
  , a.matchduration 
  , case when a.alliesgamemodedata_ai[1] > 0 then alliesgamemodedata_ai else a.axisgamemodedata_ai end as first_round_data 
  , case when alliesgamemodedata_ai[1] > 0 then alliesobj_i else axisobj_i end as first_round_obj 
  , a.dt 
 from ads_ww2.fact_mp_match_data a 
  where a.dt = date '{{DS_DATE_ADD(%s)}}' 
  and a.match_common_gametype_s in ('raid' , 'raid hc') 
  and a.match_common_previous_match_id_s = ''
  and a.context_data_match_common_playlist_id_i <> 0
),

second_round as 
(
  select distinct  a.context_headers_title_id_s 
  ,a.match_common_gametype_s 
  , a.match_common_map_s 
  , a.context_data_match_common_playlist_id_i 
  , a.context_data_match_common_matchid_s 
  , a.axisgamemodedata_ai 
  , a.axisobj_i 
  , a.match_common_utc_start_time_i 
  , a.match_common_utc_end_time_i
  , a.matchduration 
  , a.match_common_previous_match_id_s 
  , case when a.alliesgamemodedata_ai[1] > 0 then alliesgamemodedata_ai else a.axisgamemodedata_ai end as second_round_data 
  , case when alliesgamemodedata_ai[1] > 0 then alliesobj_i else axisobj_i end as second_round_obj 
  , a.dt 
 from ads_ww2.fact_mp_match_data a 
  where a.dt = date '{{DS_DATE_ADD(%s)}}' 
  and a.match_common_gametype_s in ('raid' , 'raid hc') 
  and a.match_common_previous_match_id_s <> ''
  and a.context_data_match_common_playlist_id_i <> 0
),

first_round_players AS
(
  select distinct a.*, b.context_data_players_client_user_id_l, b.disconnect_reason_s
  from first_round a join  ads_ww2.fact_mp_match_data_players b
  on a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 
  where b.utc_connect_time_s_i <> 0 
),

second_round_players AS
(
  select distinct a.*, b.context_data_players_client_user_id_l, b.disconnect_reason_s
  from second_round a join ads_ww2.fact_mp_match_data_players b
  on a.context_data_match_common_matchid_s = b.context_data_match_common_matchid_s 
  where b.utc_connect_time_s_i <> 0
), 

temp1 as 
(select distinct a.context_headers_title_id_s, a.context_data_match_common_matchid_s as allies_match, a.first_round_data 
, a.first_round_obj, a.matchduration as allies_matchduration, a.match_common_utc_end_time_i, a.match_common_gametype_s 
, a.match_common_map_s , a.context_data_match_common_playlist_id_i
, b.match_common_utc_start_time_i, b.context_data_match_common_matchid_s as axis_match, b.second_round_data, b.second_round_obj 
, b.matchduration as axis_matchduration, a.dt 
from first_round a join second_round b 
on a.dt = b.dt 
and a.context_headers_title_id_s =  b.context_headers_title_id_s
--and a.match_common_utc_end_time_i >= b.match_common_utc_start_time_i - 60 
--and a.match_common_utc_end_time_i <= b.match_common_utc_start_time_i
--and a.context_headers_user_id_s = b.context_headers_user_id_s 
 and a.context_data_match_common_matchid_s = b.match_common_previous_match_id_s
and a.match_common_gametype_s = b.match_common_gametype_s 
and a.match_common_map_s = b.match_common_map_s
),

temp2 as
(
select *, count(*) over(partition by allies_match) as allies_N, count(*) over(partition by axis_match) as axis_N from temp1
),

war_tab AS
(
	select *, concat(cast (allies_match as varchar), cast(axis_match as varchar)) as concat_matchid,  (allies_matchduration + axis_matchduration) as matchduration  from temp2
    where allies_N = 1 and axis_N = 1
),


player_mp AS 
(
  select distinct e.* 
  from (select a.*, b.context_data_players_client_user_id_l, b.disconnect_reason_s
		from war_tab a join first_round_players b 
		on a.allies_match = b.context_data_match_common_matchid_s 
		where b.disconnect_reason_s like 'EXE_DISCONNECTED%%'
		union all 
		select c.*, d.context_data_players_client_user_id_l, d.disconnect_reason_s
		from war_tab c join second_round_players d 
		on c.axis_match = d.context_data_match_common_matchid_s 
		where (d.disconnect_reason_s like 'EXE_DISCONNECTED%%' or d.disconnect_reason_s in ('EXE_MATCHENDED'))
		) e 
),

agg_war_match_disconnects as 
(
select raw_date, description, game_type, playlist_id, map_name, count(distinct matchid) as num_matches 
, sum(duration_total) as match_duration 
, sum(early_quits) as early_quits 
, sum(all_quits) as all_quits 
, sum(users) as users 
from 
(
select dt as raw_date
		, concat_matchid as matchid 
        , case when context_headers_title_id_s in ('5599') then 'PS4'
              when context_headers_title_id_s in ('5598') then 'XBOX'
              when context_headers_title_id_s in ('5597') then 'PC' end as description 
			  
-- All these game types description have to be updated with latest descriptions 

		, case when match_common_gametype_s = 'raid' then 'War'
		       when match_common_gametype_s = 'raid hc' then 'War Hardcore'
		else match_common_gametype_s 
        end as game_type 


-- Create a separate playlist Id mapping for PC 

, case when context_data_match_common_playlist_id_i = 2 then 'War'
        else cast(context_data_match_common_playlist_id_i as varchar) 
     end as playlist_id 
		
-- Map Descriptions and their Screen Names 

    ,case when match_common_map_s = 'mp_raid_cobra' then 'Operation Breakout' 
          when match_common_map_s = 'mp_raid_bulge' then 'Operation Griffin' 	
          when match_common_map_s = 'mp_raid_aachen' then 'Operation Aachen' 	
          when match_common_map_s = 'mp_raid_d_day' then 'Operation Neptune' 	  
	      else match_common_map_s 
	end as map_name 

 	,matchduration/60.0 as duration_total -- Get Match Duration 
	,count(distinct case when disconnect_reason_s like 'EXE_DISCONNECTED%%' then context_data_players_client_user_id_l end) as early_quits -- Voluntary Quits 
	,count(distinct case when disconnect_reason_s like 'EXE_DISCONNECTED%%' or disconnect_reason_s in ('EXE_MATCHENDED')  then context_data_players_client_user_id_l end) as all_quits 
	, count(distinct context_data_players_client_user_id_l) as users 
	from player_mp 
	group by 1,2,3,4,5,6,7
) 
group by 1,2,3,4,5 
) ,

--select * from agg_war_match_disconnects limit 10

--with 
war_mode_agg as 
(
with temp_lives_war as 
(
select distinct victim_user_id 
, context_headers_title_id_s as title_id 
, context_data_match_common_map_s as map_description 
, context_data_match_common_gametype_s as game_type_description 
, context_data_match_common_matchid_s 
, context_data_match_common_playlist_id_i as playlist_id
, context_data_match_common_playlist_name_s 
, attacker_user_id 
, death_time_ms_i 
, spawn_pos_ai 
, victim_weapon_s 
, duration_ms_i 
, dt from 
ads_ww2.fact_mp_match_data_lives 
where dt = date '{{DS_DATE_ADD(%s)}}'
and context_data_match_common_gametype_s in ('raid', 'raid hc')
and context_data_match_common_is_private_match_b = FALSE 
and context_headers_title_id_s in ('5597', '5598', '5599')
)

select case when a.title_id in ('5599') then 'PS4'
              when a.title_id in ('5598') then 'XBOX'
              when a.title_id in ('5597') then 'PC' 
	   end as description 
,case when a.game_type_description = 'raid' then 'War' 
            when a.game_type_description = 'raid hc' then 'War Hardcore' 
			else a.game_type_description 
       end as game_type
,case when a.playlist_id = 2 then 'War' 
	        else cast(a.playlist_id as varchar) 
       end as playlist_id  
,case  when a.map_description = 'mp_raid_cobra' then 'Operation Breakout' 
             when a.map_description = 'mp_raid_bulge' then 'Operation Griffin' 	
             when a.map_description = 'mp_raid_aachen' then 'Operation Aachen' 	
             when a.map_description = 'mp_raid_d_day' then 'Operation Neptune' 
			 else a.map_description 
       end as map_name
, a.raw_date 
, sum(num_kills) as kills 
, sum(num_deaths) as deaths 
from 
(
select victim_user_id, title_id, game_type_description, playlist_id, map_description, dt as raw_date, count(*) as num_Deaths 
from temp_lives_war 
group by 1,2,3,4,5,6
) a 
join 
(
select attacker_user_id, title_id, game_type_description, playlist_id, map_description, dt as raw_date, count(*) as num_kills 
from temp_lives_war 
group by 1,2,3,4,5,6
) b 
on a.victim_user_id = b.attacker_user_id 
and a.title_id = b.title_id 
and a.game_type_description = b.game_type_description 
and a.playlist_id = b.playlist_id 
and a.map_description = b.map_description 
and a.raw_date = b.raw_date 

group by 1,2,3,4,5
)

select a.monday_date
	,a.game_type
    ,a.platform
	,a.playlist_id
	,a.map_name
	,coalesce(c.num_matches,a.num_matches) as num_matches 
    ,coalesce(c.users, a.users) as users 
	,coalesce(b.kills, a.kills) as kills 
	,coalesce(b.deaths, a.deaths) as deaths
	,a.score
	,a.lives_count 
	,a.life_time
    ,a.xp
	,coalesce(c.match_duration, a.match_duration) as match_duration 
	,a.play_duration
	,coalesce(c.early_quits, a.early_quits ) as early_quits
	,coalesce(c.all_quits, a.all_quits ) as all_quits 
	,a.Date_now
	,a.now 
	,a.raw_date 
	from 
(
Select d.monday_date
	,game_type
    ,description as platform
	,playlist_id
	,map_name
	,count(distinct match_id) as num_matches
    ,sum(users) as users
	,sum(kills) as kills
	,sum(deaths) as deaths
	,sum(score) as score
	,sum(lives_count) as lives_count 
	,sum(life_time)/60.0 as life_time
    ,sum(xp) as xp
	,sum(duration_total) as match_duration
	,sum(play_duration) as play_duration
	,sum(early_quits) as early_quits 
	,sum(all_quits) as all_quits 
	,cast((select max(date(from_unixtime(match_common_utc_end_time_i))) from ads_ww2.fact_mp_match_data where date(from_unixtime(match_common_utc_end_time_i)) <= current_date) as date) as Date_now
	,current_date as now 
	, d.raw_date
	from 
	
	(

-- Aggreate data at Match Level 

		select a.dt
		, a.match_id
        , case when a.title_id in ('5599') then 'PS4'
              when a.title_id in ('5598') then 'XBOX'
              when a.title_id in ('5597') then 'PC' end as description 
			  
-- All these game types description have to be updated with latest descriptions 

		, case when game_type_description = 'ctf' then 'Capture The Flag'
        when game_type_description = 'war' then 'Team Deathmatch'
        when game_type_description = 'dm hc' then 'Free-for-all Hardcore'
        when game_type_description = 'hp' then 'Hardpoint' 
		when game_type_description = 'raid' then 'War'
		when game_type_description = 'sd' then 'Search and Destroy' 
		when game_type_description = 'dom' then 'Domination' 
		when game_type_description = 'conf' then 'Kill Confirmed' 
		when game_type_description = 'dm' then 'Free-for-all' 
		when game_type_description = 'ball' then 'Uplink' 
		when game_type_description = 'dom hc' then 'Domination Hardcore' 
		when game_type_description = 'war hc' then 'Team Deathmatch Hardcore' 
		when game_type_description = 'sd hc' then 'Search and Destroy Hardcore' 
		when game_type_description = 'raid hc' then 'War Hardcore'
		when game_type_description = 'hp hc' then 'Hardpoint Hardcore' 
		when game_type_description = 'ctf hc' then 'Capture The Flag Hardcore'
		when game_type_description = 'conf hc' then 'Kill Confirmed Hardcore'
		else game_type_description 
        end as game_type 


-- Create a separate playlist Id mapping for PC 

, case when playlist_id = 1 then 'Team Deathmatch'
        when playlist_id = 2 then 'War'
        when playlist_id = 3 then 'Domination'
        when playlist_id = 4 then 'Search and Destroy'
        when playlist_id = 5 then 'Kill Confirmed'
		when playlist_id = 6 then 'Gridiron' 
		when playlist_id = 7 then 'Free-for-all' 
		when playlist_id = 8 then 'Hardpoint' 
		when playlist_id = 9 then 'Capture the Flag' 
		when playlist_id = 18 then 'Team Deathmatch Hardcore' 
		when playlist_id = 19 then 'Domination  Hardcore' 
		when playlist_id = 22 then 'Free-for-all Hardcore'
		when playlist_id = 30 then 'Team Deathmatch Hardcore' 
		when playlist_id = 31 then 'Search and Destroy Hardcore' 
		when playlist_id = 32 then 'Domination Hardcore' 
		when playlist_id = 33 then 'Free-for-all Hardcore' 
		when playlist_id = 40 then 'Carentan 24/7' 
		when playlist_id = 62 then 'Comp Hardpoint' 
		when playlist_id = 63 then 'Comp Capture The Flag' 
		when playlist_id = 64 then 'Comp Search and Destroy'
        else cast(playlist_id as varchar) 
     end as playlist_id 
		
-- Map Descriptions and their Screen Names 

    ,case when a.map_description = 'mp_d_day' then 'Point Du Hoc' 
	      when a.map_description = 'mp_gibraltar_02' then 'Gibraltar' 
	      when a.map_description = 'mp_forest_01' then 'Ardennes Forest' 
	      when a.map_description = 'mp_aachen_v2' then 'Aachen' 
	      when a.map_description = 'mp_carentan_s2' then 'Carentan'
	      when a.map_description = 'mp_wolfslair' then 'Wolf''s Lair'
	      when a.map_description = 'mp_burn_ss' then 'U-Turn'
	      when a.map_description = 'mp_flak_tower' then 'Flaktower'
	      when a.map_description = 'mp_france_village' then 'Sainte Marie du Mont' 
	      when a.map_description = 'mp_canon_farm' then 'Gustav Canon'
	      when a.map_description = 'mp_battleship_2' then 'USS Texas'
	      when a.map_description = 'mp_london' then 'London Docks'
	      when a.map_description = 'mp_paris' then 'Paris' 
		  when a.map_description = 'mp_raid_cobra' then 'Operation Breakout' 
          when a.map_description = 'mp_raid_bulge' then 'Operation Griffin' 	
          when a.map_description = 'mp_raid_aachen' then 'Operation Aachen' 	
          when a.map_description = 'mp_raid_d_day' then 'Operation Neptune' 	  
	      else a.map_description 
	end as map_name 

 	,matchduration/60.0 as duration_total -- Get Match Duration 
	,sum(lives_count) as lives_count -- Total number of Spawns in the game 
	,count(distinct case when disconnect_reason_s like 'EXE_DISCONNECTED%%' then a.context_data_players_client_user_id_l end) as early_quits -- Voluntary Quits 
	,count(distinct case when disconnect_reason_s like 'EXE_DISCONNECTED%%' or disconnect_reason_s in ('EXE_MATCHENDED')  then a.context_data_players_client_user_id_l end) as all_quits -- Voluntary Quits + Match Ended -- SV_MatchEnd, EXE_MATCHENDED
	,sum(kills) as kills 
	,sum(deaths) as deaths
    ,count(distinct a.context_data_players_client_user_id_l) as users 
	,sum(score) as score 
    ,sum(playermatchtime_total_ms_i)/1000 as life_time -- Explore the sanity of playermatchtime_total_i, (Seems Faulty), Excluding it from the analysis for now 
	,sum(total_xp) as xp 

-- Get Play Duration 
	,sum(play_duration) as play_duration
	from 
		( 
		Select distinct dt 
			, context_data_players_client_user_id_l 
			, context_headers_title_id_s as title_id 
			, context_data_match_common_matchid_s as match_id 
			, (utc_disconnect_time_s_i - (case when context_data_match_common_utc_start_time_i > utc_connect_time_s_i then context_data_match_common_utc_start_time_i else utc_connect_time_s_i end)) as play_duration
			, context_data_match_common_utc_start_time_i 
			, context_data_match_common_utc_end_time_i 
--			, utc_first_spawn_timestamp 
            , playermatchtime_total_ms_i 
--			, playermatchtime_start_i 
			, utc_connect_time_s_i
			, disconnect_reason_S 
			, context_data_match_common_map_s as map_description  
			, score_i as score  
			, (end_deaths_i - start_deaths_i) as deaths 
			, (end_kills_i - start_kills_i) as kills  
			, (case when spawns_i = 65535 then 0 else spawns_i end) as lives_count 
			, context_data_match_common_is_private_match_b as private_match_flag 
			, context_data_match_common_playlist_id_i as playlist_id 
			, context_data_match_common_playlist_name_s as playlist_name 
			, total_xp_i as total_xp 
			, client_is_splitscreen_b as split_screen_flag 
			FROM ads_ww2.fact_mp_match_data_players 
			where dt = date '{{DS_DATE_ADD(%s)}}'
			and (end_kills_i - start_kills_i) >=0 
			and (end_deaths_i - start_deaths_i) >=0 
			and score_i between 0 and 15000 
			and total_xp_i between 0 and 40000 
                        and utc_connect_time_s_i > 0
                        and utc_disconnect_time_s_i > 0  
			and utc_disconnect_time_s_i <= utc_connect_time_s_i + 36000
			and spawns_i > 0 
			and context_data_match_common_playlist_id_i <> 0 
			) a 
			
		join 
		   ( 
		    select distinct context_headers_title_id_s as title_id
			 , context_data_match_common_matchid_s 
			 , match_common_map_s as map_description
			 , match_common_gametype_s as game_type_description
			 , context_headers_event_id_s 
			 , match_common_utc_start_time_i 
			 , match_common_utc_end_time_i
			 , match_common_life_count_i 
			 , match_common_player_count_i 
			 , match_common_has_bots_b 
			 , matchduration 
			 ,dt 
			 FROM ads_ww2.fact_mp_match_data 
			 where dt = date '{{DS_DATE_ADD(%s)}}'
			 and context_data_match_common_matchid_s IS NOT NULL 
			 AND match_common_is_private_match_b = FALSE 
			 AND context_headers_title_id_s in ('5597', '5598','5599') 
			 and matchduration > 0 -- https://api.qubole.com/v2/analyze?command_id=104266516 
			) b 
			
		on a.match_id = b.context_data_match_common_matchid_s 
		and a.title_id = b.title_id 
		and a.map_description = b.map_description
		
        group by 1,2,3,4,5,6,7 
		
		) a
		left join as_shared.dim_date_id_date_monday_dev d 
        on a.dt = d.raw_date
        where playlist_id is not null
		group by 1,2,3,4,5,20
) a 
left join war_mode_agg b 
on a.raw_date = b.raw_date
and a.platform = b.description 
and a.game_type = b.game_type 
and a.playlist_id = b.playlist_id 
and a.map_name = b.map_name 

left join agg_war_match_disconnects c 
on a.raw_date = c.raw_date
and a.platform = c.description 
and a.game_type = c.game_type 
and a.playlist_id = c.playlist_id 
and a.map_name = c.map_name""" 
##%(stats_date, stats_date, stats_date, stats_date, stats_date) 

insert_daily_gametype_map_playlist_usage_two_days_task = qubole_operator('daily_gametype_maps_playlist_usage_two_days',
                                              evaluate_queries(insert_daily_gametype_maps_playlist_usage_sql, -1,5), 2, timedelta(seconds=600), dag) 
insert_daily_gametype_map_playlist_usage_one_days_task = qubole_operator('daily_gametype_maps_playlist_usage_one_days',
                                             evaluate_queries(insert_daily_gametype_maps_playlist_usage_sql, 0,5), 2, timedelta(seconds=600), dag) 

# Wire up the DAG , Setting Dependency of the tasks
insert_daily_gametype_map_playlist_usage_two_days_task.set_upstream(start_time_task)
insert_daily_gametype_map_playlist_usage_one_days_task.set_upstream(insert_daily_gametype_map_playlist_usage_two_days_task)

