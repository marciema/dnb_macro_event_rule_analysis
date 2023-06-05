create or replace temp table optional_ssp_analysis as
select 
distinct user_token
, ssp_request_type
, ssp_sent_at
, ssp_first_response_at
, date_trunc(month, ssp_sent_at) as ssp_month
, case when ssp_first_response_at is null then 0 else 1 end as ssp_completion_ind
, datediff(day, ssp_sent_at, ssp_first_response_at) as ssp_completion_day
, case when ssp_completion_day <= 7 then 1 else 0 end as ssp_completion_7d
, case when ssp_completion_day <= 30 then 1 else 0 end as ssp_completion_30d
, case when min_freeze_at is null then 0 else 1 end as freeze_ind
, case when reserve_enabled_at is null then 0 else 1 end as reserve_ind
, case when (reserve_enabled_at is not null or min_freeze_at is not null) then 1 else 0 end as action_ind
, case when (ssp_completion_ind =1 and action_ind = 1 and (ssp_first_response_at > min_freeze_at or ssp_first_response_at > reserve_enabled_at)) then 1 else 0 end as ssp_completion_after_action
from app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions
where ssp_sent_at <= '2023-04-30'
;
