-------- part 1: pull for all sellers cased by macro event rules (test/manual/control)
--test population
create or replace table app_risk.app_risk_test.dnb_macro_sellers_crss_v2 as
select 
distinct al.case_id
, 'test' as case_type
, case when r.case_id is not null then 1 else 0 end as rule_flagged
, min(to_date(created_at)) as case_date
from regulator.raw_oltp.audit_logs al
left join (select distinct case_id
            from app_risk.app_risk.credit_cases_post_ssp
            where original_case_type_new like '%dnb_macro_events_test%') r
on al.case_id = r.case_id
where al.case_id in (select distinct case_id
                    from regulator.raw_oltp.audit_logs
                    where COMMENT ILIKE '%#CRsimplifiedscorecard%')
and al.case_id not in (select distinct case_id
                    from regulator.raw_oltp.audit_logs
                    where "from" ilike '%ann_gpv_25k_400k_pd_gt_7pct_f12m_profit_lt_0_bpo%') --exclude this new rule; started casing on 2023-06-05 and also got evaluated by simplified scorecard
and target_token is not null
group by 1,2,3
;

--control population
create or replace table app_risk.app_risk_test.dnb_macro_sellers_control_v2 as 
select 
distinct case_id
, 'control' as case_type
, 0 as rule_flagged
, to_date(CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', created_at)) as case_date
from app_risk.app_risk.credit_cases_post_ssp ssp
left join (select distinct target_token as user_token
            from regulator.raw_oltp.audit_logs
            where case_id in (select distinct case_id 
                            from app_risk.app_risk_test.dnb_macro_events_rule_sellers_crss_v2)) crss
on ssp.user_token = crss.user_token
where original_case_type_new like '%dnb_macro_events_control%'
and to_date(created_at) not in ('2023-02-09','2023-02-10')  --feature engine error caused volume spike up on feb 09 & feb 10 
and crss.user_token is null
qualify row_number() over(partition by ssp.user_token order by created_at) = 1
;

--all sellers involved in dnb_macro_event analysis
create or replace table app_risk.app_risk_test.dnb_macro_sellers_v2 as
select * from app_risk.app_risk_test.dnb_macro_sellers_crss_v2
union all
select * from app_risk.app_risk_test.dnb_macro_sellers_control_v2
;

select count(*), count(distinct case_id), sum(rule_flagged)
from app_risk.app_risk_test.dnb_macro_sellers_v2 
;
COUNT(*)	COUNT(DISTINCT CASE_ID)	SUM(RULE_FLAGGED)
5,832	5,832	182
;

create or replace table app_risk.app_risk_test.dnb_macro_sellers_actions_load1_v2 as
select s.*
, USER_TOKEN
, CASE_SUBROUTE
, CASE_CREATED_AT
, CASE_CLOSED_AT
, MIN_FREEZE_AT
, MAX_FREEZE_AT
, MIN_UNFREEZE_AT
, MAX_UNFREEZE_AT
, RESERVE_ENABLED_AT
, SSP_TOKEN
, SSP_REQUEST_TYPE
, SSP_SENT_AT
, SSP_FIRST_RESPONSE_AT
, SSP_REVIEWED_STATUS
from app_risk.app_risk_test.dnb_macro_sellers_v2 s
left join app_risk.app_risk.shealth_fact_risk_case_actions ca
on s.case_id = ca.case_id 
order by case_date, user_token
;

create or replace table app_risk.app_risk_test.dnb_macro_sellers_actions_load2_v2 as
select
distinct user_token
, case_type
, max(rule_flagged) rule_flagged
, min(case_date) case_date
, min(case_created_at) case_created_at
, max(case_closed_at) case_closed_at
, min(min_freeze_at) min_freeze_at
, min(reserve_enabled_at) reserve_enabled_at
, max(ssp_request_type) ssp_request_type
, min(ssp_sent_at) ssp_sent_at
, min(ssp_first_response_at) ssp_first_response_at
from app_risk.app_risk_test.dnb_macro_sellers_actions_load1_v2
group by 1,2
;

select count(*), count(distinct case_id), count(distinct user_token), sum(rule_flagged)
from app_risk.app_risk_test.dnb_macro_sellers_actions_load1_v2 
;
COUNT(*)	COUNT(DISTINCT CASE_ID)	COUNT(DISTINCT USER_TOKEN)	SUM(RULE_FLAGGED)
5,832	5,832	5,718	182
;

select count(*), count(distinct user_token), sum(rule_flagged)
from app_risk.app_risk_test.dnb_macro_sellers_actions_load2_v2
;
COUNT(*)	COUNT(DISTINCT USER_TOKEN)	SUM(RULE_FLAGGED)
5,719	5,718	172
;
select case_type, count(*), count(distinct user_token)
from app_risk.app_risk_test.dnb_macro_sellers_actions_load2_v2
group by 1
--limit 3
;
CASE_TYPE	COUNT(*)	COUNT(DISTINCT USER_TOKEN)
control	121	121
test	5,598	5,597
;

-------- test vs control analysis -------
-- action rate and completion rate after case created
create or replace table app_risk.app_risk_test.test_control_analysis_load1_v2 as
select 
distinct user_token
, case_type
, rule_flagged
, case_created_at
, case_closed_at
, ssp_sent_at
, min_freeze_at
, reserve_enabled_at
, date_trunc(month, case_created_at) as case_month
, case when ssp_sent_at is not null then 1 else 0 end as ssp_ind
, case when ssp_first_response_at is not null then 1 else 0 end as ssp_completion_ind
, case when min_freeze_at is not null then 1 else 0 end as freeze_ind
, case when reserve_enabled_at is not null then 1 else 0 end as reserve_ind
, case when (reserve_enabled_at is not null or min_freeze_at is not null) then 1 else 0 end as action_ind
, case when (ssp_completion_ind =1 and action_ind = 1 and (ssp_first_response_at > min_freeze_at or ssp_first_response_at > reserve_enabled_at)) then 1 else 0 end as ssp_completion_after_action
from app_risk.app_risk_test.dnb_macro_sellers_actions_load2_v2
where case_created_at is not null
;

-- pre and post gpv after case created
create or replace table app_risk.app_risk_test.test_control_analysis_load2_gpv_v2 as
select 
distinct s.*
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_created_at) between 0 and 30 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_30d_gpv_dllr
, sum(case when datediff(day, s.case_created_at, dps.payment_trx_recognized_date) between 1 and 31 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_30d_gpv_dllr
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_created_at) between 0 and 60 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_60d_gpv_dllr
, sum(case when datediff(day, s.case_created_at, dps.payment_trx_recognized_date) between 1 and 61 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_60d_gpv_dllr 
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_created_at) between 0 and 90 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_90d_gpv_dllr
, sum(case when datediff(day, s.case_created_at, dps.payment_trx_recognized_date) between 1 and 91 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_90d_gpv_dllr 
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_created_at) between 0 and 180 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_180d_gpv_dllr
, sum(case when datediff(day, s.case_created_at, dps.payment_trx_recognized_date) between 1 and 181 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_180d_gpv_dllr 
from app_risk.app_risk_test.test_control_analysis_load1_v2 s
left join app_bi.hexagon.vagg_seller_daily_payment_summary dps
on s.user_token = dps.user_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
;

-- pre and post cb and loss after case created
create or replace table app_risk.app_risk_test.test_control_analysis_load3_loss_v2 as
select 
distinct l.*
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 30 then cb.cb_dllr else 0 end) as pre_30d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 31  then cb.cb_dllr else 0 end) as post_30d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 30 then cb.loss_dllr else 0 end) as pre_30d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 31  then cb.loss_dllr else 0 end) as post_30d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 60 then cb.cb_dllr else 0 end) as pre_60d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 61  then cb.cb_dllr else 0 end) as post_60d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 60 then cb.loss_dllr else 0 end) as pre_60d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 61  then cb.loss_dllr else 0 end) as post_60d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 90 then cb.cb_dllr else 0 end) as pre_90d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 91  then cb.cb_dllr else 0 end) as post_90d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 90 then cb.loss_dllr else 0 end) as pre_90d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 91  then cb.loss_dllr else 0 end) as post_90d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 180 then cb.cb_dllr else 0 end) as pre_180d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 181  then cb.cb_dllr else 0 end) as post_180d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_created_at) between 0 and 180 then cb.loss_dllr else 0 end) as pre_180d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_created_at, cb.pmt_date) between 1 and 181  then cb.loss_dllr else 0 end) as post_180d_loss_dllr_pmt_dt
from app_risk.app_risk_test.test_control_analysis_load2_gpv_v2 l
left join (select 
			distinct user_token
			, to_date(payment_created_at) as pmt_date
            , sum(chargeback_cents)/100 as cb_dllr
            , sum(loss_cents)/100 as loss_dllr
			from app_risk.app_risk.chargebacks
            group by 1,2) cb
on l.user_token = cb.user_token
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
;

-- -- pre and post pofit after ssp sent
create or replace table app_risk.app_risk_test.test_control_analysis_load4_profit_v2 as
select 
distinct l.*
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 30 then p.gross_profit_processing else 0 end)/100 as pre_30d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 31 then p.gross_profit_processing else 0 end)/100 as post_30d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 60 then p.gross_profit_processing else 0 end)/100 as pre_60d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 61 then p.gross_profit_processing else 0 end)/100 as post_60d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 90 then p.gross_profit_processing else 0 end)/100 as pre_90d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 91 then p.gross_profit_processing else 0 end)/100 as post_90d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 180 then p.gross_profit_processing else 0 end)/100 as pre_180d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 181 then p.gross_profit_processing else 0 end)/100 as post_180d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 30 then p.gross_profit else 0 end)/100 as pre_30d_gross_profit_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 31 then p.gross_profit else 0 end)/100 as post_30d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 60 then p.gross_profit else 0 end)/100 as pre_60d_gross_profit_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 61 then p.gross_profit else 0 end)/100 as post_60d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 90 then p.gross_profit else 0 end)/100 as pre_90d_gross_profit_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 91 then p.gross_profit else 0 end)/100 as post_90d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.case_created_at) between 0 and 180 then p.gross_profit else 0 end)/100 as pre_180d_gross_profit_dllr
, sum(case when datediff(day, l.case_created_at, p.report_date) between 1 and 181 then p.gross_profit else 0 end)/100 as post_180d_gross_profit_dllr
from app_risk.app_risk_test.test_control_analysis_load3_loss_v2 l
left join app_risk.app_risk.seller_profit_daily_v1 p
on l.user_token = p.user_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39
;

select count(*), count(distinct user_token), sum(rule_flagged)
from app_risk.app_risk_test.test_control_analysis_load4_profit_v2
;
COUNT(*)	COUNT(DISTINCT USER_TOKEN)	SUM(RULE_FLAGGED)
5,718	5,718	172
;
select case_type, count(*), count(distinct user_token)
from app_risk.app_risk_test.test_control_analysis_load4_profit_v2
group by 1
--limit 3
;
CASE_TYPE	COUNT(*)	COUNT(DISTINCT USER_TOKEN)
control	121	121
test	5,597	5,597
;


---------- optional ssp vs delayed ssp analysis --------
-- action rate and completion rate after ssp sent
create or replace table app_risk.app_risk_test.ssp_analysis_load1_v2 as
select 
distinct user_token
, case_type
, rule_flagged
, ssp_request_type
, ssp_sent_at
, ssp_first_response_at
, min_freeze_at
, reserve_enabled_at
, date_trunc(month, ssp_sent_at) as ssp_month
, case when ssp_first_response_at is not null then 1 else 0 end as ssp_completion_ind
, datediff(day, ssp_sent_at, ssp_first_response_at) as ssp_completion_day
, case when ssp_completion_day <= 7 then 1 else 0 end as ssp_completion_7d
, case when ssp_completion_day <= 30 then 1 else 0 end as ssp_completion_30d
, case when min_freeze_at is not null then 1 else 0 end as freeze_ind
, case when reserve_enabled_at is not null then 1 else 0 end as reserve_ind
, case when (reserve_enabled_at is not null or min_freeze_at is not null) then 1 else 0 end as action_ind
, case when (ssp_completion_ind =1 and action_ind = 1 and (ssp_first_response_at > min_freeze_at or ssp_first_response_at > reserve_enabled_at)) then 1 else 0 end as ssp_completion_after_action
from app_risk.app_risk_test.dnb_macro_sellers_actions_load2_v2
where ssp_sent_at is not null
;

-- pre and post gpv after ssp sent
create or replace table app_risk.app_risk_test.ssp_analysis_load2_gpv_v2 as
select 
distinct s.*
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.SSP_SENT_AT) between 0 and 30 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_30d_gpv_dllr
, sum(case when datediff(day, s.SSP_SENT_AT, dps.payment_trx_recognized_date) between 1 and 31 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_30d_gpv_dllr
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.SSP_SENT_AT) between 0 and 60 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_60d_gpv_dllr
, sum(case when datediff(day, s.SSP_SENT_AT, dps.payment_trx_recognized_date) between 1 and 61 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_60d_gpv_dllr 
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.SSP_SENT_AT) between 0 and 90 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_90d_gpv_dllr
, sum(case when datediff(day, s.SSP_SENT_AT, dps.payment_trx_recognized_date) between 1 and 91 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_90d_gpv_dllr 
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.SSP_SENT_AT) between 0 and 180 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_180d_gpv_dllr
, sum(case when datediff(day, s.SSP_SENT_AT, dps.payment_trx_recognized_date) between 1 and 181 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_180d_gpv_dllr 
from app_risk.app_risk_test.ssp_analysis_load1_v2 s
left join app_bi.hexagon.vagg_seller_daily_payment_summary dps
on s.user_token = dps.user_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
;

-- pre and post cb and loss after ssp sent
create or replace table app_risk.app_risk_test.ssp_analysis_load3_loss_v2 as
select 
distinct l.*
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 30 then cb.cb_dllr else 0 end) as pre_30d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 31  then cb.cb_dllr else 0 end) as post_30d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 30 then cb.loss_dllr else 0 end) as pre_30d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 31  then cb.loss_dllr else 0 end) as post_30d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 60 then cb.cb_dllr else 0 end) as pre_60d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 61  then cb.cb_dllr else 0 end) as post_60d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 60 then cb.loss_dllr else 0 end) as pre_60d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 61  then cb.loss_dllr else 0 end) as post_60d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 90 then cb.cb_dllr else 0 end) as pre_90d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 91  then cb.cb_dllr else 0 end) as post_90d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 90 then cb.loss_dllr else 0 end) as pre_90d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 91  then cb.loss_dllr else 0 end) as post_90d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 180 then cb.cb_dllr else 0 end) as pre_180d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 181  then cb.cb_dllr else 0 end) as post_180d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.SSP_SENT_AT) between 0 and 180 then cb.loss_dllr else 0 end) as pre_180d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.SSP_SENT_AT, cb.pmt_date) between 1 and 181  then cb.loss_dllr else 0 end) as post_180d_loss_dllr_pmt_dt
from app_risk.app_risk_test.ssp_analysis_load2_gpv_v2 l
left join (select 
			distinct user_token
			, to_date(payment_created_at) as pmt_date
            , sum(chargeback_cents)/100 as cb_dllr
            , sum(loss_cents)/100 as loss_dllr
			from app_risk.app_risk.chargebacks
            group by 1,2) cb
on l.user_token = cb.user_token
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25
;

-- -- pre and post pofit after ssp sent
create or replace table app_risk.app_risk_test.ssp_analysis_load4_profit_v2 as
select 
distinct l.*
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 30 then p.gross_profit_processing else 0 end)/100 as pre_30d_gross_profit_processing_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 31 then p.gross_profit_processing else 0 end)/100 as post_30d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 60 then p.gross_profit_processing else 0 end)/100 as pre_60d_gross_profit_processing_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 61 then p.gross_profit_processing else 0 end)/100 as post_60d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 90 then p.gross_profit_processing else 0 end)/100 as pre_90d_gross_profit_processing_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 91 then p.gross_profit_processing else 0 end)/100 as post_90d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 180 then p.gross_profit_processing else 0 end)/100 as pre_180d_gross_profit_processing_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 181 then p.gross_profit_processing else 0 end)/100 as post_180d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 30 then p.gross_profit else 0 end)/100 as pre_30d_gross_profit_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 31 then p.gross_profit else 0 end)/100 as post_30d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 60 then p.gross_profit else 0 end)/100 as pre_60d_gross_profit_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 61 then p.gross_profit else 0 end)/100 as post_60d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 90 then p.gross_profit else 0 end)/100 as pre_90d_gross_profit_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 91 then p.gross_profit else 0 end)/100 as post_90d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.SSP_SENT_AT) between 0 and 180 then p.gross_profit else 0 end)/100 as pre_180d_gross_profit_dllr
, sum(case when datediff(day, l.SSP_SENT_AT, p.report_date) between 1 and 181 then p.gross_profit else 0 end)/100 as post_180d_gross_profit_dllr
from app_risk.app_risk_test.ssp_analysis_load3_loss_v2 l
left join app_risk.app_risk.seller_profit_daily_v1 p
on l.user_token = p.user_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41
;

select count(*), count(distinct user_token), sum(rule_flagged)
from app_risk.app_risk_test.ssp_analysis_load4_profit_v2
;
COUNT(*)	COUNT(DISTINCT USER_TOKEN)	SUM(RULE_FLAGGED)
4,863	4,863	164
;


---------- pull raw data for results
select * 
from app_risk.app_risk_test.test_control_analysis_load4_profit_v2
;

select *
from app_risk.app_risk_test.ssp_analysis_load4_profit_v2
;
