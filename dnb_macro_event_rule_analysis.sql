-------- part 1: pull for all sellers cased by macro event rules (test/manual/control)
--test population
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_test as 
select 
distinct user_token
, case_id
, to_date(CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', created_at)) as case_date
, 'test' as case_type
from app_risk.app_risk.credit_cases_post_ssp
where original_case_type_new like '%dnb_macro_events_test%'
qualify row_number() over(partition by user_token order by created_at) = 1 --there are 5 test rules, a seller could possibly be cased by multiple rules
;
--manual population
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_manual as
select 
distinct target_token as user_token
, case_id
, to_date(created_at) as case_date
, 'manual' as case_type
from regulator.raw_oltp.audit_logs
where COMMENT ILIKE '%#CRsimplifiedscorecard%' --all cased sellers in dnb_macro_events were evaluated by simplified scorecard
and target_token not in (select distinct user_token
                        from app_risk.app_risk_test.dnb_macro_events_rule_sellers_test)
qualify row_number() over(partition by target_token order by created_at) = 1
;
--control population
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_control as 
select 
distinct user_token
, case_id
, to_date(CONVERT_TIMEZONE('America/Los_Angeles', 'UTC', created_at)) as case_date
, 'control' as case_type
from app_risk.app_risk.credit_cases_post_ssp
where original_case_type_new like '%dnb_macro_events_control%'
and to_date(created_at) not in ('2023-02-09','2023-02-10')  --feature engine error caused volume spike up on feb 09 & feb 10 
and user_token not in (select distinct user_token
                        from app_risk.app_risk_test.dnb_macro_events_rule_sellers_test)
and user_token not in (select distinct user_token
                        from app_risk.app_risk_test.dnb_macro_events_rule_sellers_manual)
qualify row_number() over(partition by user_token order by created_at) = 1
;
--all sellers involved in dnb_macro_event analysis
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers as
select * from app_risk.app_risk_test.dnb_macro_events_rule_sellers_test
union all
select * from app_risk.app_risk_test.dnb_macro_events_rule_sellers_manual
union all
select * from app_risk.app_risk_test.dnb_macro_events_rule_sellers_control
;

--check for seller count and no duplicates
select --distinct user_token, count(*)
case_type, count(*), count(distinct user_token), count(distinct case_id)
from app_risk.app_risk_test.dnb_macro_events_rule_sellers
group by 1
order by 1
--order by 2 desc
;
/* @2023-05-30
CASE_TYPE	COUNT(*)	COUNT(DISTINCT USER_TOKEN)
control	119	119
manual	5,127	5,127
test	182	182
*/

-------- part 2: pull actions (SSP/delayed freeze SSP/optional SSP/reserve for all sellers cased by macro event rules (test/manual/control) 
-------- from app_risk.app_risk.shealth_fact_risk_case_actions
-------- join by case_id 
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions as
select s.*
, CASE_SUBROUTE
, CASE_CREATED_AT
, CASE_CLOSED_AT
, MANUAL_FREEZE_AT
, DELAYED_SSP_FREEZE_AT	
, MIN_FREEZE_AT
, MAX_FREEZE_AT	
, MANUAL_UNFREEZE_AT
, MIN_NEAREST_MANUAL_UNFREEZE_AT
, MAX_NEAREST_MANUAL_UNFREEZE_AT
, MIN_UNFREEZE_AT
, MAX_UNFREEZE_AT
, RESERVE_ENABLED_AT
, RESERVE_DISABLED_AT
, SSP_TOKEN
, SSP_REQUEST_TYPE
, SSP_SENT_AT
, SSP_FIRST_RESPONSE_AT
, SSP_LAST_RESPONSE_AT	
, SSP_REVIEWED_AT
, SSP_TOUCHPOINTS
, SSP_REVIEWED_STATUS
from app_risk.app_risk_test.dnb_macro_events_rule_sellers s
left join app_risk.app_risk.shealth_fact_risk_case_actions ca
on s.case_id = ca.case_id 
order by case_date, user_token
;

/*sample check for some sellers/cases
select 
distinct case_id
, al.target_token
, action_name
, comment
, al.updated_at
, al.created_at
, r.request_target_token
, r.updated_at
, r.created_at
, request_type
, r.id
from regulator.raw_oltp.audit_logs al
left join secure_profile.raw_oltp.requests r 
ON r.request_target_token = al.target_token
--where case_id = '120551641'
where case_id = '115304958'
--and r.created_at >='2023-04-18'
order by al.updated_at
--where comment like '%optional%'
--limit 5
;*/

-------- part 3: pull performance
-- pre and post gpv for cased sellers
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions_gpv as
select 
distinct s.*
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_date) between 0 and 30 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_30d_gpv_dllr
, sum(case when datediff(day, s.case_closed_at, dps.payment_trx_recognized_date) between 0 and 30 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_30d_gpv_dllr
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_date) between 0 and 60 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_60d_gpv_dllr
, sum(case when datediff(day, s.case_closed_at, dps.payment_trx_recognized_date) between 0 and 60 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_60d_gpv_dllr 
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_date) between 0 and 90 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_90d_gpv_dllr
, sum(case when datediff(day, s.case_closed_at, dps.payment_trx_recognized_date) between 0 and 90 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_90d_gpv_dllr 
, sum(case when datediff(day, dps.payment_trx_recognized_date, s.case_date) between 0 and 180 then dps.gpv_payment_amount_base_unit else 0 end)/100 as pre_180d_gpv_dllr
, sum(case when datediff(day, s.case_closed_at, dps.payment_trx_recognized_date) between 0 and 180 then dps.gpv_payment_amount_base_unit else 0 end)/100 as post_180d_gpv_dllr 
from app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions s
left join app_bi.hexagon.vagg_seller_daily_payment_summary dps
on s.user_token = dps.user_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
;

-- pre and post cb and loss for cased sellers by cb arrival date
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions_cb_loss_arr_dt as
select 
distinct l.*
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 30 then cb.cb_dllr else 0 end) as pre_30d_cb_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 30  then cb.cb_dllr else 0 end) as post_30d_cb_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 30 then cb.loss_dllr else 0 end) as pre_30d_loss_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 30  then cb.loss_dllr else 0 end) as post_30d_loss_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 60 then cb.cb_dllr else 0 end) as pre_60d_cb_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 60  then cb.cb_dllr else 0 end) as post_60d_cb_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 60 then cb.loss_dllr else 0 end) as pre_60d_loss_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 60  then cb.loss_dllr else 0 end) as post_60d_loss_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 90 then cb.cb_dllr else 0 end) as pre_90d_cb_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 90  then cb.cb_dllr else 0 end) as post_90d_cb_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 90 then cb.loss_dllr else 0 end) as pre_90d_loss_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 90  then cb.loss_dllr else 0 end) as post_90d_loss_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 180 then cb.cb_dllr else 0 end) as pre_180d_cb_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 180  then cb.cb_dllr else 0 end) as post_180d_cb_dllr_arr_dt
, sum(case when datediff(day, cb.cb_date, l.case_date) between 0 and 180 then cb.loss_dllr else 0 end) as pre_180d_loss_dllr_arr_dt
, sum(case when datediff(day, l.case_closed_at, cb.cb_date) between 0 and 180  then cb.loss_dllr else 0 end) as post_180d_loss_dllr_arr_dt
from app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions l
left join (select 
			distinct user_token
			, to_date(chargeback_date) as cb_date
            , sum(chargeback_cents)/100 as cb_dllr
            , sum(loss_cents)/100 as loss_dllr
			from app_risk.app_risk.chargebacks
            group by 1,2) cb
on l.user_token = cb.user_token
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
;

-- pre and post cb and loss for cased sellers by payment date
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions_cb_loss_pmt_dt as
select 
distinct l.*
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 30 then cb.cb_dllr else 0 end) as pre_30d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 30  then cb.cb_dllr else 0 end) as post_30d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 30 then cb.loss_dllr else 0 end) as pre_30d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 30  then cb.loss_dllr else 0 end) as post_30d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 60 then cb.cb_dllr else 0 end) as pre_60d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 60  then cb.cb_dllr else 0 end) as post_60d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 60 then cb.loss_dllr else 0 end) as pre_60d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 60  then cb.loss_dllr else 0 end) as post_60d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 90 then cb.cb_dllr else 0 end) as pre_90d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 90  then cb.cb_dllr else 0 end) as post_90d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 90 then cb.loss_dllr else 0 end) as pre_90d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 90  then cb.loss_dllr else 0 end) as post_90d_loss_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 180 then cb.cb_dllr else 0 end) as pre_180d_cb_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 180  then cb.cb_dllr else 0 end) as post_180d_cb_dllr_pmt_dt
, sum(case when datediff(day, cb.pmt_date, l.case_date) between 0 and 180 then cb.loss_dllr else 0 end) as pre_180d_loss_dllr_pmt_dt
, sum(case when datediff(day, l.case_closed_at, cb.pmt_date) between 0 and 180  then cb.loss_dllr else 0 end) as post_180d_loss_dllr_pmt_dt
from app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions l
left join (select 
			distinct user_token
			, to_date(payment_created_at) as pmt_date
            , sum(chargeback_cents)/100 as cb_dllr
            , sum(loss_cents)/100 as loss_dllr
			from app_risk.app_risk.chargebacks
            group by 1,2) cb
on l.user_token = cb.user_token
group by  1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
;

-- -- pre and post pofit for cased sellers 
create or replace table app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions_profit as
select 
distinct l.*
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 30 then p.gross_profit_processing else 0 end)/100 as pre_30d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 30 then p.gross_profit_processing else 0 end)/100 as post_30d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 60 then p.gross_profit_processing else 0 end)/100 as pre_60d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 60 then p.gross_profit_processing else 0 end)/100 as post_60d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 90 then p.gross_profit_processing else 0 end)/100 as pre_90d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 90 then p.gross_profit_processing else 0 end)/100 as post_90d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 180 then p.gross_profit_processing else 0 end)/100 as pre_180d_gross_profit_processing_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 180 then p.gross_profit_processing else 0 end)/100 as post_180d_gross_profit_processing_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 30 then p.gross_profit else 0 end)/100 as pre_30d_gross_profit_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 30 then p.gross_profit else 0 end)/100 as post_30d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 60 then p.gross_profit else 0 end)/100 as pre_60d_gross_profit_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 60 then p.gross_profit else 0 end)/100 as post_60d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 90 then p.gross_profit else 0 end)/100 as pre_90d_gross_profit_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 90 then p.gross_profit else 0 end)/100 as post_90d_gross_profit_dllr
, sum(case when datediff(day, p.report_date, l.case_date) between 0 and 180 then p.gross_profit else 0 end)/100 as pre_180d_gross_profit_dllr
, sum(case when datediff(day, l.case_closed_at, p.report_date) between 0 and 180 then p.gross_profit else 0 end)/100 as post_9180d_gross_profit_dllr
from app_risk.app_risk_test.dnb_macro_events_rule_sellers_actions l
left join app_risk.app_risk.seller_profit_daily_v1 p
on l.user_token = p.user_token
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26
;
