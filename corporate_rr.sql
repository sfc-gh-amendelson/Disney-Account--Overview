create or replace dynamic table TEMP.AMENDELSON.CORPORATE_RR(
	USAGE_DATE,
	SNOWFLAKE_GROUP_ROLLUP,
	SNOWFLAKE_ACCOUNT_NAMES,
	SNOWFLAKE_ACCOUNT_ALIASES,
	TOTAL_CREDITS,
	ANNUAL_RR_CREDITS,
	ANNUAL_RR_DOLLARS,
	TOTAL_PATCH_SPEND,
	PERC_OF_TOTAL
) target_lag = '1 day' refresh_mode = FULL initialize = ON_CREATE warehouse = SNOWHOUSE
 COMMENT='pulls the rr for corporate accts for the last 30 days'
 as
WITH corp_accts AS (
SELECT 
*
FROM
FINANCE.CUSTOMER.SALESFORCE_SNOWFLAKE_MAPPING
WHERE organization_name = 'DISNEYCORP'
OR organization_name = 'JM73985'
OR salesforce_account_name = 'Disney Worldwide Services, Inc. - Disney Corporate (Main)'
OR organization_name = 'JM73985'
)
, consumption AS (

SELECT
corp_accts.*,
finance.usage_date,
finance.snowflake_primary_deployment_shard,
--finance.snowflake_account_alias,
finance.total_credits
FROM FINANCE.CUSTOMER.USAGE_DAILY finance
JOIN corp_accts 
ON finance.snowflake_deployment = corp_accts.snowflake_deployment
AND finance.snowflake_account_name = corp_accts.snowflake_account_name
--WHERE finance.usage_date >= CURRENT_DATE()-30
--AND finance.usage_date < CURRENT_DATE()
)

, summary AS (
SELECT 
usage_date,
snowflake_account_name,
snowflake_account_alias,
CASE WHEN snowflake_account_alias ILIKE '%DDSI%' THEN 'DDSI'
    WHEN snowflake_account_alias ILIKE '%payment_processor%' THEN 'PAYMENT_PROCESSOR'
    WHEN snowflake_account_alias ILIKE '%HR_ANALYTICS%' THEN 'HR ANALYTICS'
    WHEN snowflake_account_alias ILIKE '%legal%' THEN 'LEGAL'
    WHEN snowflake_account_alias ILIKE '%GIS%' THEN 'GIS'
    WHEN snowflake_account_alias ILIKE '%remediation%' THEN 'GIS'
    WHEN snowflake_account_alias ILIKE '%servicenow%' THEN 'ServiceNow'
    WHEN snowflake_account_alias ILIKE '%CRE%%' THEN 'CRE'
    ELSE snowflake_account_alias
    END AS snowflake_group_rollup,
SUM(total_credits) AS total_credits
FROM consumption
GROUP BY 1,2,3
ORDER BY total_credits DESC
)

, top_10 AS (
SELECT 
    summary.*,
    total_credits * 12 AS annualized_rr,
    SUM(total_credits) OVER() total_april_credits,
    total_credits / SUM(total_credits) OVER() AS perc_of_all_credits
FROM summary
ORDER BY TOTAL_CREDITS DESC
)

, rollup AS (
SELECT 
usage_date,
snowflake_group_rollup,
array_agg(snowflake_account_name) AS snowflake_account_names,
array_agg(snowflake_account_alias) AS snowflake_account_aliases,
SUM(total_credits) as total_credits,
SUM(total_credits)*12 AS annual_rr_credits,
SUM(total_credits)*12*1.68 AS annual_rr_dollars,
FROM top_10
GROUP BY 1,2
ORDER BY 4 DESC
)

SELECT *,
SUM(annual_rr_dollars) OVER() total_patch_spend,
annual_rr_dollars / SUM(annual_rr_dollars) OVER() AS perc_of_total
FROM rollup;