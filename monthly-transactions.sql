-- Sample data
-- CREATE TABLE `nyt-bigquery-beta-workspace.jose_data.monthly-transactions`
-- PARTITION BY fact_date
-- AS
INSERT  `nyt-bigquery-beta-workspace.jose_data.monthly-transactions`

with page as (Select
 DATE_TRUNC(date, MONTH) as fact_date
--  , page_title
-- ,  home_category
-- , post_id 
-- , page_type
, SUM(page_views) as pageviews
-- , SUM(page_views_organic) as search_pageviews
, SUM(product_clicks) as prod_clicks
, SUM(transactions) as transactions
, SUM(affiliate_earnings) as affiliate_earnings
, safe_divide(sum(affiliate_earnings),sum(product_clicks)) as epc
, safe_divide(sum(product_clicks),sum(page_views)) as ctr
, safe_divide(sum(transactions),sum(product_clicks)) as conversion
FrOM
  `nyt-wccomposer-prd.wc_data_reporting.page_performance_mv` page
  where date between '2021-01-01' and '2021-12-31'
group by 1),

users as (
  SELECT 
        DATE_TRUNC(DATE(_pt), MONTH) as date,
        -- pg.agent_id,
           COUNT(DISTINCT COALESCE(CAST(pg.combined_regi_id AS STRING), pg.agent_id)) AS users,
          --  COALESCE(CAST(pg.combined_regi_id AS STRING), pg.agent_id) AS users, 
    FROM
        nyt-eventtracker-prd.et.page AS pg
      
    WHERE
        DATE(_pt) between '2021-01-01' and '2021-12-31'
        AND source_app LIKE '%wirecutter%'
        GROUP BY 1
-- )
-- users as(
--   SELECT 
--   DATE_TRUNC(date,MONTH) AS date,
--   COUNT(DISTINCT user_id) as users ,
--   -- COUNT(DISTINCT pageview_id) pageviews
-- FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel`
-- where date between '2021-01-01' and '2021-12-31'
-- GROUP BY 1
-- -- ORDER BY 2
)
SELECT 
    * EXCEPT (date)
FROM page p
join users u on p.fact_date = u.date



