-- Sample data
CREATE TABLE `nyt-bigquery-beta-workspace.jose_data.weekly-transactions-etsor`
PARTITION BY week_end
AS
-- INSERT  `nyt-bigquery-beta-workspace.jose_data.weekly-transactions`

with page as (Select
 DATE_TRUNC(date, WEEK(MONDAY)) + 6 as week_end
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
--   `nyt-wccomposer-prd.wc_data_reporting.page_performance_mv` page --GA-MV
`nyt-wccomposer-prd.wc_data_reporting.etsor_page_performance_mv` page -- ETSOR-MV
  where date between '2021-01-04' and '2023-02-19'
group by 1),

-- users as (
--   SELECT 
--         DATE_TRUNC(DATE(_pt), WEEK(MONDAY)) + 6 as week_end,
--         -- pg.agent_id,
--            COUNT(DISTINCT COALESCE(CAST(pg.combined_regi_id AS STRING), pg.agent_id)) AS users,
--           --  COALESCE(CAST(pg.combined_regi_id AS STRING), pg.agent_id) AS users, 
--     FROM
--         nyt-eventtracker-prd.et.page AS pg
      
--     WHERE
--         DATE(_pt) between '2021-01-04' and '2023-02-19'
--         AND source_app LIKE '%wirecutter%'
--         GROUP BY 1
-- ),
users as(
  SELECT 
  DATE_TRUNC(date, WEEK(MONDAY)) + 6 as week_end,
  COUNT(DISTINCT user_id) as users ,
  -- COUNT(DISTINCT pageview_id) pageviews
FROM `nyt-bigquery-beta-workspace.wirecutter_data.channel`
where date between '2021-01-04' and '2023-02-19'
GROUP BY 1
-- ORDER BY 2
)
SELECT 
      p.week_end,
      users,
      -- users_c,
      pageviews,
      ROUND(safe_divide(SUM(pageviews),SUM(users)/1000),3) as pvs_per_1k_users,
      prod_clicks,
      ROUND(safe_divide(SUM(prod_clicks),SUM(users)/1000),3) as clicks_per_1k_users,
      transactions,
      ROUND(safe_divide(SUM(transactions),SUM(users)/1000),3) as transactions_per_1k_users,
      affiliate_earnings,
      epc,
      ROUND(ctr,3) as ctr,
      ROUND(conversion,3) as conversion,
FROM page p
-- join users u on p.week_end = u.week_end
join users uc on uc.week_end = p.week_end
group by 1,2,3,5,7,9,10,11,12
-- ORDER BY 1


