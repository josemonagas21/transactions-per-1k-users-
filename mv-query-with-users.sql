with page as (Select
 date
 , page_title
,  home_category
, post_id 
, page_type
, SUM(page_views) as pageviews
, SUM(page_views_organic) as search_pageviews
, SUM(product_clicks) as prod_clicks
, SUM(transactions) as transactions
, SUM(affiliate_earnings) as affiliate_earnings
, safe_divide(sum(affiliate_earnings),sum(product_clicks)) as epc
, safe_divide(sum(product_clicks),sum(page_views)) as ctr
, safe_divide(sum(transactions),sum(product_clicks)) as conversion
FrOM
  `nyt-wccomposer-prd.wc_data_reporting.page_performance_mv` page
group by 1,2,3,4,5
), 

 prod as (
  select 
  date 
  , post_id
  , primary_category
  , sum(coalesce(retail_revenue,0)) as retail_revenue
  from `nyt-wccomposer-prd.wc_data_reporting.product_performance_mv` 
 group by 1,2,3),

 agents as (
  SELECT 
        DATE(_pt) as date,
        -- pg.agent_id,
        -- COALESCE(CAST(pg.combined_regi_id AS STRING), pg.agent_id) AS user_id,
           COUNT(DISTINCT COALESCE(CAST(pg.combined_regi_id AS STRING), pg.agent_id)) AS users,
        -- pageview_id,
        SUBSTR(wirecutter.asset.id, 3) AS object_id,
        -- CASE
        --     WHEN wirecutter.asset.headline  = '' THEN 'UNKNOWN'
        --     ELSE wirecutter.asset.headline 
        -- END AS page_title
   
    FROM
        nyt-eventtracker-prd.et.page AS pg,  UNNEST(interactions)
      

    WHERE
        DATE(_pt) between '2023-01-01' and '2023-01-14'
        AND source_app LIKE '%wirecutter%'
        AND module.element.name LIKE '%outbound_product%'  -- filtering by only users with a product click 
        GROUP BY 1,3

 )
 SELECT  page.page_title as Page_Title
,  home_category as Home_Category
, page.post_id as Post_ID
, page_type as Page_Type
, sum(pageviews) as Pageviews
-- , COUNT(DISTINCT agents.user_id) as users
, SUM(agents.users) as users 
, SUM(search_pageviews) as Search_pageviews
, sum(prod_clicks) as PClicks
, sum(page.transactions) as Transactions
, sum(affiliate_earnings) as Affiliate_Earnings
, safe_divide(sum(affiliate_earnings),sum(prod_clicks)) as EPC
, safe_divide(sum(prod_clicks),sum(pageviews)) as CTR
, safe_divide(sum(transactions),sum(prod_clicks)) as Conversion
, safe_divide(sum(coalesce(retail_revenue,0)),sum(transactions)) as AOV
, safe_divide(sum(affiliate_earnings),sum(coalesce(retail_revenue,0))) as Commission_Rate
 from page
 left join prod 
 on prod.date = page.date
 and prod.post_id = page.post_id
 left join agents ON page.post_id = agents.object_id AND page.date = agents.date
 where 
  page.date = '2023-01-14' --between '2023-01-01' and '2023-01-14'
  -- '{{start_date}}' and '{{end_date}}'
  -- and ( home_category IN  ( {{home_category}} )  )
  -- and ( page_title IN ( {{page_title}} ) or page_title is null )
  --and prod.post_id is not null
  group by 1,2,3,4
  order by pageviews desc
 