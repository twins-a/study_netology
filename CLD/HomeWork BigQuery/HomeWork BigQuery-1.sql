SELECT
  referer,
  count(trafficSource.source) AS count_visits 
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`, UNNEST(hits) AS hits
WHERE
  -- _TABLE_SUFFIX BETWEEN '20160101' AND '20161231'
  _TABLE_SUFFIX BETWEEN '20160801' AND '20161231'   -- данные присутствуют только с 01.08.2016, поэтому фильтрацию можно указать или с 01.01.2016 или с 01.08.2016
  AND referer IS NOT NULL
GROUP BY
    1
ORDER BY
    count_visits DESC
LIMIT
    10