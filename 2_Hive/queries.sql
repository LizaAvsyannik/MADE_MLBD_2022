-- # 1 max number of scrobbles
SELECT artist_mb FROM artists
WHERE scrobbles_lastfm IN (SELECT max(scrobbles_lastfm) FROM artists);

-- # 2 most popular track on last fm
WITH
tags AS ( 
    SELECT explode(split(tags_lastfm,';')) AS `tag`
    FROM  artists
)
SELECT tag, count(tag) as cnt FROM tags
WHERE tag != ''
GROUP BY tag
ORDER BY cnt DESC
LIMIT 1;

-- # 3 most popular artist of 10 most popular last fm tags
WITH
artist_listeners_tag AS (
  SELECT artist_lastfm, listeners_lastfm, tmp_tag AS tag
  FROM artists
  LATERAL VIEW explode(split(tags_lastfm, ';')) tmp_tag_table AS tmp_tag
),
most_popular_tags AS (
  SELECT tag, COUNT(tag) AS tag_cnt
  FROM artist_listeners_tag
  WHERE tag != ''
  GROUP BY tag
  ORDER BY tag_cnt DESC
  LIMIT 10
),
most_popular_artists AS (
  SELECT artist_lastfm, listeners_lastfm
  FROM artist_listeners_tag
  WHERE tag IN (SELECT tag FROM most_popular_tags)
  ORDER BY listeners_lastfm DESC
  LIMIT 10
 )
 SELECT DISTINCT artist_lastfm FROM most_popular_artists;

 -- # 4 countries for 200 most scrobbled artists
WITH 
most_scrobbled_artists AS (
    SELECT country_lastfm, artist_lastfm, scrobbles_lastfm FROM artists
    ORDER BY scrobbles_lastfm DESC
    LIMIT 200
)
SELECT country_lastfm, count(country_lastfm) AS cnt
FROM most_scrobbled_artists
WHERE country_lastfm != ''
GROUP BY country_lastfm
ORDER BY cnt DESC;
