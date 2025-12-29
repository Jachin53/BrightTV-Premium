SELECT DISTINCT
  A.*,
  B.*,

  -- Convert RecordDate2 into a proper timestamp
  TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI') AS sale_timestamp,

  -- Extract temporal components (replace NULLs with 'None' for text, 0 for numbers)
  COALESCE(DAYNAME(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')), 'None') AS day_of_week,
  COALESCE(HOUR(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')), 0) AS hour_of_day,
  COALESCE(MONTHNAME(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')), 'None') AS month_name,
  COALESCE(YEAR(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')), 0) AS year,
  -- Classify Age and provide description
  CASE 
        WHEN age BETWEEN 0 AND 17 THEN 'Under 18'
        WHEN age BETWEEN 18 AND 24 THEN 'Young Adult'
        WHEN age BETWEEN 25 AND 34 THEN 'Adult'
        WHEN age BETWEEN 35 AND 44 THEN 'Mid-Aged-Adult'
        WHEN age BETWEEN 45 AND 54 THEN 'Mature Adult'
        ELSE 'Senior'
          END  AS Age_Bucket,

  -- Classify day as Weekday or Weekend
  COALESCE(
    CASE
      WHEN DAYOFWEEK(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) IN (1, 7) THEN 'Weekend'
      ELSE 'Weekday'
    END, 'None'
  ) AS day_classification,

  -- Classify season based on month
  COALESCE(
    CASE
      WHEN MONTH(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) IN (12, 1, 2) THEN 'Summer'
      WHEN MONTH(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) IN (3, 4, 5) THEN 'Autumn'
      WHEN MONTH(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) IN (6, 7, 8) THEN 'Winter'
      ELSE 'Spring'
    END, 'None'
  ) AS season_classification,

  -- Classify time of day into buckets
  COALESCE(
    CASE
      WHEN HOUR(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) BETWEEN 0 AND 5 THEN 'Late Night'
      WHEN HOUR(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) BETWEEN 6 AND 11 THEN 'Morning'
      WHEN HOUR(TRY_TO_TIMESTAMP(B.RecordDate2, 'YYYY/MM/DD HH24:MI')) BETWEEN 12 AND 17 THEN 'Afternoon'
      ELSE 'Evening'
    END, 'None'
  ) AS daytype_classification,

  -- Convert Duration2 to total seconds
  COALESCE(
    DATE_PART('HOUR', TO_TIME(B.Duration2)) * 3600 +
    DATE_PART('MINUTE', TO_TIME(B.Duration2)) * 60 +
    DATE_PART('SECOND', TO_TIME(B.Duration2)), 0
  ) AS duration_seconds,

  -- Convert Duration2 to total minutes
  COALESCE(
    DATE_PART('HOUR', TO_TIME(B.Duration2)) * 60 +
    DATE_PART('MINUTE', TO_TIME(B.Duration2)) +
    DATE_PART('SECOND', TO_TIME(B.Duration2)) / 60, 0
  ) AS duration_minutes,

  -- Classify duration into engagement buckets
  COALESCE(
    CASE 
      WHEN DATE_PART('HOUR', TO_TIME(B.Duration2)) * 3600 +
           DATE_PART('MINUTE', TO_TIME(B.Duration2)) * 60 +
           DATE_PART('SECOND', TO_TIME(B.Duration2)) < 60 THEN 'Short (<1 min)'
      WHEN DATE_PART('HOUR', TO_TIME(B.Duration2)) * 3600 +
           DATE_PART('MINUTE', TO_TIME(B.Duration2)) * 60 +
           DATE_PART('SECOND', TO_TIME(B.Duration2)) < 300 THEN 'Medium (1–5 min)'
      ELSE 'Long (>5 min)'
    END, 'None'
  ) AS duration_category

FROM BRIGHTTV.GREAT_CHANNELS.USERDATSET AS A
INNER JOIN BRIGHTTV.GREAT_CHANNELS.VIEWDAT AS B
  ON A.USERID = B.USERID;
