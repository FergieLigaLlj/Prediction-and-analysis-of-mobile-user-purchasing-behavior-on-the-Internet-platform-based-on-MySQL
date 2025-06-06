CREATE DATABASE IF NOT EXISTS ali_recsys CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ali_recsys;

-- Table to store user behavior data
CREATE TABLE IF NOT EXISTS train_user_behaviors (
    user_id INT UNSIGNED NOT NULL,
    item_id INT UNSIGNED NOT NULL,
    behavior_type TINYINT UNSIGNED NOT NULL COMMENT '1:Browse, 2:Collect, 3:Add to cart, 4:Purchase',
    user_geohash VARCHAR(16) DEFAULT NULL,
    item_category INT UNSIGNED NOT NULL,
    time_str VARCHAR(19) NOT NULL COMMENT 'Original time string e.g., YYYY-MM-DD HH',
    event_date DATE DEFAULT NULL,
    event_hour TINYINT UNSIGNED DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Table to store item information
CREATE TABLE IF NOT EXISTS train_item_info (
    item_id INT UNSIGNED NOT NULL,
    item_geohash VARCHAR(16) DEFAULT NULL,
    item_category INT UNSIGNED NOT NULL,
    PRIMARY KEY (item_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SHOW GLOBAL VARIABLES LIKE 'local_infile';

-- Load data into train_user_behaviors table
-- Ensure the CSV file path is correct and MySQL server has permissions to read it.
-- If using LOAD DATA LOCAL INFILE, ensure 'local_infile' is enabled on both client and server.
LOAD DATA LOCAL INFILE 'C:/Users/yishu/Desktop/M Recommend/tianchi_mobile_recommend_train_user.csv'
INTO TABLE train_user_behaviors
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' -- or '\r\n' if using Windows line endings
IGNORE 1 ROWS -- Skip header row
(user_id, item_id, behavior_type, user_geohash, item_category, time_str);



SHOW GLOBAL VARIABLES LIKE 'net_read_timeout';
SHOW GLOBAL VARIABLES LIKE 'net_write_timeout';
SHOW GLOBAL VARIABLES LIKE 'wait_timeout';
SHOW SESSION VARIABLES LIKE 'net_read_timeout'; -- For session


SET SESSION net_read_timeout = 3000; -- 5 minutes, adjust as needed
SET SESSION net_write_timeout = 3000;

-- Populate event_date and event_hour from time_str
-- This step assumes time_str is in 'YYYY-MM-DD HH' format.
-- Adjust STR_TO_DATE format string if necessary.
UPDATE train_user_behaviors
SET event_date = STR_TO_DATE(SUBSTRING_INDEX(time_str, ' ', 1), '%Y-%m-%d'),
    event_hour = CAST(SUBSTRING_INDEX(time_str, ' ', -1) AS UNSIGNED);

-- Load data into train_item_info table
LOAD DATA LOCAL INFILE 'C:/Users/yishu/Desktop/M Recommend/tianchi_mobile_recommend_train_item.csv'
INTO TABLE train_item_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\n' -- or '\r\n'
IGNORE 1 ROWS -- Skip header row
(item_id, item_geohash, item_category);
select count(*) from train_item_info;
select count(*) from train_user_behaviors;
SHOW WARNINGS;


-- Indexes for train_user_behaviors
ALTER TABLE train_user_behaviors ADD INDEX idx_user_date (user_id, event_date);
ALTER TABLE train_user_behaviors ADD INDEX idx_item_date (item_id, event_date);
ALTER TABLE train_user_behaviors ADD INDEX idx_date_behavior (event_date, behavior_type);
ALTER TABLE train_user_behaviors ADD INDEX idx_user_item_date (user_id, item_id, event_date);
ALTER TABLE train_user_behaviors ADD INDEX idx_category_date (item_category, event_date);

-- Indexes for train_item_info (Primary Key on item_id already exists)
ALTER TABLE train_item_info ADD INDEX idx_item_cat (item_category);


CREATE TABLE IF NOT EXISTS filtered_user_behaviors AS
SELECT *
FROM train_user_behaviors
WHERE
    -- Keep all purchases within the observation window
    (behavior_type = 4 AND event_date BETWEEN '2014-11-18' AND '2014-12-18')
    OR
    -- Keep browse actions only from Dec 18th
    (behavior_type = 1 AND event_date = '2014-12-18')
    OR
    -- Keep collect and cart actions from Dec 12th to Dec 18th (within 7 days of Dec 19th)
    (behavior_type IN (2, 3) AND event_date BETWEEN '2014-12-12' AND '2014-12-18');

-- Add necessary indexes to the filtered table
ALTER TABLE filtered_user_behaviors ADD INDEX idx_fub_user_date (user_id, event_date);
ALTER TABLE filtered_user_behaviors ADD INDEX idx_fub_item_date (item_id, event_date);
ALTER TABLE filtered_user_behaviors ADD INDEX idx_fub_date_behavior (event_date, behavior_type);
ALTER TABLE filtered_user_behaviors ADD INDEX idx_fub_user_item_date (user_id, item_id, event_date);

CREATE TABLE IF NOT EXISTS candidate_user_item_pairs AS
SELECT DISTINCT user_id, item_id
FROM train_user_behaviors -- Using all interactions to define candidates
WHERE event_date BETWEEN '2014-11-18' AND '2014-12-18';

ALTER TABLE candidate_user_item_pairs ADD PRIMARY KEY (user_id, item_id);
USE ali_recsys;
drop table user_features;
CREATE TABLE IF NOT EXISTS user_features AS
SELECT
    user_id,
    COUNT(*) AS u_total_actions,
    COUNT(DISTINCT item_id) AS u_distinct_items_interacted,
    SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) AS u_purchase_count,
    SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) / COUNT(*) AS u_purchase_ratio,
    COUNT(DISTINCT event_date) AS u_days_active,
    DATEDIFF('2014-12-18', MAX(event_date)) AS u_last_action_recency,
    DATEDIFF('2014-12-18', MAX(CASE WHEN behavior_type = 4 THEN event_date ELSE NULL END)) AS u_last_purchase_recency,
    COUNT(DISTINCT item_category) AS u_distinct_categories_interacted
FROM filtered_user_behaviors -- or filtered_user_behaviors for certain features
WHERE event_date BETWEEN '2014-11-18' AND '2014-12-18'
GROUP BY user_id;
SHOW WARNINGS;
ALTER TABLE user_features ADD PRIMARY KEY (user_id);

CREATE TABLE IF NOT EXISTS item_features AS
SELECT
    item_id,
    COUNT(*) AS i_total_interactions,
    COUNT(DISTINCT user_id) AS i_distinct_users_interacted,
    SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) AS i_purchase_count,
    SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) / COUNT(*) AS i_purchase_ratio,
    DATEDIFF('2014-12-18', MAX(event_date)) AS i_last_interaction_recency,
    DATEDIFF('2014-12-18', MAX(CASE WHEN behavior_type = 4 THEN event_date ELSE NULL END)) AS i_last_purchase_recency
FROM filtered_user_behaviors -- or filtered_user_behaviors
WHERE event_date BETWEEN '2014-11-18' AND '2014-12-18'
GROUP BY item_id;

ALTER TABLE item_features ADD PRIMARY KEY (item_id);

CREATE TABLE IF NOT EXISTS user_item_interaction_features AS
SELECT
    user_id,
    item_id,
    SUM(CASE WHEN behavior_type = 1 THEN 1 ELSE 0 END) AS ui_browse_count,
    SUM(CASE WHEN behavior_type = 2 THEN 1 ELSE 0 END) AS ui_collect_count,
    SUM(CASE WHEN behavior_type = 3 THEN 1 ELSE 0 END) AS ui_cart_count,
    SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) AS ui_purchase_count_hist,
    DATEDIFF('2014-12-18', MAX(CASE WHEN behavior_type = 1 THEN event_date ELSE NULL END)) AS ui_last_browse_recency,
    DATEDIFF('2014-12-18', MAX(CASE WHEN behavior_type = 2 THEN event_date ELSE NULL END)) AS ui_last_collect_recency,
    DATEDIFF('2014-12-18', MAX(CASE WHEN behavior_type = 3 THEN event_date ELSE NULL END)) AS ui_last_cart_recency,
    DATEDIFF('2014-12-18', MAX(event_date)) AS ui_last_any_interaction_recency
FROM filtered_user_behaviors -- or filtered_user_behaviors for recency on specific recent actions
WHERE event_date BETWEEN '2014-11-18' AND '2014-12-18'
GROUP BY user_id, item_id;

ALTER TABLE user_item_interaction_features ADD PRIMARY KEY (user_id, item_id);






CREATE TABLE IF NOT EXISTS user_item_features_dec18 AS
SELECT
    cp.user_id,
    cp.item_id,
    -- User features
    uf.u_total_actions, uf.u_distinct_items_interacted, uf.u_purchase_count AS u_total_purchase_count,
    uf.u_purchase_ratio, uf.u_days_active, uf.u_last_action_recency AS u_g_last_action_recency,
    uf.u_last_purchase_recency AS u_g_last_purchase_recency, uf.u_distinct_categories_interacted,
    -- Item features (joined with train_item_info for canonical category)
    itf.i_total_interactions, itf.i_distinct_users_interacted, itf.i_purchase_count AS i_total_purchase_count,
    itf.i_purchase_ratio, itf.i_last_interaction_recency AS i_g_last_interaction_recency,
    itf.i_last_purchase_recency AS i_g_last_purchase_recency, ti.item_category,
    -- User-item interaction features
    uif.ui_browse_count, uif.ui_collect_count, uif.ui_cart_count, uif.ui_purchase_count_hist,
    uif.ui_last_browse_recency, uif.ui_last_collect_recency, uif.ui_last_cart_recency,
    uif.ui_last_any_interaction_recency,
    -- Time-window features (examples)
    (SELECT COUNT(*) FROM filtered_user_behaviors tb
     WHERE tb.user_id = cp.user_id AND tb.item_id = cp.item_id AND tb.behavior_type = 3
     AND tb.event_date BETWEEN '2014-12-12' AND '2014-12-18') AS ui_cart_count_7d,
    (SELECT COUNT(*) FROM filtered_user_behaviors tb
     WHERE tb.user_id = cp.user_id AND tb.item_id = cp.item_id AND tb.behavior_type = 1
     AND tb.event_date = '2014-12-18') AS ui_browse_count_1d,
    -- User-Category interaction features (example: user's purchase count in this item's category)
    (SELECT COUNT(*) FROM filtered_user_behaviors ucb
     WHERE ucb.user_id = cp.user_id AND ucb.item_category = ti.item_category AND ucb.behavior_type = 4
     AND ucb.event_date BETWEEN '2014-11-18' AND '2014-12-18') AS user_cat_purchase_count
FROM
    candidate_user_item_pairs cp
LEFT JOIN
    user_features uf ON cp.user_id = uf.user_id
LEFT JOIN
    item_features itf ON cp.item_id = itf.item_id
LEFT JOIN
    train_item_info ti ON cp.item_id = ti.item_id -- For canonical item_category
LEFT JOIN
    user_item_interaction_features uif ON cp.user_id = uif.user_id AND cp.item_id = uif.item_id
limit 1000000;
select count(distinct t.user_id)
from user_item_features_dec18 t;
ALTER TABLE user_item_features_dec18 ADD PRIMARY KEY (user_id, item_id);
-- Add further indexes as needed for scoring performance
ALTER TABLE user_item_features_dec18 ADD INDEX idx_cat_ipur_cnt (item_category, i_total_purchase_count);



-- This query calculates a score and ranks pairs.
-- The actual weights (w1, w2, etc.) need to be determined heuristically or via offline tuning if a validation set is used.
SELECT
    user_id,
    item_id,
    (
        (COALESCE(ui_purchase_count_hist, 0) * 50.0) +
        (COALESCE(ui_cart_count_7d, 0) * 20.0) +
        (CASE WHEN COALESCE(ui_last_cart_recency, 999) < 3 THEN 15.0 ELSE 0 END) + -- Bonus for very recent cart
        (COALESCE(ui_collect_count, 0) * 10.0) +
        (COALESCE(i_total_purchase_count, 0) * 0.1) + -- Item popularity
        (COALESCE(u_total_purchase_count, 0) * 0.05) - -- User general purchase tendency
        (COALESCE(ui_last_any_interaction_recency, 999) * 0.5) -- Penalty for old interactions
        -- Add more weighted features as needed
    ) AS prediction_score
FROM
    user_item_features_dec18
ORDER BY
    prediction_score DESC;



CREATE TABLE IF NOT EXISTS final_predictions AS
SELECT user_id, item_id , prediction_score -- Score can be kept for analysis but not for submission
FROM (
    SELECT
        user_id,
        item_id,
        (
            (COALESCE(ui_purchase_count_hist, 0) * 50.0) +
            (COALESCE(ui_cart_count_7d, 0) * 20.0) +
            (CASE WHEN COALESCE(ui_last_cart_recency, 999) < 3 THEN 15.0 ELSE 0 END) +
            (COALESCE(ui_collect_count, 0) * 10.0) +
            (COALESCE(i_total_purchase_count, 0) * 0.1) +
            (COALESCE(u_total_purchase_count, 0) * 0.05) -
            (COALESCE(ui_last_any_interaction_recency, 999) * 0.5)
        ) AS prediction_score,
        ROW_NUMBER() OVER (ORDER BY
            (
                (COALESCE(ui_purchase_count_hist, 0) * 50.0) +
                (COALESCE(ui_cart_count_7d, 0) * 20.0) +
                (CASE WHEN COALESCE(ui_last_cart_recency, 999) < 3 THEN 15.0 ELSE 0 END) +
                (COALESCE(ui_collect_count, 0) * 10.0) +
                (COALESCE(i_total_purchase_count, 0) * 0.1) +
                (COALESCE(u_total_purchase_count, 0) * 0.05) -
                (COALESCE(ui_last_any_interaction_recency, 999) * 0.5)
            ) DESC
        ) as rn -- Rank based on score
    FROM
        user_item_features_dec18
    -- Optional: Add a WHERE clause here to filter out pairs with very low scores
    -- WHERE ( (COALESCE(ui_purchase_count_hist, 0) * 50.0) +... ) > some_threshold
) ranked_predictions
WHERE rn <= 100000; -- Example: Select Top 100,000 predictions
SHOW VARIABLES LIKE 'secure_file_priv';
SELECT DISTINCT user_id, item_id
FROM final_predictions
INTO OUTFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\predict.txt'
 -- Specify your desired server path and filename
FIELDS TERMINATED BY '	' -- Use comma as a delimiter (common for CSV-like TXT files)
LINES TERMINATED BY '\n'; -- Use newline character for line endings
-- Assume 'final_predictions (user_id, item_id)' and
-- 'ground_truth_dec19_purchases (user_id, item_id)' tables exist.

-- Number of true positives (intersection)
SET @true_positives = (
    SELECT COUNT(*)
    FROM final_predictions fp
    JOIN ground_truth_dec19_purchases gt
    ON fp.user_id = gt.user_id AND fp.item_id = gt.item_id
);
SELECT MIN(event_date), MAX(event_date) FROM train_user_behaviors;
-- Ensure the correct database is selected
USE ali_recsys;
drop table ground_truth_dec19_purchases;
-- Drop the table if it already exists to ensure a fresh creation (optional, use with caution)
-- DROP TABLE IF EXISTS ground_truth_dec19_purchases;

-- Create the ground_truth_dec19_purchases table
-- This table will store the actual user_id and item_id pairs for purchases made on 2014-12-19
CREATE TABLE IF NOT EXISTS ground_truth_dec19_purchases AS
SELECT DISTINCT -- Select distinct pairs to avoid duplicates if any in the source
    user_id,
    item_id
FROM
    train_user_behaviors -- Assuming this table contains all user behaviors including the target date
WHERE
    behavior_type = 4 -- Filter for purchase actions (behavior_type 4 means purchase)
    AND event_date = '2014-12-18'; -- Filter for the specific target prediction date

-- Add a composite primary key to ensure uniqueness and for efficient joins
-- This step is crucial if the table was created with "IF NOT EXISTS" and might already exist without a PK
-- Or if you want to explicitly define it after creation.
-- If the table is always dropped and recreated, this can be part of the CREATE TABLE statement.
ALTER TABLE ground_truth_dec19_purchases
ADD PRIMARY KEY (user_id, item_id);
-- Number of predicted positives
SET @predicted_positives = (SELECT COUNT(*) FROM final_predictions);

-- Number of actual positives (reference set size)
SET @actual_positives = (SELECT COUNT(*) FROM ground_truth_dec19_purchases);

-- Calculate Precision and Recall (handle division by zero)
SET @precision = IF(@predicted_positives > 0, @true_positives / @predicted_positives, 0);
SET @recall = IF(@actual_positives > 0, @true_positives / @actual_positives, 0);

-- Calculate F1-Score (handle division by zero)
SET @f1_score = IF((@precision + @recall) > 0, (2 * @precision * @recall) / (@precision + @recall), 0);

SELECT @true_positives, @predicted_positives, @actual_positives, @precision, @recall, @f1_score;
