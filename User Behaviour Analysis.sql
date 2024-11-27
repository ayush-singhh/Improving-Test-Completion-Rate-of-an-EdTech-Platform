




-- Time it took to user to take the test after visiting the site

    SELECT
        anonymous_id, 
        MIN(CASE WHEN event = 'viewed_kys_landing_page' THEN original_timestamp END) AS visit_time,
        MIN(CASE WHEN event = 'attempted_starting_kys_test' THEN original_timestamp END) AS start_time, 
        DATEDIFF(SECOND,MIN(CASE WHEN event = 'viewed_kys_landing_page' THEN original_timestamp END),
        MIN(CASE WHEN event = 'attempted_starting_kys_test' THEN original_timestamp END) ) AS visit_to_test_duration
    FROM #clean_kys_data
    GROUP BY anonymous_id
    ORDER BY visit_to_test_duration DESC





-- Distribution of sessions duration where users dropped off from the test

    WITH test_status AS (
        SELECT 
            anonymous_id,
            MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN 1 ELSE 0 END) AS started_test,
            MAX(CASE WHEN event = 'attempted_resuming_kys_test' THEN 1 ELSE 0 END) AS resumed_test,
            MAX(CASE WHEN event = 'attempted_finishing_kys_test' THEN 1 ELSE 0 END) AS finished_test
        FROM #clean_kys_data
        WHERE event IN ('attempted_starting_kys_test', 'attempted_finishing_kys_test', 'attempted_resuming_kys_test')
        GROUP BY anonymous_id
    ), 
    all_data AS (
        SELECT cd.* 
        FROM test_status ts
        INNER JOIN #clean_kys_data cd ON ts.anonymous_id = cd.anonymous_id
        WHERE finished_test = 0 AND started_test > 0 AND resumed_test = 0 AND  cd.event IN ('attempted_starting_kys_test', 'attempted_question') 
        -- Removing finished test and resumes
    )--, test_duration_cte AS (
    SELECT 
        anonymous_id, 
        MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN original_timestamp END) AS start_time,
        MAX(CASE WHEN event = 'attempted_question' THEN original_timestamp END) AS dropoff_time, 
        DATEDIFF(SECOND, MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN original_timestamp END), 
        MAX(CASE WHEN event = 'attempted_question' THEN original_timestamp END) ) AS test_duration
    FROM all_data
    GROUP BY anonymous_id
    ORDER BY test_duration DESC
    -- ) 
    -- SELECT AVG(test_duration)
    -- FROM test_duration_cte



-- Distribution of numbeer of Question Answered by Test's non-Finishers and Finishers

    WITH cte AS (
        SELECT 
            anonymous_id, 
            COUNT(event) AS question_count
        FROM #clean_kys_data
        WHERE event = 'attempted_question'
        GROUP BY anonymous_id 
        -- question count per user
        ), finished_session AS (
        SELECT 
            anonymous_id,
            MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN 1 ELSE 0 END) AS started_test,
            MAX(CASE WHEN event = 'attempted_resuming_kys_test' THEN 1 ELSE 0 END) AS resumed_test,
            MAX(CASE WHEN event = 'attempted_finishing_kys_test' THEN 1 ELSE 0 END) AS finished_test
        FROM #clean_kys_data
        WHERE event IN ('attempted_starting_kys_test', 'attempted_finishing_kys_test', 'attempted_resuming_kys_test')
        GROUP BY anonymous_id
        )
        SELECT  
        -- cte.anonymous_id , 
            -- cte.question_count, 
            -- fs.finished_test 
            -- AVG(cte.question_count) AS finished_questions --, fs.finished_count
            SUM(CASE WHEN cte.question_count < 10 THEN 1 ELSE 0 END) AS less_than_10,
            SUM(CASE WHEN cte.question_count BETWEEN 10 AND 28 THEN 1 ELSE 0 END) AS between_10_28,
            SUM(CASE WHEN cte.question_count > 28 THEN 1 ELSE 0 END) AS more_than_28
        FROM cte
        INNER JOIN finished_session fs ON cte.anonymous_id = fs.anonymous_id
        WHERE 
            cte.question_count < 55 AND 
            fs.finished_test > 0
        --GROUP BY cte.anonymous_id
        --ORDER BY question_count DESC
