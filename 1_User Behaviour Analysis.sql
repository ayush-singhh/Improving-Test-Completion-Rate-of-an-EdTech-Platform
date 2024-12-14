





-- Data cleaning (adding only data where the user visited in this month for the first time)

    -- Assigning event order for each anonymous_id
    WITH event_order_cte AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY anonymous_id ORDER BY original_timestamp) AS event_order
        FROM kys_track_events_data
    ),
    -- Identifying users with qualifying first events
    first_event AS (
        SELECT DISTINCT
            anonymous_id
        FROM event_order_cte
        WHERE event_order = 1 
        AND event IN ('viewed_kys_landing_page', 'attempted_starting_kys_test')
    ),
    -- Excluding user where they finished the test but didn't start it in this month
    removing_last_months_users AS (
        SELECT 
            anonymous_id
        FROM event_order_cte
        WHERE event IN ('attempted_starting_kys_test', 'attempted_resuming_kys_test', 'attempted_finishing_kys_test')
        GROUP BY anonymous_id
        HAVING   
            MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN 1 ELSE 0 END) = 0
            AND MAX(CASE WHEN event = 'attempted_finishing_kys_test' THEN 1 ELSE 0 END) > 0
    )
    SELECT 
        cte.*
    INTO #clean_kys_data
    FROM event_order_cte cte
    -- Include only users from qualifying sessions
    INNER JOIN first_event fe
        ON cte.anonymous_id = fe.anonymous_id
    -- Exclude users flagged by the exclusion logic
    LEFT JOIN removing_last_months_users lmu
        ON cte.anonymous_id = lmu.anonymous_id
    WHERE lmu.anonymous_id IS NULL -- Exclude users meeting exclusion criteria
    ORDER BY cte.anonymous_id, cte.original_timestamp;



-- Releven Columns for Analysis

    SELECT 
        anonymous_id,
        original_timestamp, 
        event,
        candidate_reason_to_join,     
        is_retrying, 
        question_sequence, 
        time_taken, 
        question_type,
        action,
        context_screen_width,
        context_screen_height, 
        event_order
    FROM #clean_kys_data
    Order BY anonymous_id, original_timestamp






-- User Flow / Journey

    SELECT
        anonymous_id,
        STRING_AGG(event, N' → ') AS user_journey
    FROM
        kys_track_events_data
    GROUP BY
        anonymous_id
    ORDER BY 
        COUNT(anonymous_id) DESC




-- Funnel Analysis

    WITH funnel AS (
        SELECT 
            anonymous_id,
            MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN 1 ELSE 0 END) AS started,
            MAX(CASE WHEN event = 'attempted_question' THEN 1 ELSE 0 END) AS attempted_questions,
            MAX(CASE WHEN event = 'attempted_finishing_kys_test' THEN 1 ELSE 0 END) AS finished,
            MAX(CASE WHEN event = 'viewed_kys_report_overview_slide' THEN 1 ELSE 0 END) AS viewed_overview,
            MAX(CASE WHEN event = 'viewed_kys_report' THEN 1 ELSE 0 END) AS viewed_full_report
        FROM #clean_kys_data
        WHERE event IN (
            'attempted_starting_kys_test', 
            'attempted_question', 
            'attempted_finishing_kys_test', 
            'viewed_kys_report_overview_slide', 
            'viewed_kys_report'
        )
        GROUP BY anonymous_id
        -- ORDER BY [started] DESC
    )
    SELECT 
        COUNT(*) AS total_users,
        SUM(started) AS started_count,
        SUM(attempted_questions) AS attempted_count,
        SUM(finished) AS finished_count,
        SUM(viewed_overview) AS overview_count,
        SUM(viewed_full_report) AS full_report_count
    FROM funnel;




-- Number of Users who Resumed and Either Finished or Didn't Finished the test

    WITH user_events AS (
    SELECT 
        anonymous_id,
        MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN 1 ELSE 0 END) AS started,
        MAX(CASE WHEN event = 'attempted_resuming_kys_test' THEN 1 ELSE 0 END) AS resumed,
        MAX(CASE WHEN event = 'attempted_finishing_kys_test' THEN 1 ELSE 0 END) AS finished
    FROM #clean_kys_data
    WHERE event IN (
        'attempted_starting_kys_test', 
        'attempted_resuming_kys_test', 
        'attempted_finishing_kys_test'
    )
    GROUP BY anonymous_id
    )
    SELECT *
        COUNT(*) AS total_users,
        SUM(started) AS started_count,
        SUM(resumed) AS resumed_count,
        SUM(finished) AS finished_count
    FROM user_events
    WHERE resumed > 0 AND finished > 0 -- Who Resumed and Finished
    -- WHERE resumed > 0 AND finished = 0 -- Who Resumed But not Finished



-- Time Users took to take the test after visiting 

    SELECT
        anonymous_id, 
        COUNT(event) event_count, 
        COUNT(CASE WHEN event = 'viewed_kys_landing_page' THEN original_timestamp END) AS visit_time,
        MIN(CASE WHEN event = 'attempted_starting_kys_test' THEN original_timestamp END) AS start_time, 
        DATEDIFF(SECOND,MIN(CASE WHEN event = 'viewed_kys_landing_page' THEN original_timestamp END),
        MIN(CASE WHEN event = 'attempted_starting_kys_test' THEN original_timestamp END) ) AS visit_to_test_time
    FROM #clean_kys_data
    GROUP BY anonymous_id
    ORDER BY visit_to_test_time DESC



-- Distribution of Sessions Duration where users dropped off from the test

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


-- Distribution of Number of Questions Answered by non-Finishers and Finishers

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



-- Count of Users who only Visited the site and how many times they visited


    WITH landing_page_visits AS (
        SELECT 
            anonymous_id,
            COUNT(*) AS visit_count
        FROM #clean_kys_data
        WHERE event = 'viewed_kys_landing_page'
        GROUP BY anonymous_id
    ),
    test_starts AS (
        SELECT DISTINCT anonymous_id
        FROM #clean_kys_data
        WHERE event = 'attempted_starting_kys_test'
    ),
    report_views AS (
        SELECT DISTINCT anonymous_id
        FROM #clean_kys_data
        WHERE event = 'viewed_kys_report'
    ), cte AS (
    SELECT 
        lpv.anonymous_id as anonymous_id ,
        lpv.visit_count as visit_count
    FROM landing_page_visits lpv
    LEFT JOIN test_starts ts 
        ON lpv.anonymous_id = ts.anonymous_id 
    LEFT JOIN report_views rv
        ON lpv.anonymous_id = rv.anonymous_id
    WHERE ts.anonymous_id IS NULL -- Exclude users who started the test
    AND rv.anonymous_id IS NULL -- Exclude users who viewed the report
    --ORDER BY lpv.visit_count DESC 
    )
    SELECT visit_count, COUNT(visit_count) as visit_count_count
    FROM cte
    GROUP BY visit_count;






-- Funnel Analysis With Segmentation

    WITH Segmentation_Device AS (
            SELECT 
                user_id, 
                CASE 
                    WHEN context_screen_width <= 768 THEN 'smart_phone'
                    WHEN context_screen_width > 768 AND context_screen_width < 1025 THEN 'tablet/smallpc'
                    WHEN context_screen_width >= 1025 THEN 'pc' 
                ELSE NULL
                END AS device_type
            FROM #clean_kys_data
    ), Segmentation_Filter AS (
            SELECT * 
            FROM Segmentation_Device
            WHERE device_type = 'pc'
    ), Funnel AS (
            SELECT 
                user_id,
                MAX(CASE WHEN event = 'attempted_starting_kys_test' THEN 1 ELSE 0 END) AS started,
                MAX(CASE WHEN event = 'attempted_question' THEN 1 ELSE 0 END) AS attempted_questions,
                MAX(CASE WHEN event = 'attempted_finishing_kys_test' THEN 1 ELSE 0 END) AS finished,
                MAX(CASE WHEN event = 'viewed_kys_report_overview_slide' THEN 1 ELSE 0 END) AS viewed_overview,
                MAX(CASE WHEN event = 'viewed_kys_report' THEN 1 ELSE 0 END) AS viewed_full_report
            FROM #clean_kys_data
            WHERE event IN (
                'attempted_starting_kys_test', 
                'attempted_question', 
                'attempted_finishing_kys_test', 
                'viewed_kys_report_overview_slide', 
                'viewed_kys_report'
            )
            GROUP BY user_id
            -- ORDER BY [started] DESC
    )-- Compilation of logic 
        SELECT 
            COUNT(F.user_id) AS total_users,
            SUM(started) AS started_count,
            SUM(attempted_questions) AS attempted_count,
            SUM(finished) AS finished_count,
            SUM(viewed_overview) AS overview_count,
            SUM(viewed_full_report) AS full_report_count
        FROM Funnel F
        LEFT JOIN Segmentation_Filter SF 
            ON F.user_id = SF.user_id
        WHERE SF.user_id IS NULL 




















-- Total from Funnel
-- total_users	started_count	attempted_count	finished_count	overview_count	full_report_count
-- 448	384	362	245	245	315

-- device_type	(No column name)
-- tablet/smallpc	5
-- smart_phone	227
-- pc	277




-- smart_phone
-- total_users	started_count	attempted_count	finished_count	overview_count	full_report_count
-- 252	211	201	166	166	211


-- pc
-- total_users	started_count	attempted_count	finished_count	overview_count	full_report_count
-- 193	171	159	77	77	103



-- Using Inner Join and Left Join, Group By Device Type

-- total_users	started_count	attempted_count	finished_count	overview_count	full_report_count
-- 5834	5719	5750	4573	4573	4779


-- Using Left Join with Segmentatio Filter

-- total_users	started_count	attempted_count	finished_count	overview_count	full_report_count
-- 252	211	201	166	166	211











-- Segmentation by Device type

    WITH Device_Segmentation AS (
    SELECT 
        user_id, 
        CASE 
            WHEN context_screen_width <= 768 THEN 'smart_phone'
            WHEN context_screen_width > 768 AND context_screen_width < 1025 THEN 'tablet/smallpc'
            WHEN context_screen_width >= 1025 THEN 'pc' 
        ELSE NULL
        END AS device_type
    FROM #clean_kys_data
    )
    SELECT device_type, COUNT(DISTINCT user_id)
    FROM Device_Segmentation
    --WHERE device_type = 'smart_phone'
    GROUP BY device_type







-- Sementation by Marketing Attribution
    -- context_page_initial_referring_domain



-- context_user_agent





-- Cohort


-- Action which adds users into the cohort 










