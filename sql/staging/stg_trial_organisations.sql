WITH events AS (

    SELECT * FROM {{ ref('stg_trial_events') }}

),

org_spine AS (

    -- Deduplicate org-level metadata (constant per org in source)
    SELECT DISTINCT
        organization_id,
        converted,
        converted_at,
        trial_start_at,
        trial_end_at,
        days_to_convert
    FROM events

),

org_stats AS (

    SELECT
        organization_id,
        COUNT(*)                            AS total_events,
        COUNT(DISTINCT activity_name)       AS distinct_activities,
        COUNT(DISTINCT module)              AS distinct_modules_used,
        COUNT(DISTINCT DATE(event_at))      AS active_days,
        MIN(event_at)                       AS first_event_at,
        MAX(event_at)                       AS last_event_at,
        COUNT(DISTINCT trial_week)          AS distinct_weeks_active,

        -- Per-week flags
        MAX(CASE WHEN trial_week = 1 THEN 1 ELSE 0 END) AS active_in_week_1,
        MAX(CASE WHEN trial_week = 2 THEN 1 ELSE 0 END) AS active_in_week_2,
        MAX(CASE WHEN trial_week = 3 THEN 1 ELSE 0 END) AS active_in_week_3,
        MAX(CASE WHEN trial_week = 4 THEN 1 ELSE 0 END) AS active_in_week_4,

        -- Week 1 event count (used in G1 goal computation)
        SUM(CASE WHEN trial_week = 1 THEN 1 ELSE 0 END) AS week_1_events

    FROM events
    GROUP BY organization_id

)

SELECT
    s.organization_id,
    s.converted,
    s.converted_at,
    s.trial_start_at,
    s.trial_end_at,
    s.days_to_convert,

    -- Engagement stats
    o.total_events,
    o.distinct_activities,
    o.distinct_modules_used,
    o.active_days,
    o.first_event_at,
    o.last_event_at,
    o.distinct_weeks_active,
    o.active_in_week_1,
    o.active_in_week_2,
    o.active_in_week_3,
    o.active_in_week_4,
    o.week_1_events,

    -- Derived metadata
    DATE_DIFF('hour', s.trial_start_at, o.first_event_at)
        AS hours_to_first_event,

    DATE_TRUNC('month', s.trial_start_at)
        AS trial_cohort_month,

    -- Conversion timing bucket
    CASE
        WHEN s.days_to_convert IS NULL             THEN 'Not converted'
        WHEN s.days_to_convert <= 30               THEN 'Within trial'
        WHEN s.days_to_convert <= 45               THEN 'Post-trial (31–45d)'
        ELSE                                            'Post-trial (45d+)'
    END AS conversion_timing_bucket

FROM org_spine   s
JOIN org_stats   o USING (organization_id)
