-- models/staging/stg_trial_events.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- Staging: clean, type-cast, deduplicate, and enrich raw trial events.
-- Source:  {{ source('raw', 'trial_events') }}
-- Grain:   one row per event (post-dedup)
-- ─────────────────────────────────────────────────────────────────────────────

WITH source AS (

    SELECT * FROM {{ source('raw', 'trial_events') }}

),

deduplicated AS (

    SELECT DISTINCT
        organization_id,
        activity_name,
        CAST(timestamp    AS TIMESTAMP) AS event_at,
        CAST(converted    AS BOOLEAN)   AS converted,
        CAST(converted_at AS TIMESTAMP) AS converted_at,
        CAST(trial_start  AS TIMESTAMP) AS trial_start_at,
        CAST(trial_end    AS TIMESTAMP) AS trial_end_at
    FROM source
    WHERE timestamp IS NOT NULL

),

enriched AS (

    SELECT
        organization_id,
        activity_name,
        event_at,
        converted,
        converted_at,
        trial_start_at,
        trial_end_at,

        -- Days / hours into trial (float, allows sub-day precision)
        DATE_DIFF('second', trial_start_at, event_at) / 86400.0
            AS days_into_trial,

        DATE_DIFF('second', trial_start_at, event_at) / 3600.0
            AS hours_into_trial,

        -- Trial week bucket (1-indexed, capped at 4)
        LEAST(
            FLOOR(DATE_DIFF('second', trial_start_at, event_at) / 86400.0 / 7) + 1,
            4
        )::INTEGER AS trial_week,

        -- Module classification (product area)
        CASE
            WHEN activity_name LIKE 'Scheduling.%'                         THEN 'Scheduling'
            WHEN activity_name IN ('Mobile.Schedule.Loaded',
                                   'Shift.View.Opened',
                                   'ShiftDetails.View.Opened')             THEN 'Mobile'
            WHEN activity_name LIKE 'PunchClock.%'
              OR activity_name LIKE 'Break.%'                              THEN 'PunchClock'
            WHEN activity_name LIKE 'Absence.%'                            THEN 'Absence'
            WHEN activity_name IN (
                'Timesheets.BulkApprove.Confirmed',
                'Integration.Xero.PayrollExport.Synced',
                'Revenue.Budgets.Created')                                 THEN 'Payroll'
            WHEN activity_name = 'Communication.Message.Created'           THEN 'Communication'
            ELSE 'Other'
        END AS module,

        -- Whether this event falls within the valid 0–30-day trial window
        (
            DATE_DIFF('second', trial_start_at, event_at) >= 0
            AND DATE_DIFF('second', trial_start_at, event_at) / 86400.0 <= 30
        ) AS is_within_trial_window,

        -- Days from trial start to conversion (NULL for non-converters)
        DATE_DIFF('second', trial_start_at, converted_at) / 86400.0
            AS days_to_convert

    FROM deduplicated

)

SELECT *
FROM   enriched
WHERE  is_within_trial_window   -- only retain events inside the trial window
