-- models/marts/mart_trial_goals.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- MART: Trial Goals
-- Grain: one row per organization_id
-- Tracks whether each trialist has completed each of the four defined
-- trial goals, along with the timestamp of first completion per goal.
--
-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  GOAL DEFINITIONS                                                       │
-- ├─────────────────────────────────────────────────────────────────────────┤
-- │                                                                         │
-- │  G1 – Early Schedule Setup                                              │
-- │       ≥2 shifts created within the first 3 days of trial.              │
-- │       Rationale: Orgs creating 2+ shifts in 3 days invest early and    │
-- │       deliberately. 61.4% of orgs reach this, CR = 23.1% (1.08x).     │
-- │       The 3-day window captures committed early adopters.               │
-- │                                                                         │
-- │  G2 – Live Operations Proof                                             │
-- │       Mobile.Schedule.Loaded AND (PunchClock.PunchedIn OR              │
-- │       Scheduling.Shift.AssignmentChanged) — any time in trial.         │
-- │       Rationale: Mobile + operational event = a live team using the    │
-- │       product, not just a solo admin setup. 27.6% reach this,          │
-- │       CR = 23.2% (1.09x). Hardest signal to fake — requires staff      │
-- │       onboarding.                                                       │
-- │                                                                         │
-- │  G3 – Admin Approval Workflow                                           │
-- │       ≥2 shifts approved (Scheduling.Shift.Approved).                  │
-- │       Rationale: 2+ approvals confirms a repeatable approval cycle,    │
-- │       not a one-off test. Gateway to payroll. 13.9% reach this,        │
-- │       CR = 23.9% (1.12x — highest single-activity lift).               │
-- │                                                                         │
-- │  G4 – Sustained Return Engagement                                       │
-- │       Active in at least 3 distinct trial weeks.                       │
-- │       Rationale: Returning for 3 weeks requires genuine product        │
-- │       adoption. Strongest signal in the data: 17.2% of orgs,          │
-- │       CR = 24.7% (1.16x — highest in the dataset).                     │
-- │                                                                         │
-- │  NOTE: All goals are HYPOTHESES, not proven conversion levers.         │
-- │  The model AUC (≈0.50–0.54) confirms that in-trial behaviour cannot   │
-- │  deterministically predict conversion, as 51.9% of conversions occur  │
-- │  after day 30, driven by post-trial sales/procurement processes.       │
-- │  Goals should be validated via A/B testing once instrumented.          │
-- └─────────────────────────────────────────────────────────────────────────┘
-- ─────────────────────────────────────────────────────────────────────────────

WITH events AS (

    SELECT * FROM {{ ref('stg_trial_events') }}

),

orgs AS (

    SELECT * FROM {{ ref('stg_trial_organisations') }}

),

-- ── Goal 1: ≥2 shifts created within first 3 days ────────────────────────────
g1_data AS (

    SELECT
        organization_id,
        COUNT(*)                        AS g1_shifts_in_3_days,
        MIN(event_at)                   AS g1_first_shift_at
    FROM events
    WHERE activity_name   = 'Scheduling.Shift.Created'
      AND days_into_trial <= 3
    GROUP BY organization_id

),

g1 AS (

    SELECT
        organization_id,
        g1_shifts_in_3_days >= 2        AS goal_1_met,
        g1_first_shift_at,
        g1_shifts_in_3_days
    FROM g1_data

),

-- ── Goal 2: Mobile view + operational event (any point in trial) ─────────────
g2_mobile AS (

    SELECT DISTINCT
        organization_id,
        MIN(event_at) AS g2_mobile_at
    FROM events
    WHERE activity_name = 'Mobile.Schedule.Loaded'
    GROUP BY organization_id

),

g2_ops AS (

    SELECT DISTINCT
        organization_id,
        MIN(event_at) AS g2_ops_at
    FROM events
    WHERE activity_name IN (
        'PunchClock.PunchedIn',
        'Scheduling.Shift.AssignmentChanged'
    )
    GROUP BY organization_id

),

g2 AS (

    SELECT
        COALESCE(m.organization_id, o.organization_id)  AS organization_id,
        (m.organization_id IS NOT NULL
         AND o.organization_id IS NOT NULL)              AS goal_2_met,
        m.g2_mobile_at,
        o.g2_ops_at,
        GREATEST(m.g2_mobile_at, o.g2_ops_at)           AS goal_2_completed_at
    FROM g2_mobile m
    FULL OUTER JOIN g2_ops o USING (organization_id)

),

-- ── Goal 3: ≥2 shifts approved ───────────────────────────────────────────────
g3_data AS (

    SELECT
        organization_id,
        COUNT(*)        AS g3_approvals_count,
        MIN(event_at)   AS g3_first_approval_at
    FROM events
    WHERE activity_name = 'Scheduling.Shift.Approved'
    GROUP BY organization_id

),

g3 AS (

    SELECT
        organization_id,
        g3_approvals_count >= 2         AS goal_3_met,
        g3_first_approval_at,
        g3_approvals_count,

        -- Timestamp when 2nd approval occurred (goal completion point)
        CASE
            WHEN g3_approvals_count >= 2 THEN g3_first_approval_at
        END AS goal_3_completed_at

    FROM g3_data

),

-- ── Goal 4: Active in ≥3 distinct trial weeks ─────────────────────────────────
g4_data AS (

    SELECT
        organization_id,
        COUNT(DISTINCT trial_week)      AS g4_weeks_active
    FROM events
    GROUP BY organization_id

),

g4 AS (

    SELECT
        organization_id,
        g4_weeks_active >= 3            AS goal_4_met,
        g4_weeks_active
    FROM g4_data

),

-- ── Combine all goals ─────────────────────────────────────────────────────────
combined AS (

    SELECT
        o.organization_id,
        o.converted,
        o.trial_start_at,
        o.trial_end_at,
        o.trial_cohort_month,
        o.conversion_timing_bucket,

        -- ── Goal 1 ──
        COALESCE(g1.goal_1_met, FALSE)              AS goal_1_early_schedule,
        g1.g1_first_shift_at                        AS goal_1_completed_at,
        COALESCE(g1.g1_shifts_in_3_days, 0)         AS g1_shifts_in_first_3_days,

        -- ── Goal 2 ──
        COALESCE(g2.goal_2_met, FALSE)              AS goal_2_live_operations,
        g2.goal_2_completed_at,

        -- ── Goal 3 ──
        COALESCE(g3.goal_3_met, FALSE)              AS goal_3_approval_workflow,
        g3.goal_3_completed_at,
        COALESCE(g3.g3_approvals_count, 0)          AS g3_shift_approvals,

        -- ── Goal 4 ──
        COALESCE(g4.goal_4_met, FALSE)              AS goal_4_sustained_return,
        COALESCE(g4.g4_weeks_active, 0)             AS g4_weeks_active,

        -- ── Summary ──
        (
            COALESCE(g1.goal_1_met, FALSE)::INTEGER +
            COALESCE(g2.goal_2_met, FALSE)::INTEGER +
            COALESCE(g3.goal_3_met, FALSE)::INTEGER +
            COALESCE(g4.goal_4_met, FALSE)::INTEGER
        ) AS goals_completed_count

    FROM orgs o
    LEFT JOIN g1 USING (organization_id)
    LEFT JOIN g2 USING (organization_id)
    LEFT JOIN g3 USING (organization_id)
    LEFT JOIN g4 USING (organization_id)

)

SELECT
    *,
    goals_completed_count = 4   AS all_goals_completed
FROM combined
