-- models/marts/mart_trial_activation.sql
-- ─────────────────────────────────────────────────────────────────────────────
-- MART: Trial Activation
-- Grain: one row per organization_id
-- Primary output for dashboards, intervention triggers, and funnel reporting.
--
-- "Trial Activation" = completing ALL four trial goals:
--   G1 – Early Schedule Setup    (≥2 shifts in first 3 days)
--   G2 – Live Operations Proof   (mobile view + operational event)
--   G3 – Admin Approval Workflow (≥2 shift approvals)
--   G4 – Sustained Return        (active in ≥3 trial weeks)
--
-- KEY CONTEXT FOR CONSUMERS OF THIS MODEL:
--   In-trial behaviour has limited direct predictive power for conversion
--   (LR/RF AUC ≈ 0.50–0.54). This is because 51.9% of conversions occur
--   AFTER the 30-day trial ends, driven by post-trial sales and procurement
--   processes outside the behavioural log. These goals are designed as
--   engagement quality signals and should be treated as leading indicators
--   of conversion intent, not guaranteed conversion predictors.
--   Validate via A/B testing before using for automated decision-making.
-- ─────────────────────────────────────────────────────────────────────────────

WITH goals AS (

    SELECT * FROM {{ ref('mart_trial_goals') }}

),

orgs AS (

    SELECT * FROM {{ ref('stg_trial_organisations') }}

),

activation AS (

    SELECT
        g.organization_id,
        g.converted,
        g.trial_start_at,
        g.trial_end_at,
        g.trial_cohort_month,
        g.conversion_timing_bucket,

        -- ── Activation status ──────────────────────────────────────────────
        g.all_goals_completed                                   AS is_activated,

        -- Timestamp at which activation was achieved
        -- (the moment the last of the 4 goals was completed)
        CASE
            WHEN g.all_goals_completed THEN
                GREATEST(
                    g.goal_1_completed_at,
                    g.goal_2_completed_at,
                    g.goal_3_completed_at,
                    -- G4 completion = when the org's 3rd distinct week of activity started
                    -- We approximate as first event in week 3 (min trial_start + 14 days)
                    g.trial_start_at + INTERVAL '14 days'
                )
        END                                                     AS activated_at,

        -- Days from trial start to activation
        CASE
            WHEN g.all_goals_completed THEN
                GREATEST(
                    DATE_DIFF('second', g.trial_start_at, g.goal_1_completed_at),
                    DATE_DIFF('second', g.trial_start_at, g.goal_2_completed_at),
                    DATE_DIFF('second', g.trial_start_at, g.goal_3_completed_at),
                    14 * 86400   -- G4 minimum: 14 days to reach week 3
                ) / 86400.0
        END                                                     AS days_to_activation,

        -- ── Individual goal flags ─────────────────────────────────────────
        g.goal_1_early_schedule,
        g.goal_1_completed_at,
        g.goal_2_live_operations,
        g.goal_2_completed_at,
        g.goal_3_approval_workflow,
        g.goal_3_completed_at,
        g.goal_4_sustained_return,
        g.goals_completed_count,

        -- ── Supporting counts ─────────────────────────────────────────────
        g.g1_shifts_in_first_3_days,
        g.g3_shift_approvals,
        g.g4_weeks_active,

        -- ── Org engagement context ────────────────────────────────────────
        o.total_events,
        o.distinct_activities,
        o.distinct_modules_used,
        o.active_days,
        o.distinct_weeks_active,
        o.hours_to_first_event,
        o.days_to_convert,
        o.week_1_events,
        o.active_in_week_1,
        o.active_in_week_2,
        o.active_in_week_3,
        o.active_in_week_4,

        -- ── Activation funnel stage ────────────────────────────────────────
        -- Ordered funnel: each org placed at their deepest completed stage.
        -- G1 → G2 → G3 → G4 reflects the product value chain order:
        -- Build schedule → Operate live → Approve for payroll → Return to use
        CASE
            WHEN g.all_goals_completed       THEN '5_fully_activated'
            WHEN g.goal_4_sustained_return   THEN '4_sustained_return'
            WHEN g.goal_3_approval_workflow  THEN '3_approval_workflow'
            WHEN g.goal_2_live_operations    THEN '2_live_operations'
            WHEN g.goal_1_early_schedule     THEN '1_early_schedule'
            ELSE                                  '0_no_goal_met'
        END                                                     AS activation_stage,

        -- ── Intervention signals ──────────────────────────────────────────
        -- Nudge: completed G1 but never proved live operations (G2)
        -- → Send mobile app adoption prompt / schedule a team walkthrough
        (
            g.goal_1_early_schedule
            AND NOT g.goal_2_live_operations
            AND NOT g.converted
        )                                                       AS nudge_mobile_adoption,

        -- Nudge: doing live operations but hasn't run approval cycle (G3)
        -- → Prompt admin to approve pending shifts for payroll
        (
            g.goal_2_live_operations
            AND NOT g.goal_3_approval_workflow
            AND NOT g.converted
        )                                                       AS nudge_approval_workflow,

        -- Nudge: completed G1–G3 but hasn't returned for 3 weeks (G4 gap)
        -- → Re-engagement campaign at day 12–14 if no week 3 activity
        (
            g.goal_1_early_schedule
            AND g.goal_2_live_operations
            AND g.goal_3_approval_workflow
            AND NOT g.goal_4_sustained_return
            AND NOT g.converted
        )                                                       AS nudge_reengagement,

        -- High-intent signal: activated but not yet converted
        -- → Priority outreach for sales / CS team
        (
            g.all_goals_completed
            AND NOT g.converted
        )                                                       AS priority_sales_outreach

    FROM goals g
    JOIN orgs  o USING (organization_id)

)

SELECT * FROM activation
