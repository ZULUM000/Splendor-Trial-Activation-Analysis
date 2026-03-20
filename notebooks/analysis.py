import warnings
warnings.filterwarnings("ignore")

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from scipy import stats
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import cross_val_score
from sklearn.preprocessing import StandardScaler


# Basic plotting settings
plt.rcParams.update({
    "figure.dpi": 140,
    "axes.spines.top": False,
    "axes.spines.right": False,
    "axes.titlesize": 13,
    "axes.labelsize": 11,
    "font.family": "DejaVu Sans",
})

# Project colors
INDIGO = "#4F46E5"
GREEN = "#10B981"
RED = "#EF4444"
AMBER = "#F59E0B"
SLATE = "#64748B"

# File paths
RAW = "../data/Copy of DA task.csv"
OUT = "../outputs"

# Helper functions: Used these functions keep the script easier to read and to reduce repetition.
def print_section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


def print_subsection(title):
    print("\n" + title)
    print("-" * len(title))


def format_pct(value):
    return f"{value:.1%}"


def save_plot(path):
    plt.tight_layout()
    plt.savefig(path)
    plt.close()


def get_orgs_for_activity(data, activity_name):
    return set(data.loc[data["activity_name"] == activity_name, "organization_id"])


def get_orgs_active_in_week(data, week_number):
    start_day = (week_number - 1) * 7
    end_day = week_number * 7
    mask = (data["days_into_trial"] >= start_day) & (data["days_into_trial"] < end_day)
    return set(data.loc[mask, "organization_id"])


def conversion_rate_for_orgs(org_conversion_series, org_ids):
    mask = org_conversion_series.index.isin(org_ids)
    return org_conversion_series[mask].mean()


# TASK 1 is to Load data, clean it, and prepare fields used later
# This part checks data quality and creates new variables needed for the rest of the analysis.

print_section("TASK 1: DATA CLEANING, EXPLORATION, AND CONVERSION ANALYSIS")

df_raw = pd.read_csv(RAW)
df = df_raw.copy()
df.columns = df.columns.str.lower()

print_subsection("Initial data checks")
print(f"Raw dataset shape: {df_raw.shape[0]:,} rows and {df_raw.shape[1]} columns")
print(f"Exact duplicate rows: {df_raw.duplicated().sum():,} ({df_raw.duplicated().mean():.1%} of raw data)")

date_columns = ["timestamp", "converted_at", "trial_start", "trial_end"]
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors="coerce")

print("\nMissing values after datetime conversion:")
null_found = False
for col in df.columns:
    null_count = df[col].isnull().sum()
    if null_count > 0:
        null_found = True
        print(f"- {col}: {null_count:,} missing values")
if not null_found:
    print("- No missing values found")

rows_before_dedup = len(df)
df = df.drop_duplicates()
rows_removed = rows_before_dedup - len(df)

print("\nAfter removing duplicates:")
print(f"- Rows remaining: {len(df):,}")
print(f"- Rows removed: {rows_removed:,}")

df["days_into_trial"] = (df["timestamp"] - df["trial_start"]).dt.total_seconds() / 86400
df["trial_week"] = (df["days_into_trial"] // 7).clip(upper=3).astype(int) + 1

df["module"] = df["activity_name"].map({
    "Scheduling.Shift.Created": "Scheduling",
    "Scheduling.Shift.AssignmentChanged": "Scheduling",
    "Scheduling.Shift.Approved": "Scheduling",
    "Scheduling.Template.ApplyModal.Applied": "Scheduling",
    "Scheduling.Availability.Set": "Scheduling",
    "Scheduling.OpenShiftRequest.Created": "Scheduling",
    "Scheduling.OpenShiftRequest.Approved": "Scheduling",
    "Scheduling.ShiftSwap.Created": "Scheduling",
    "Scheduling.ShiftSwap.Accepted": "Scheduling",
    "Scheduling.ShiftHandover.Created": "Scheduling",
    "Scheduling.ShiftHandover.Accepted": "Scheduling",
    "Mobile.Schedule.Loaded": "Mobile",
    "Shift.View.Opened": "Mobile",
    "ShiftDetails.View.Opened": "Mobile",
    "PunchClock.PunchedIn": "PunchClock",
    "PunchClock.PunchedOut": "PunchClock",
    "PunchClock.Entry.Edited": "PunchClock",
    "PunchClockStartNote.Add.Completed": "PunchClock",
    "PunchClockEndNote.Add.Completed": "PunchClock",
    "Break.Activate.Started": "PunchClock",
    "Break.Activate.Finished": "PunchClock",
    "Absence.Request.Created": "Absence",
    "Absence.Request.Approved": "Absence",
    "Absence.Request.Rejected": "Absence",
    "Timesheets.BulkApprove.Confirmed": "Payroll",
    "Integration.Xero.PayrollExport.Synced": "Payroll",
    "Revenue.Budgets.Created": "Payroll",
    "Communication.Message.Created": "Communication",
}).fillna("Other")

rows_before_filter = len(df)
df = df[(df["days_into_trial"] >= 0) & (df["days_into_trial"] <= 30)]
rows_filtered_out = rows_before_filter - len(df)

print("\nTrial window filtering:")
print(f"- Events removed outside the 0 to 30 day trial window: {rows_filtered_out:,}")
print(f"- Final cleaned event rows: {len(df):,}")

bad_true = df[(df["converted"] == True) & (df["converted_at"].isnull())]
bad_false = df[(df["converted"] == False) & (df["converted_at"].notnull())]

print("\nConversion field consistency checks:")
print(f"- Organisations marked converted=True but missing converted_at: {bad_true['organization_id'].nunique()}")
print(f"- Organisations marked converted=False but with converted_at present: {bad_false['organization_id'].nunique()}")

org_meta = (
    df.groupby("organization_id")
    .agg(
        converted=("converted", "first"),
        trial_start=("trial_start", "first"),
        trial_end=("trial_end", "first"),
        converted_at=("converted_at", "first"),
        total_events=("activity_name", "count"),
        distinct_acts=("activity_name", "nunique"),
        distinct_modules=("module", "nunique"),
        active_days=("timestamp", lambda x: x.dt.date.nunique()),
    )
    .reset_index()
)

org_meta["days_to_convert"] = (
    (org_meta["converted_at"] - org_meta["trial_start"]).dt.total_seconds() / 86400
)
org_meta["cohort_month"] = org_meta["trial_start"].dt.to_period("M")

for week in [1, 2, 3, 4]:
    week_orgs = get_orgs_active_in_week(df, week)
    org_meta[f"active_w{week}"] = org_meta["organization_id"].isin(week_orgs)

org_meta["weeks_active"] = org_meta[["active_w1", "active_w2", "active_w3", "active_w4"]].sum(axis=1)

org_conv = org_meta.set_index("organization_id")["converted"]
BASELINE = org_conv.mean()

total_orgs = len(org_meta)
converted_orgs = org_meta["converted"].sum()

print("\nOrganisation-level summary:")
print(f"- Total organisations: {total_orgs:,}")
print(f"- Converted organisations: {converted_orgs:,}")
print(f"- Overall conversion rate: {BASELINE:.1%}")

# FIGURE 1: Overall activity volume Shows which product activities appear most often in the trial data.


fig, ax = plt.subplots(figsize=(11, 6))
act_counts = df["activity_name"].value_counts()

short_labels = (
    act_counts.index
    .str.replace("Scheduling.", "Sched.", regex=False)
    .str.replace("PunchClock.", "PC.", regex=False)
    .str.replace("Mobile.", "M.", regex=False)
    .str.replace("Communication.", "Comm.", regex=False)
    .str.replace("Integration.", "Int.", regex=False)
    .str.replace("Timesheets.", "TS.", regex=False)
    .str.replace("Revenue.", "Rev.", regex=False)
)

colors = [INDIGO] * 5 + [SLATE] * (len(act_counts) - 5)

ax.barh(short_labels[::-1], act_counts.values[::-1], color=colors[::-1], alpha=0.85)
ax.set_xlabel("Number of events after deduplication")
ax.set_title("Figure 1: Activity volume across all trials")
ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x, _: f"{x:,.0f}"))

save_plot(f"{OUT}/fig1_activity_volume.png")
print("\nSaved: fig1_activity_volume.png")

# FIGURE 2: Conversion rate by activity usage Compares organisations that used an activity against those that did not.

rows = []
for act in df["activity_name"].value_counts().index:
    used_orgs = get_orgs_for_activity(df, act)
    used_conv = org_conv[org_conv.index.isin(used_orgs)]
    not_used_conv = org_conv[~org_conv.index.isin(used_orgs)]

    if len(used_conv) < 10:
        continue

    contingency = pd.crosstab(org_conv.index.isin(used_orgs).astype(int), org_conv)
    chi2, p_value, _, _ = stats.chi2_contingency(contingency)

    rows.append({
        "act": act.replace("Scheduling.", "S.").replace("PunchClock.", "PC.").replace("Mobile.", "M."),
        "cr_used": used_conv.mean(),
        "cr_unused": not_used_conv.mean(),
        "lift": used_conv.mean() / not_used_conv.mean(),
        "p": p_value,
        "n": len(used_conv)
    })

lift_df = pd.DataFrame(rows).sort_values("lift", ascending=False)

fig, ax = plt.subplots(figsize=(11, 5))
x = np.arange(len(lift_df))
width = 0.35

ax.bar(x - width / 2, lift_df["cr_used"], width, label="Used activity", color=GREEN, alpha=0.85)
ax.bar(x + width / 2, lift_df["cr_unused"], width, label="Did not use activity", color=RED, alpha=0.6)
ax.axhline(BASELINE, color=SLATE, linestyle="--", linewidth=1.2, label=f"Baseline conversion rate ({BASELINE:.1%})")
ax.set_xticks(x)
ax.set_xticklabels(lift_df["act"], rotation=42, ha="right", fontsize=7.5)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.set_title("Figure 2: Conversion rate by activity usage")
ax.legend(fontsize=9)

save_plot(f"{OUT}/fig2_activity_conversion_lift.png")
print("Saved: fig2_activity_conversion_lift.png")

# FIGURE 3: Cohort conversion Looks at conversion rate by trial start month.

cohort = org_meta.groupby("cohort_month")["converted"].agg(["mean", "count"]).reset_index()

fig, ax = plt.subplots(figsize=(7, 4))
ax.bar([str(c) for c in cohort["cohort_month"]], cohort["mean"], color=INDIGO, alpha=0.85)
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.set_ylim(0, 0.35)
ax.axhline(BASELINE, color=SLATE, linestyle="--", linewidth=1, label="Overall average")

for i, (cr, n) in enumerate(zip(cohort["mean"], cohort["count"])):
    ax.text(i, cr + 0.006, f"n={n}\n{cr:.1%}", ha="center", fontsize=9)

ax.set_title("Figure 3: Conversion rate by trial start cohort")
ax.set_ylabel("Conversion rate")
ax.legend(fontsize=9)

save_plot(f"{OUT}/fig3_cohort_conversion.png")
print("Saved: fig3_cohort_conversion.png")

# FIGURE 4: Weekly retention and conversion, this shows how many organisations remain active by week, and their conversion rate if they were active in that week.

weekly_active_counts = {w: org_meta[f"active_w{w}"].sum() for w in [1, 2, 3, 4]}
weekly_conversion_rates = {
    w: org_meta.loc[org_meta[f"active_w{w}"], "converted"].mean()
    for w in [1, 2, 3, 4]
}

fig, ax1 = plt.subplots(figsize=(7, 4))
ax2 = ax1.twinx()

ax1.bar([1, 2, 3, 4], [weekly_active_counts[w] for w in [1, 2, 3, 4]], color=INDIGO, alpha=0.7, label="Organisations active")
ax2.plot([1, 2, 3, 4], [weekly_conversion_rates[w] for w in [1, 2, 3, 4]], "o-", color=AMBER, linewidth=2.5, markersize=8, label="Conversion rate")

ax1.set_xlabel("Trial week")
ax1.set_ylabel("Organisations active")
ax2.set_ylabel("Conversion rate")
ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax2.set_ylim(0, 0.35)
ax1.set_title("Figure 4: Weekly engagement and conversion rate")

h1, l1 = ax1.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
ax1.legend(h1 + h2, l1 + l2, fontsize=9)

save_plot(f"{OUT}/fig4_weekly_retention.png")
print("Saved: fig4_weekly_retention.png")

# FIGURE 5: Weeks active versus conversion, this measures whether staying active across more weeks is linked to better conversion outcomes.

weeks_group = org_meta.groupby("weeks_active")["converted"].agg(["mean", "count"]).reset_index()

fig, ax = plt.subplots(figsize=(7, 4))
bar_colors = [RED, SLATE, AMBER, GREEN, GREEN]

ax.bar(weeks_group["weeks_active"], weeks_group["mean"], color=bar_colors[:len(weeks_group)], alpha=0.85)
ax.axhline(BASELINE, color=SLATE, linestyle="--", linewidth=1.2, label=f"Baseline {BASELINE:.1%}")
ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax.set_xticks(weeks_group["weeks_active"])
ax.set_xticklabels(["0 wks", "1 wk", "2 wks", "3 wks", "4 wks"], fontsize=10)
ax.set_ylabel("Conversion rate")
ax.set_title("Figure 5: Conversion rate by number of active weeks")

for _, row in weeks_group.iterrows():
    ax.text(row["weeks_active"], row["mean"] + 0.004, f"n={int(row['count'])}", ha="center", fontsize=9)

ax.legend(fontsize=9)

save_plot(f"{OUT}/fig5_weeks_active_conversion.png")
print("Saved: fig5_weeks_active_conversion.png")

# FIGURE 6: Module adoption and conversion, this compares how widely each product module is used and the conversion rate for organisations that used it.

mods = {
    "Scheduling": df["module"] == "Scheduling",
    "Mobile App": df["module"] == "Mobile",
    "Punch Clock": df["module"] == "PunchClock",
    "Absence Mgmt": df["module"] == "Absence",
    "Payroll": df["module"] == "Payroll",
    "Communication": df["module"] == "Communication",
}

module_rows = []
for module_name, module_mask in mods.items():
    module_orgs = set(df.loc[module_mask, "organization_id"])
    module_rows.append({
        "module": module_name,
        "adoption": len(module_orgs) / total_orgs,
        "cr": conversion_rate_for_orgs(org_conv, module_orgs)
    })

mod_df = pd.DataFrame(module_rows).sort_values("adoption", ascending=True)

fig, ax1 = plt.subplots(figsize=(9, 4))
ax2 = ax1.twinx()

y = np.arange(len(mod_df))

ax1.barh(y, mod_df["adoption"], color=INDIGO, alpha=0.75, label="Adoption rate")
ax2.plot(mod_df["cr"], y, "D", color=AMBER, markersize=9, label="Conversion rate", zorder=5)
ax2.axvline(BASELINE, color=SLATE, linestyle="--", linewidth=1)

ax1.set_yticks(y)
ax1.set_yticklabels(mod_df["module"])
ax1.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax2.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax1.set_xlabel("Adoption rate")
ax2.set_xlabel("Conversion rate")
ax1.set_title("Figure 6: Module adoption versus conversion rate")

h1, l1 = ax1.get_legend_handles_labels()
h2, l2 = ax2.get_legend_handles_labels()
ax1.legend(h1 + h2, l1 + l2, fontsize=9, loc="lower right")

save_plot(f"{OUT}/fig6_module_adoption.png")
print("Saved: fig6_module_adoption.png")


# FIGURE 7: Time to convert, this shows when conversion happens relative to trial start and helps toexplain whether conversion is mostly during or after the trial period.


converted_org_meta = org_meta[org_meta["converted"] == True].copy()
ttc = converted_org_meta["days_to_convert"].dropna().sort_values()

days_range = np.arange(0, 70)
pct_converted_all = [(ttc <= d).sum() / total_orgs for d in days_range]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

ax1.plot(days_range, [p * 100 for p in pct_converted_all], color=INDIGO, linewidth=2)
ax1.axvline(30, color=RED, linestyle="--", linewidth=1.2, label="Trial end (day 30)")
ax1.set_xlabel("Days since trial start")
ax1.set_ylabel("% of all organisations converted")
ax1.set_title("Conversion accumulation over time")
ax1.legend(fontsize=9)

ax2.hist(ttc, bins=20, color=INDIGO, alpha=0.8, edgecolor="white")
ax2.axvline(30, color=RED, linestyle="--", linewidth=1.2, label="Trial end")
ax2.axvline(ttc.median(), color=AMBER, linestyle="--", linewidth=1.2, label=f"Median ({ttc.median():.0f} days)")
ax2.set_xlabel("Days to convert")
ax2.set_ylabel("Number of organisations")
ax2.set_title("Distribution of time to convert")
ax2.legend(fontsize=9)

plt.suptitle("Figure 7: Time-to-convert analysis", fontsize=13, fontweight="bold")
save_plot(f"{OUT}/fig7_time_to_convert.png")
print("Saved: fig7_time_to_convert.png")

# TASK 1C: Conversion driver analysis, This section tests whether certain behaviours or engagement patterns are associated with conversion.


print_section("TASK 1C: CONVERSION DRIVER ANALYSIS")

activity_pivot = df.pivot_table(index="organization_id", columns="activity_name", aggfunc="size", fill_value=0)
feat = (activity_pivot > 0).astype(int)

feat["total_events"] = org_meta.set_index("organization_id")["total_events"]
feat["distinct_acts"] = org_meta.set_index("organization_id")["distinct_acts"]
feat["active_days"] = org_meta.set_index("organization_id")["active_days"]
feat["weeks_active"] = org_meta.set_index("organization_id")["weeks_active"]
feat["w1_events"] = (
    df[df["days_into_trial"] <= 7]
    .groupby("organization_id")
    .size()
    .reindex(feat.index, fill_value=0)
)

for week in [1, 2, 3, 4]:
    week_orgs = get_orgs_active_in_week(df, week)
    feat[f"active_w{week}"] = feat.index.isin(week_orgs).astype(int)

feat["converted"] = org_conv

print_subsection("Method 1: Chi-square test for each activity")
print(f"{'Activity':<46} {'N used':>7} {'CR used':>10} {'CR not used':>12} {'Lift':>8} {'p-value':>10}")
print("-" * 100)

stat_rows = []

for act in df["activity_name"].value_counts().index:
    used_orgs = set(df.loc[df["activity_name"] == act, "organization_id"])
    used_flag = org_conv.index.isin(used_orgs)

    used_group = org_conv[used_flag]
    not_used_group = org_conv[~used_flag]

    if len(used_group) < 5:
        continue

    contingency = pd.crosstab(used_flag.astype(int), org_conv)
    chi2, p_value, _, _ = stats.chi2_contingency(contingency)
    lift = used_group.mean() / not_used_group.mean() if not_used_group.mean() > 0 else np.nan

    stat_rows.append({
        "activity": act,
        "n_used": len(used_group),
        "cr_used": used_group.mean(),
        "cr_not": not_used_group.mean(),
        "lift": lift,
        "p_value": p_value
    })

    print(
        f"{act[:46]:<46} "
        f"{len(used_group):>7} "
        f"{used_group.mean():>10.3f} "
        f"{not_used_group.mean():>12.3f} "
        f"{lift:>8.2f} "
        f"{p_value:>10.4f}"
    )

stat_df = pd.DataFrame(stat_rows).sort_values("lift", ascending=False)
stat_df.to_csv(f"{OUT}/conversion_driver_stats.csv", index=False)

print_subsection("Method 2: Mann-Whitney U test for continuous engagement features")

conv_grp = org_meta[org_meta["converted"] == True]
nonconv_grp = org_meta[org_meta["converted"] == False]

for col in ["total_events", "distinct_acts", "active_days", "weeks_active"]:
    stat, p_value = stats.mannwhitneyu(conv_grp[col], nonconv_grp[col], alternative="two-sided")
    print(
        f"{col:<20} | "
        f"Converters median: {conv_grp[col].median():.1f} | "
        f"Non-converters median: {nonconv_grp[col].median():.1f} | "
        f"p-value: {p_value:.4f}"
    )

print_subsection("Method 3: Logistic Regression")

X = feat.drop(columns=["converted"])
y = feat["converted"].astype(int)

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

lr = LogisticRegression(
    max_iter=2000,
    class_weight="balanced",
    C=0.1,
    random_state=42
)

lr.fit(X_scaled, y)
lr_cv_auc = cross_val_score(lr, X_scaled, y, cv=5, scoring="roc_auc").mean()

coef_df = pd.DataFrame({
    "feature": X.columns,
    "coef": lr.coef_[0]
})
coef_df["odds_ratio"] = np.exp(coef_df["coef"])
coef_df = coef_df.sort_values("odds_ratio", ascending=False)

print("Top 15 features by odds ratio:")
print(coef_df[["feature", "odds_ratio", "coef"]].head(15).to_string(index=False))
print(f"\nLogistic Regression 5-fold CV AUC: {lr_cv_auc:.3f}")

coef_df.to_csv(f"{OUT}/logistic_regression_odds.csv", index=False)

print_subsection("Method 4: Random Forest")

rf = RandomForestClassifier(
    n_estimators=300,
    random_state=42,
    class_weight="balanced",
    n_jobs=-1
)

rf.fit(X, y)
rf_cv_auc = cross_val_score(rf, X, y, cv=5, scoring="roc_auc").mean()

importance_df = (
    pd.DataFrame({
        "feature": X.columns,
        "importance": rf.feature_importances_
    })
    .sort_values("importance", ascending=False)
    .head(10)
)

print("Top 10 features by feature importance:")
print(importance_df.to_string(index=False))
print(f"\nRandom Forest 5-fold CV AUC: {rf_cv_auc:.3f}")


print_subsection("Interpretation")
print(
    f"Both Logistic Regression (AUC={lr_cv_auc:.3f}) and Random Forest (AUC={rf_cv_auc:.3f}) "
    f"show near-random predictive performance."
)
print("This does not point to a modelling failure.")
print("It points to a limitation in the data itself.")
print("\nWhat the data suggests:")
print("- 51.9% of conversions happen after day 30")
print("- No conversions happen in the first 14 days")
print("- Conversion is likely influenced by follow-up sales activity, pricing, budget approval, or procurement")
print("- Those factors are not captured in the product event log")
print("\nConclusion:")
print("Trial behaviour is useful as a signal of intent, but it is not enough to directly predict conversion in this dataset.")

print_subsection("Method 5: RFM-style engagement segmentation")

rfm = org_meta[[
    "organization_id", "converted", "weeks_active", "total_events",
    "distinct_acts", "active_days", "distinct_modules"
]].copy()

rfm["recency"] = (
    df.groupby("organization_id")["days_into_trial"]
    .max()
    .reindex(rfm["organization_id"])
    .values
)

def rfm_segment(row):
    if row["weeks_active"] >= 3 and row["distinct_acts"] >= 4:
        return "Champions"
    elif row["weeks_active"] >= 3 or (row["weeks_active"] >= 2 and row["distinct_acts"] >= 3):
        return "Loyal"
    elif row["total_events"] >= 30 and row["weeks_active"] == 1:
        return "One-session power users"
    elif row["weeks_active"] >= 2:
        return "Returning"
    elif row["distinct_acts"] >= 3:
        return "Explorers"
    else:
        return "Passive / at-risk"

rfm["segment"] = rfm.apply(rfm_segment, axis=1)

seg_summary = (
    rfm.groupby("segment")["converted"]
    .agg(["mean", "count"])
    .rename(columns={"mean": "conv_rate", "count": "n_orgs"})
    .assign(lift=lambda d: d["conv_rate"] / BASELINE)
    .sort_values("conv_rate", ascending=False)
)

print(seg_summary.round(3))
rfm.to_csv(f"{OUT}/rfm_segments.csv", index=False)

# FIGURE 8: RFM segmentation result, This Figure Shows conversion rates across engagement-based behavioural segments.

fig, ax = plt.subplots(figsize=(9, 4))
seg_plot = seg_summary.sort_values("conv_rate")
seg_colors = [GREEN if x >= BASELINE else RED for x in seg_plot["conv_rate"]]

ax.barh(seg_plot.index, seg_plot["conv_rate"], color=seg_colors, alpha=0.85)
ax.axvline(BASELINE, color=SLATE, linestyle="--", linewidth=1.2, label=f"Baseline {BASELINE:.1%}")
ax.xaxis.set_major_formatter(mtick.PercentFormatter(1.0))

for i, (cr, n) in enumerate(zip(seg_plot["conv_rate"], seg_plot["n_orgs"])):
    ax.text(cr + 0.003, i, f"n={n}", va="center", fontsize=9)

ax.set_title("Figure 8: Conversion rate by engagement segment")
ax.legend(fontsize=9)

save_plot(f"{OUT}/fig8_rfm_segments.png")
print("\nSaved: fig8_rfm_segments.png")

# TASK 1D: Trial activation definition
# Since no single behaviour clearly predicts conversion, this section defines activation using a sequence of meaningful product actions.

print_section("TASK 1D: TRIAL ACTIVATION DEFINITION")

print("Activation logic used in this project:")
print("- Goal 1: Early schedule setup")
print("- Goal 2: Real operational use")
print("- Goal 3: Approval workflow")
print("- Goal 4: Sustained return across weeks")
print("\nThese are practical behavioural goals based on how the product is expected to deliver value.")
print("They should be treated as working hypotheses, not final proof of what causes conversion.")

mobile_orgs = get_orgs_for_activity(df, "Mobile.Schedule.Loaded")
punch_orgs = get_orgs_for_activity(df, "PunchClock.PunchedIn")
assign_orgs = get_orgs_for_activity(df, "Scheduling.Shift.AssignmentChanged")

g1_orgs = set(
    df[
        (df["activity_name"] == "Scheduling.Shift.Created") &
        (df["days_into_trial"] <= 3)
    ]
    .groupby("organization_id")
    .size()[lambda x: x >= 2]
    .index
)

g2_orgs = mobile_orgs & (punch_orgs | assign_orgs)

g3_orgs = set(
    df[df["activity_name"] == "Scheduling.Shift.Approved"]
    .groupby("organization_id")
    .size()[lambda x: x >= 2]
    .index
)

g4_orgs = set(
    org_id for org_id in org_conv.index
    if sum(org_id in get_orgs_active_in_week(df, week) for week in [1, 2, 3, 4]) >= 3
)

goal_records = []
for org_id in org_meta["organization_id"]:
    g1 = org_id in g1_orgs
    g2 = org_id in g2_orgs
    g3 = org_id in g3_orgs
    g4 = org_id in g4_orgs

    goal_records.append({
        "organization_id": org_id,
        "goal_1_early_schedule": g1,
        "goal_2_live_operations": g2,
        "goal_3_approval_workflow": g3,
        "goal_4_sustained_return": g4,
        "goals_completed": int(g1) + int(g2) + int(g3) + int(g4),
        "trial_activated": g1 and g2 and g3 and g4
    })

goals_df = pd.DataFrame(goal_records)
goals_df = goals_df.merge(org_meta[["organization_id", "converted"]], on="organization_id")
goals_df.to_csv(f"{OUT}/org_trial_goals.csv", index=False)

print("\nGoal completion summary:")
for col in [
    "goal_1_early_schedule",
    "goal_2_live_operations",
    "goal_3_approval_workflow",
    "goal_4_sustained_return",
    "trial_activated"
]:
    n = goals_df[col].sum()
    pct = goals_df[col].mean()
    cr = goals_df.loc[goals_df[col], "converted"].mean()
    lift = cr / BASELINE

    print(
        f"- {col}: "
        f"{n} organisations | "
        f"share = {pct:.1%} | "
        f"conversion rate = {cr:.3f} | "
        f"lift = {lift:.2f}x"
    )



goal_cols = [
    "goal_1_early_schedule",
    "goal_2_live_operations",
    "goal_3_approval_workflow",
    "goal_4_sustained_return",
    "trial_activated"
]

goal_labels = [
    "G1: Early\nSchedule",
    "G2: Live\nOperations",
    "G3: Approval\nWorkflow",
    "G4: Sustained\nReturn",
    "Fully\nActivated"
]

goal_ns = [goals_df[c].sum() for c in goal_cols]
goal_crs = [goals_df.loc[goals_df[c], "converted"].mean() for c in goal_cols]
goal_colors = [INDIGO, INDIGO, INDIGO, GREEN, "#7C3AED"]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

ax1.bar(range(5), goal_ns, color=goal_colors, alpha=0.85)
ax1.set_xticks(range(5))
ax1.set_xticklabels(goal_labels, fontsize=8.5)
ax1.set_ylabel("Organisations completing goal")
ax1.set_title("Goal completion volume")

for i, n in enumerate(goal_ns):
    ax1.text(i, n + 5, f"n={n}", ha="center", fontsize=9)

ax2.bar(range(5), goal_crs, color=goal_colors, alpha=0.85)
ax2.axhline(BASELINE, color=SLATE, linestyle="--", linewidth=1.2, label=f"Baseline {BASELINE:.1%}")
ax2.set_xticks(range(5))
ax2.set_xticklabels(goal_labels, fontsize=8.5)
ax2.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
ax2.set_ylabel("Conversion rate")
ax2.set_title("Conversion rate at each goal")
ax2.legend(fontsize=9)

for i, cr in enumerate(goal_crs):
    ax2.text(i, cr + 0.004, f"{cr:.1%}", ha="center", fontsize=9)

plt.suptitle("Figure 9: Trial goal completion and conversion", fontsize=13, fontweight="bold")
save_plot(f"{OUT}/fig9_trial_goals.png")
print("\nSaved: fig9_trial_goals.png")



# TASK 3: Product metrics and descriptive summary


print_section("TASK 3: DESCRIPTIVE ANALYTICS AND PRODUCT METRICS")

print("Core trial metrics")
print("-" * 70)
print(f"{'Total trial organisations':<50} {total_orgs:>12,}")
print(f"{'Converted to paid':<50} {converted_orgs:>12,}")
print(f"{'Trial-to-paid conversion rate':<50} {converted_orgs / total_orgs:>12.1%}")
print(f"{'Median days to convert':<50} {org_meta['days_to_convert'].median():>12.0f}")
print(f"{'Mean days to convert':<50} {org_meta['days_to_convert'].mean():>12.1f}")

ttc_all = org_meta.loc[org_meta["converted"], "days_to_convert"]
print(f"{'Converted within 30 days':<50} {(ttc_all <= 30).mean():>12.1%}")
print(f"{'Converted after 30 days':<50} {(ttc_all > 30).mean():>12.1%}")
print(f"{'Week 1 activation rate':<50} {org_meta['active_w1'].mean():>12.1%}")
print(f"{'Week 2 retention rate':<50} {org_meta['active_w2'].mean():>12.1%}")
print(f"{'Week 3 retention rate':<50} {org_meta['active_w3'].mean():>12.1%}")
print(f"{'Week 4 retention rate':<50} {org_meta['active_w4'].mean():>12.1%}")
print(f"{'Week 1 to Week 2 retention':<50} {org_meta['active_w2'].sum() / org_meta['active_w1'].sum():>12.1%}")
print(f"{'Median events per organisation':<50} {org_meta['total_events'].median():>12.0f}")
print(f"{'Median distinct activities per organisation':<50} {org_meta['distinct_acts'].median():>12.0f}")

print("\nModule-level summary")
print("-" * 70)
print(f"{'Module':<28} {'Adoption':>10} {'Conv Rate':>12} {'Lift':>8}")

for module_name, module_mask in mods.items():
    module_orgs = set(df.loc[module_mask, "organization_id"])
    adoption = len(module_orgs) / total_orgs
    cr = conversion_rate_for_orgs(org_conv, module_orgs)
    lift = cr / BASELINE

    print(f"{module_name:<28} {adoption:>10.1%} {cr:>12.1%} {lift:>8.2f}x")

print_subsection("Key product insights")
print("1. Conversion is mostly a post-trial event.")
print("   More than half of converted organisations convert after day 30.")
print("")
print("2. The largest drop-off happens between Week 1 and Week 2.")
print("   This is the clearest place where intervention could matter most.")
print("")
print("3. Scheduling is widely used, but mobile usage is much lower.")
print("   Mobile behaviour may reflect real team adoption better than admin-only activity.")
print("")
print("4. Communication usage is linked with lower conversion.")
print("   This may suggest support-related or issue-related usage rather than healthy adoption.")
print("")
print("5. Template usage is relatively low, but may signal repeatable workflow setup.")
print("   That makes it worth watching as a higher-quality usage behaviour.")

print("\nFinished. All output files have been saved in ../outputs/")