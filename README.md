# Splendor Analytics — Trial Activation Challenge

**Submitted by:** Chukwuebuka Jeremiah Enoch | [@EnochJeremiah6]

---

## The Problem

Splendor Analytics runs a 30-day free trial for its workforce management platform. About 1 in 5 organisations later convert to a paid plan.

The issue is that the team cannot clearly tell:

* which organisations are likely to convert
* when they should step in during the trial
* what product behaviour actually matters

In this project, I focused on understanding what is really happening during the trial and how to track meaningful usage.

---

## What I Found

| Finding                | Detail                                             |
| ---------------------- | -------------------------------------------------- |
| **Conversion rate**    | 21.3% across 966 organisations                     |
| **No clear predictor** | No single activity strongly explains conversion    |
| **Main reason**        | 51.9% of conversions happen after the trial ends   |
| **Best signal**        | Orgs active in 3+ weeks convert at 30.9%           |
| **Biggest issue**      | Huge drop from Week 1 to Week 2                    |
| **Week 4 matters**     | Orgs still active late in the trial perform better |
| **Messaging insight**  | Lower conversion, likely used when issues come up  |

---

## Trial Activation Definition

I defined Trial Activation as completing four key behaviours during the trial:

| Goal                      | Definition                                                            | Completion Rate | CR    | Lift  |
| ------------------------- | --------------------------------------------------------------------- | --------------- | ----- | ----- |
| **G1 – Early Setup**      | Create at least 2 shifts within first 3 days                          | 61.4%           | 23.1% | 1.08x |
| **G2 – Real Usage**       | View mobile schedule and perform an action (punch-in or shift change) | 27.6%           | 23.2% | 1.09x |
| **G3 – Approval Flow**    | Approve at least 2 shifts                                             | 13.9%           | 23.9% | 1.12x |
| **G4 – Keep Coming Back** | Active in at least 3 different weeks                                  | 17.2%           | 24.7% | 1.16x |

**Fully Activated:** 64 organisations (6.6%)

These steps follow how the product is meant to be used:
Set up → Use → Approve → Return

These are not perfect predictors, just a practical way to track meaningful usage.

---

## Repo Structure

```text
splendor_challenge/
├── notebooks/
│   └── analysis.py
├── sql/
│   ├── staging/
│   ├── marts/
│   └── dbt_project/
├── outputs/
├── data/
├── README.md
└── requirements.txt
```

---

## What I Did

### 1. Data Cleaning and Preparation

* Loaded the raw dataset
* Converted date columns properly
* Checked for missing values
* Removed duplicate rows
* Filtered events to stay within the 30-day trial
* Created new fields like:

  * days_into_trial
  * trial_week
  * module

I also built an organisation-level dataset with things like:

* total activity
* number of features used
* active days
* weeks active
* time to convert

---

### 2. Analysis

I explored the data using charts and summaries, including:

* activity volume
* conversion by activity
* cohort trends
* weekly retention
* weeks active vs conversion
* module usage
* time to convert
* engagement segments
* trial goal completion

---

### 3. Conversion Driver Checks

I tested different ways to see what might explain conversion:

* Chi-square tests
* Mann-Whitney test
* Logistic Regression
* Random Forest
* Behaviour-based segmentation

All of them pointed to the same thing:

There is no strong in-trial behaviour that clearly predicts conversion.

The main reason is simple:
Many organisations convert after the trial ends, so the decision is likely influenced by things outside the product.

---

### 4. Trial Goals

Since the data does not give a clear predictor, I defined trial goals based on how the product should be used.

The idea is:

* start using it properly
* use it in real work
* complete workflows
* keep returning

This gives a better way to track activation even if conversion itself happens later.

---

### 5. SQL Models

I built SQL models (dbt-style) to track:

* cleaned trial events
* organisation-level summaries
* goal completion
* activation status

This makes it easier to monitor behaviour in a structured way.

---

## Outputs

The script generates:

* charts (PNG files)
* cleaned datasets
* model outputs

All saved in the `outputs/` folder.

---

## Key Insights

1. Conversion mostly happens after the trial
   This means product usage alone cannot explain it

2. The biggest problem is early drop-off
   Most users do not come back after Week 1

3. Consistent usage matters more than heavy one-time use

4. Mobile activity likely shows real team usage

5. Messaging may indicate struggling users rather than engaged ones

---

## How to Run

### Python

```bash
pip install -r requirements.txt

cd notebooks
python analysis.py
```

---

### SQL (DuckDB)

```python
import duckdb, pandas as pd

con = duckdb.connect()
df = pd.read_csv("data/Copy of DA task.csv")
con.register("raw__trial_events", df)
```

Run SQL models in order:

1. staging
2. organisation summary
3. goals
4. activation

---

## Requirements

See `requirements.txt`
