# MIT 805 Big Data Semester Project (2026)

## Arrival-Delay Risk by Scheduled Departure Time (2020–2024) Using PySpark

### Authors
- Pumlisa Lusiba (u25162323)
- Makhosazane Mthethwa (u14241839)


Prepared for: Dr. Olaperi Okuboyejo

Module: MIT 805 – Big Data

---

## Project Overview

This project investigates the relationship between scheduled departure time and the likelihood of a completed flight arriving at least 15 minutes late.

Using PySpark and MapReduce-style distributed processing, we analyse U.S. domestic airline on-time performance data from 2020 to 2024. The analysis evaluates how arrival-delay risk changes throughout the day and examines the contribution of late-arriving aircraft delays across different departure periods.

The project demonstrates:

- Large-scale data processing using PySpark
- MapReduce-style transformations and aggregations
- Spark shuffle and reduction operations
- DAG-based execution concepts
- Data visualization and business interpretation

---

## Research Question

**What is the relationship between scheduled departure hour and the likelihood of a completed flight arriving at least 15 minutes late during the period 2020–2024?**

Additional analysis investigates how delay minutes attributed to a late-arriving aircraft differ between:

- Early Morning (05:00–09:59)
- Midday (10:00–15:59)
- Evening (16:00–22:59)

---

## Dataset

### Source

U.S. Bureau of Transportation Statistics (BTS)

Reporting Carrier On-Time Performance Dataset (1987–Present)

TranStats Portal:

https://www.transtats.bts.gov/

### Dataset Sizes

| Dataset Stage | Size |
|--------------|------|
| Raw Dataset (2015–2024) | 26.495 GiB |
| Download Archive | 2.971 GiB |
| Working Dataset (2020–2024) | 14.130 GB |
| Processing Dataset | 60 monthly files (2020–2024) |

### Data Coverage

- January 2020 to December 2024
- U.S. domestic non-stop airline flights
- More than 31 million records with valid scheduled departure times

### Important Note

This repository does not redistribute BTS raw data.

Users must download the data directly from the BTS TranStats website and comply with all applicable usage conditions.

---

## Analytical Measures

### Completed Arrival

A flight is considered a completed arrival when:

- Cancelled = 0
- Diverted = 0
- ArrDelay is not null

### Late Completed Arrival

A completed arrival with:

`ArrDelay >= 15 minutes`

### Arrival Delay Rate

Late Completed Arrivals divided by Completed Arrivals.

---

## PySpark MapReduce Workflow

### Mapping / Transformation

The raw dataset is cleaned and transformed to create:

- ScheduledDepHour
- DeparturePeriod
- CompletedArrival
- LateCompletedArrival

Example:

```python
.withColumn(
    "ScheduledDepHour",
    F.floor(F.col("CRSDepTime") / 100).cast("int")
)
```

### Shuffle / Grouping

Flights are grouped by:

```python
groupBy("Year", "ScheduledDepHour")
```

Spark redistributes records across partitions during this stage.

### Reduction / Aggregation

Aggregations calculate:

- Scheduled flights
- Completed arrivals
- Late completed arrivals
- Delay rates
- Delay-cause totals

### Execution Evidence

Execution plans demonstrate:

- HashAggregate
- Exchange hashpartitioning
- Range partitioning
- Sort operations
- AdaptiveSparkPlan

These operators provide evidence of Spark's distributed processing model and shuffle behaviour.

---

## Key Findings

### Annual Late-Arrival Rate

| Year | Late Rate |
|------|-----------|
| 2020 | 9.82% |
| 2021 | 17.19% |
| 2022 | 21.08% |
| 2023 | 20.56% |
| 2024 | 20.82% |

### 2024 Operational Period Comparison

| Period | Late Rate |
|---------|----------|
| Early Morning | 12.27% |
| Midday | 20.40% |
| Evening | 28.66% |

Key observation:

Flights scheduled later in the day experienced substantially higher arrival-delay risk than flights departing during early morning periods.

---

## Visualizations

The project includes:

- Arrival-delay heatmap by hour and year
- Period-level delay comparison
- Delay-cause analysis
- Spark execution plan evidence

Figures are available in the `figures/` directory.

---

## Repository Structure

```text
project/
│
├── README.md
├── requirements.txt
├── data/
│   └── README.md
├─
