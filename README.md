# NYC Traffic & Road Infrastructure Analysis
### CIS 9440 | Data Warehousing & Analytics | Group 3 | Baruch College

> **Group Members:** Zesen Chen · Bridgette Wang · Paven Oommen · Sung Ik Park · Raúl J. Solá Navarro

This repository contains the **final project report** for CIS 9440, built as a Quarto
website and published via GitHub Pages. It documents the full data warehousing pipeline:
data model → ELT (BigQuery + dbt) → analysis (SQL) → visualization (Looker Studio).

---

## 🗂️ Repository Structure

```
nyc-analytics-project-rjsn/
├── _quarto.yml                        # Quarto website config
├── nyc-analytics-project-rjsn.Rproj  # RStudio project file
├── styles.css                         # Custom theme styles
├── index.qmd                          # Home / Introduction
├── sections/
│   ├── 02-dimensional-model.qmd      # ERD + table definitions
│   ├── 03-elt-pipeline.qmd           # Cloud Function → BQ → dbt
│   ├── 04-analysis.qmd               # SQL queries + dashboard
│   ├── 05-conclusion.qmd             # Findings + recommendations
│   └── 06-references.qmd             # Citations + links
├── images/                            # Screenshots (see To-Do below)
│   ├── erd-diagram.png
│   ├── cloud-function.png
│   ├── bq-raw-311.png
│   ├── bq-raw-traffic.png
│   ├── bq-datasets.png
│   ├── dbt-dag.png
│   ├── dashboard-page1.png
│   ├── dashboard-page2.png
│   └── dashboard-page3.png
├── queries/
│   └── milestone5_queries.sql        # All 7 BigQuery analytical queries
└── docs/                              # Quarto render output (GitHub Pages)
```

---

## 🚀 Setup — RStudio (First Time)

### 1. Prerequisites

Install these if you don't have them already:

| Tool | Download |
|------|----------|
| R (≥ 4.3) | https://cran.r-project.org |
| RStudio Desktop | https://posit.co/products/open-source/rstudio/ |
| Quarto CLI | https://quarto.org/docs/get-started/ |

Verify Quarto is installed by running in your terminal:
```bash
quarto --version
```

### 2. Clone the repository

```bash
git clone https://github.com/RaulSolaNavarro/nyc-analytics-project-rjsn.git
cd nyc-analytics-project-rjsn
```

### 3. Open the RStudio project

Double-click `nyc-analytics-project-rjsn.Rproj` — this opens the project in RStudio
with the correct working directory and build settings already configured.

### 4. Install required R packages

In the RStudio Console:

```r
# Core packages needed for the Quarto website
install.packages(c(
  "quarto",    # R interface to Quarto CLI
  "knitr",     # Document rendering engine
  "rmarkdown"  # Underlying rendering support
))

# Optional: only needed if you wire up a live BigQuery connection later
# install.packages(c("bigrquery", "DBI", "dplyr", "ggplot2", "gt"))
```

### 5. Preview the site locally

**Option A: RStudio Build pane:**  
Go to the **Build** tab (top-right pane) → click **Render Website**.

**Option B: RStudio Terminal:**
```bash
quarto preview
```
This opens a live-reloading preview at `http://localhost:4848`.

### 6. Full render (creates `docs/` folder)

```bash
quarto render
```
The output goes to `docs/`, this is what GitHub Pages will serve.

---

## 📸 Images To-Do

Before the site renders without placeholder warnings, add these screenshots
to the `images/` folder:

| File | Where to get it |
|------|----------------|
| `erd-diagram.png` | Export from dbdiagram.io → Download PNG |
| `cloud-function-311.png`     | GCP Console → Cloud Functions → 311 function    |
| `cloud-function-traffic.png` | GCP Console → Cloud Functions → traffic function |
| `bq-raw-311.png` | BigQuery → group_3_raw → raw_311 table → Preview tab |
| `bq-raw-traffic.png` | BigQuery → group_3_raw → raw_traffic table → Preview tab |
| `bq-datasets.png` | BigQuery → left-hand resource panel showing all three datasets |
| `dbt-dag.png` | dbt docs or dbt Cloud → Lineage graph → screenshot full DAG |
| `dashboard-page1.png` | Looker Studio → Page 1 → screenshot |
| `dashboard-page2.png` | Looker Studio → Page 2 → screenshot |
| `dashboard-page3.png` | Looker Studio → Page 3 → screenshot |

---

## 🌐 Publishing to GitHub Pages

### One-time setup

1. Push the repository to GitHub (if not already there)
2. Go to **Settings → Pages** in your GitHub repo
3. Set **Source** to: `Deploy from a branch`
4. Set **Branch** to: `main` and **folder** to: `/docs`
5. Click **Save**

GitHub will publish the site at:  
`https://raulsolanavarro.github.io/nyc-analytics-project-rjsn/`

### Update workflow (after every edit)

```bash
quarto render          # re-renders everything into /docs
git add .
git commit -m "Update report content"
git push
```
GitHub Pages will automatically update within ~1 minute.

---

## 🔗 Links

| Resource | URL |
|----------|-----|
| Published site | https://raulsolanavarro.github.io/nyc-analytics-project-rjsn/ |
| DataStudio dashboard | https://datastudio.google.com/reporting/b7835c43-9acb-4aa1-9cd6-e9230b5421d8 |
| BigQuery project | `raul-cis-9440-spring2026` |
| dbt models | [/models](./models/) |
| SQL queries | [/queries/milestone5_queries.sql](./queries/milestone5_queries.sql) |
