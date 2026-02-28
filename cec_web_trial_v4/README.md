# CEC Factory Trial V4

Free local trial web system for production tracking, machine status, OT, breakdown, variance reporting, and customer item-by-item status tracing.

## New in V4
- Customer **item tracker** page
- Trace each bought item from **customer file -> job -> batch -> current process**
- **Planner update form** for remaining hours and ready time
- Timeline trace for each customer item
- Customer file detail now shows **current status / stage / planner ETA / risk**
- Process scans now update the linked customer item timeline automatically

## Core modules
- Dashboard
- Machine Board
- Lines
- Machines
- Customers
- Customer Files
- Item Tracker
- Products
- Materials
- Jobs
- Batches
- Process Scan
- OT
- Breakdowns
- Reports

## Tech Stack
- Python
- Flask
- SQLite
- Bootstrap

## How to run on Windows

### 1) Open terminal in this folder

### 2) Create virtual environment
```bash
py -m venv .venv
```

### 3) Activate
PowerShell:
```bash
.venv\Scripts\Activate.ps1
```

Command Prompt:
```bash
.venv\Scripts\activate.bat
```

### 4) Install packages
```bash
py -m pip install -r requirements.txt
```

### 5) Start app
```bash
py app.py
```

### 6) Open browser
```text
http://127.0.0.1:5000
```

## Recommended test flow
1. Open **Customer Files**
2. Open **CF-0001**
3. Check each line's **Current / Stage / Planner Ready / Risk**
4. Click **Trace**
5. In the item detail page, update **remaining hours** and **ready at**
6. Go to **Jobs** / **Batches** / **Process Scan**
7. Record another scan and go back to the item timeline

## Demo data already included
- Customers
- Products
- Materials
- Lines
- Machines
- Customer files with multiple product lines
- Jobs
- Batches
- Process logs
- OT logs
- Breakdown records
- Planner ETA demo data
- Item timeline demo data

## Notes
- This is still a local trial version.
- No login/permissions yet.
- No barcode image printing yet.
- No BOM deduction yet.
- No cloud deployment yet.
- Database file is `trial_app_v4.db`.

## Suggested next version
- login + roles
- barcode label printing
- BOM / recipe consumption
- Excel export
- cloud deployment
- tablet-friendly shopfloor UI
