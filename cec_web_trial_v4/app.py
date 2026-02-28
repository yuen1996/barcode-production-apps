from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

from flask import Flask, flash, g, redirect, render_template, request, url_for

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / 'trial_app_v4.db'

app = Flask(__name__)
app.config['SECRET_KEY'] = 'cec-trial-v4-secret-key'

PROCESS_FLOW = ['MIXING', 'EXTRUDER', 'VULCANISING', 'CUTTING', 'FINISHING', 'PACKING', 'STORE_RECEIVING']
SECTIONS = ['STORE', 'RND', 'PLANNING', 'MIXING', 'EXTRUDER', 'VULCANISING', 'CUTTING', 'FINISHING', 'PACKING', 'QC', 'STORE_RECEIVING', 'MAINTENANCE']
MACHINE_STATUSES = ['IDLE', 'RUNNING', 'BREAKDOWN', 'SETUP', 'MAINTENANCE']
ITEM_STATUS_CHOICES = ['WAITING_PLANNING', 'READY_FOR_PRODUCTION', 'JOB_CREATED', 'IN_PRODUCTION', 'HOLD', 'COMPLETED']


def normalize_dt(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    return value.replace('T', ' ')


def get_db() -> sqlite3.Connection:
    if 'db' not in g:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        g.db = conn
    return g.db


@app.teardown_appcontext
def close_db(exception: Exception | None) -> None:
    db = g.pop('db', None)
    if db is not None:
        db.close()


def query_all(sql: str, params: tuple = ()) -> list[sqlite3.Row]:
    cur = get_db().execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def query_one(sql: str, params: tuple = ()) -> sqlite3.Row | None:
    cur = get_db().execute(sql, params)
    row = cur.fetchone()
    cur.close()
    return row


def execute(sql: str, params: tuple = ()) -> int:
    db = get_db()
    cur = db.execute(sql, params)
    db.commit()
    lastrowid = cur.lastrowid
    cur.close()
    return lastrowid


def execute_script(script: str) -> None:
    db = get_db()
    db.executescript(script)
    db.commit()


def init_db() -> None:
    db = sqlite3.connect(DB_PATH)
    db.executescript(
        '''
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sku TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            unit TEXT NOT NULL DEFAULT 'KG',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS materials (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            uom TEXT NOT NULL DEFAULT 'KG',
            cost_per_kg REAL NOT NULL DEFAULT 0,
            stock_qty REAL NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS production_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL,
            section_name TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS machines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_code TEXT UNIQUE NOT NULL,
            machine_name TEXT NOT NULL,
            section_name TEXT NOT NULL,
            process_name TEXT NOT NULL,
            line_id INTEGER,
            status TEXT NOT NULL DEFAULT 'IDLE',
            is_active INTEGER NOT NULL DEFAULT 1,
            last_activity_at TEXT,
            current_batch_id INTEGER,
            current_note TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(line_id) REFERENCES production_lines(id),
            FOREIGN KEY(current_batch_id) REFERENCES batches(id)
        );

        CREATE TABLE IF NOT EXISTS customer_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_no TEXT UNIQUE NOT NULL,
            customer_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            due_date TEXT,
            po_no TEXT,
            remarks TEXT,
            status TEXT NOT NULL DEFAULT 'OPEN',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(customer_id) REFERENCES customers(id)
        );

        CREATE TABLE IF NOT EXISTS customer_file_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_file_id INTEGER NOT NULL,
            line_no INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            ordered_qty_kg REAL NOT NULL,
            target_line_id INTEGER,
            remarks TEXT,
            status TEXT NOT NULL DEFAULT 'PENDING_JOB',
            planned_job_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(customer_file_id) REFERENCES customer_files(id),
            FOREIGN KEY(product_id) REFERENCES products(id),
            FOREIGN KEY(target_line_id) REFERENCES production_lines(id),
            FOREIGN KEY(planned_job_id) REFERENCES jobs(id)
        );

        CREATE TABLE IF NOT EXISTS jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_no TEXT UNIQUE NOT NULL,
            customer_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            planned_date TEXT NOT NULL,
            planned_qty_kg REAL NOT NULL,
            target_line_id INTEGER,
            source_customer_file_id INTEGER,
            source_customer_file_item_id INTEGER,
            remarks TEXT,
            status TEXT NOT NULL DEFAULT 'OPEN',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(customer_id) REFERENCES customers(id),
            FOREIGN KEY(product_id) REFERENCES products(id),
            FOREIGN KEY(target_line_id) REFERENCES production_lines(id),
            FOREIGN KEY(source_customer_file_id) REFERENCES customer_files(id),
            FOREIGN KEY(source_customer_file_item_id) REFERENCES customer_file_items(id)
        );

        CREATE TABLE IF NOT EXISTS batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_no TEXT UNIQUE NOT NULL,
            job_id INTEGER NOT NULL,
            recipe_code TEXT,
            planned_input_kg REAL NOT NULL,
            current_process TEXT NOT NULL DEFAULT 'NOT_STARTED',
            status TEXT NOT NULL DEFAULT 'OPEN',
            barcode_text TEXT NOT NULL,
            assigned_line_id INTEGER,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(job_id) REFERENCES jobs(id),
            FOREIGN KEY(assigned_line_id) REFERENCES production_lines(id)
        );

        CREATE TABLE IF NOT EXISTS process_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            process_name TEXT NOT NULL,
            scan_time TEXT NOT NULL,
            input_qty_kg REAL NOT NULL,
            good_qty_kg REAL NOT NULL,
            reject_qty_kg REAL NOT NULL DEFAULT 0,
            operator_name TEXT,
            machine_id INTEGER,
            machine_name TEXT,
            line_id INTEGER,
            next_action TEXT NOT NULL DEFAULT 'MOVE_NEXT',
            remarks TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(id),
            FOREIGN KEY(machine_id) REFERENCES machines(id),
            FOREIGN KEY(line_id) REFERENCES production_lines(id)
        );

        CREATE TABLE IF NOT EXISTS ot_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            section_name TEXT NOT NULL,
            line_id INTEGER,
            machine_id INTEGER,
            work_date TEXT NOT NULL,
            employees INTEGER NOT NULL,
            ot_hours REAL NOT NULL,
            remarks TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(line_id) REFERENCES production_lines(id),
            FOREIGN KEY(machine_id) REFERENCES machines(id)
        );

        CREATE TABLE IF NOT EXISTS breakdowns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            machine_id INTEGER NOT NULL,
            machine_name TEXT NOT NULL,
            section_name TEXT NOT NULL,
            start_time TEXT NOT NULL,
            end_time TEXT,
            technician_name TEXT,
            batch_id INTEGER,
            reason TEXT NOT NULL,
            remarks TEXT,
            status TEXT NOT NULL DEFAULT 'OPEN',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(machine_id) REFERENCES machines(id),
            FOREIGN KEY(batch_id) REFERENCES batches(id)
        );

        CREATE TABLE IF NOT EXISTS item_planner_updates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL UNIQUE,
            planner_status TEXT NOT NULL DEFAULT 'WAITING_PLANNING',
            remaining_hours REAL,
            ready_at TEXT,
            note TEXT,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(item_id) REFERENCES customer_file_items(id)
        );

        CREATE TABLE IF NOT EXISTS item_tracking_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            event_time TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT NOT NULL,
            stage_name TEXT,
            status_label TEXT,
            source_ref TEXT,
            user_role TEXT NOT NULL DEFAULT 'SYSTEM',
            note TEXT,
            FOREIGN KEY(item_id) REFERENCES customer_file_items(id)
        );

        
        CREATE TABLE IF NOT EXISTS next_process_labels (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transfer_barcode TEXT UNIQUE NOT NULL,
            batch_id INTEGER NOT NULL,
            from_process TEXT NOT NULL,
            to_process TEXT NOT NULL,
            issued_qty_kg REAL NOT NULL,
            received_qty_kg REAL,
            qty_loss_kg REAL NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'ISSUED',
            recipe_code TEXT,
            item_name TEXT,
            document_no TEXT,
            customer_name TEXT,
            issued_at TEXT NOT NULL,
            received_at TEXT,
            issued_machine_id INTEGER,
            received_machine_id INTEGER,
            issued_by TEXT,
            received_by TEXT,
            notes TEXT,
            FOREIGN KEY(batch_id) REFERENCES batches(id),
            FOREIGN KEY(issued_machine_id) REFERENCES machines(id),
            FOREIGN KEY(received_machine_id) REFERENCES machines(id)
        );
        '''
    )

    existing = db.execute('SELECT COUNT(*) AS c FROM customers').fetchone()[0]
    if existing == 0:
        seed_demo_data(db)

    db.commit()
    db.close()


def seed_demo_data(db: sqlite3.Connection) -> None:
    db.executemany(
        'INSERT INTO customers(code, name) VALUES(?, ?)',
        [('CUS-001', 'Alpha Rubber'), ('CUS-002', 'Beta Components'), ('CUS-003', 'Delta Seals')]
    )
    db.executemany(
        'INSERT INTO products(sku, name, unit) VALUES(?, ?, ?)',
        [
            ('PRD-001', 'Rubber Seal', 'KG'),
            ('PRD-002', 'Industrial Gasket', 'KG'),
            ('PRD-003', 'Oil Resistant Ring', 'KG'),
        ]
    )
    db.executemany(
        'INSERT INTO materials(code, name, uom, cost_per_kg, stock_qty) VALUES(?, ?, ?, ?, ?)',
        [
            ('MAT-001', 'Natural Rubber', 'KG', 8.50, 1200),
            ('MAT-002', 'Carbon Black', 'KG', 6.20, 450),
            ('MAT-003', 'Sulphur', 'KG', 3.80, 180),
            ('MAT-004', 'Processing Oil', 'KG', 5.10, 320),
        ]
    )
    db.executemany(
        'INSERT INTO production_lines(code, name, section_name) VALUES(?, ?, ?)',
        [
            ('LINE-MX-A', 'Mixing Line A', 'MIXING'),
            ('LINE-MX-B', 'Mixing Line B', 'MIXING'),
            ('LINE-EX-A', 'Extruder Line A', 'EXTRUDER'),
            ('LINE-VC-A', 'Vulcanising Line A', 'VULCANISING'),
            ('LINE-CT-A', 'Cutting Line A', 'CUTTING'),
            ('LINE-FN-A', 'Finishing Line A', 'FINISHING'),
            ('LINE-PK-A', 'Packing Line A', 'PACKING'),
        ]
    )

    line_ids = {row[0]: row[1] for row in db.execute('SELECT code, id FROM production_lines').fetchall()}
    customer_ids = {row[0]: row[1] for row in db.execute('SELECT code, id FROM customers').fetchall()}
    product_ids = {row[0]: row[1] for row in db.execute('SELECT sku, id FROM products').fetchall()}

    db.executemany(
        '''INSERT INTO customer_files(file_no, customer_id, order_date, due_date, po_no, remarks, status)
           VALUES(?, ?, ?, ?, ?, ?, ?)''',
        [
            ('CF-0001', customer_ids['CUS-001'], '2026-02-28', '2026-03-05', 'PO-ALPHA-1001', 'Alpha Rubber mixed order file', 'OPEN'),
            ('CF-0002', customer_ids['CUS-002'], '2026-02-28', '2026-03-07', 'PO-BETA-8802', 'Beta Components urgent mixed order', 'OPEN'),
        ]
    )

    file_ids = {row[0]: row[1] for row in db.execute('SELECT file_no, id FROM customer_files').fetchall()}
    db.executemany(
        '''INSERT INTO customer_file_items(customer_file_id, line_no, product_id, ordered_qty_kg, target_line_id, remarks, status)
           VALUES(?, ?, ?, ?, ?, ?, ?)''',
        [
            (file_ids['CF-0001'], 1, product_ids['PRD-001'], 500, line_ids['LINE-MX-A'], 'Rubber Seal 500KG', 'JOB_CREATED'),
            (file_ids['CF-0001'], 2, product_ids['PRD-003'], 180, line_ids['LINE-MX-B'], 'Oil Resistant Ring 180KG', 'PENDING_JOB'),
            (file_ids['CF-0002'], 1, product_ids['PRD-002'], 350, line_ids['LINE-MX-B'], 'Industrial Gasket 350KG', 'JOB_CREATED'),
            (file_ids['CF-0002'], 2, product_ids['PRD-001'], 120, line_ids['LINE-MX-A'], 'Additional Rubber Seal 120KG', 'PENDING_JOB'),
        ]
    )

    machine_rows = [
        ('MX-A-01', 'Mixer A-01', 'MIXING', 'MIXING', line_ids['LINE-MX-A'], 'RUNNING', '2026-02-28 08:15', None, 'Running Alpha Rubber batch'),
        ('MX-A-02', 'Mixer A-02', 'MIXING', 'MIXING', line_ids['LINE-MX-A'], 'IDLE', '2026-02-28 07:00', None, 'Waiting next batch'),
        ('MX-B-01', 'Mixer B-01', 'MIXING', 'MIXING', line_ids['LINE-MX-B'], 'SETUP', '2026-02-28 07:40', None, 'Colour change setup'),
        ('EX-A-01', 'Extruder A-01', 'EXTRUDER', 'EXTRUDER', line_ids['LINE-EX-A'], 'RUNNING', '2026-02-28 08:30', None, 'Running current batch'),
        ('EX-A-02', 'Extruder A-02', 'EXTRUDER', 'EXTRUDER', line_ids['LINE-EX-A'], 'BREAKDOWN', '2026-02-28 08:00', None, 'Sensor unstable'),
        ('VC-A-01', 'Vulcaniser A-01', 'VULCANISING', 'VULCANISING', line_ids['LINE-VC-A'], 'IDLE', '2026-02-28 06:55', None, 'Available'),
        ('CT-A-01', 'Cutting A-01', 'CUTTING', 'CUTTING', line_ids['LINE-CT-A'], 'RUNNING', '2026-02-28 08:50', None, 'Selection run'),
        ('FN-A-01', 'Finishing A-01', 'FINISHING', 'FINISHING', line_ids['LINE-FN-A'], 'IDLE', '2026-02-28 06:30', None, 'Available'),
        ('PK-A-01', 'Packing A-01', 'PACKING', 'PACKING', line_ids['LINE-PK-A'], 'RUNNING', '2026-02-28 09:05', None, 'Packing urgent order'),
    ]
    db.executemany(
        '''INSERT INTO machines(machine_code, machine_name, section_name, process_name, line_id, status, last_activity_at, current_batch_id, current_note)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        machine_rows,
    )

    item_lookup = {
        (row[0], row[1]): row[2]
        for row in db.execute('SELECT customer_file_id, line_no, id FROM customer_file_items').fetchall()
    }
    db.executemany(
        '''INSERT INTO item_planner_updates(item_id, planner_status, remaining_hours, ready_at, note, updated_at)
           VALUES(?, ?, ?, ?, ?, ?)''',
        [
            (item_lookup[(file_ids['CF-0001'], 1)], 'IN_PRODUCTION', 18, '2026-03-01 10:00', 'Current urgent item already released to production.', '2026-02-28 09:15'),
            (item_lookup[(file_ids['CF-0001'], 2)], 'READY_FOR_PRODUCTION', 30, '2026-03-02 16:00', 'Planner reserved Mixing Line B next slot.', '2026-02-28 09:20'),
            (item_lookup[(file_ids['CF-0002'], 1)], 'IN_PRODUCTION', 12, '2026-03-01 06:00', 'Urgent Beta Components order.', '2026-02-28 09:25'),
            (item_lookup[(file_ids['CF-0002'], 2)], 'WAITING_PLANNING', 40, '2026-03-03 09:00', 'Need final planner confirmation.', '2026-02-28 09:30'),
        ]
    )
    db.executemany(
        '''INSERT INTO item_tracking_events(item_id, event_time, event_type, stage_name, status_label, source_ref, user_role, note)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
        [
            (item_lookup[(file_ids['CF-0001'], 1)], '2026-02-28 08:00', 'ORDER_LINE_CREATED', 'ORDER', 'WAITING_PLANNING', 'CF-0001-L1', 'MARKETING', 'Customer file line created.'),
            (item_lookup[(file_ids['CF-0001'], 1)], '2026-02-28 09:15', 'PLANNER_UPDATE', 'PLANNING', 'IN_PRODUCTION', 'CF-0001-L1', 'PLANNING', 'Planner promised ready by 2026-03-01 10:00.'),
            (item_lookup[(file_ids['CF-0001'], 2)], '2026-02-28 08:05', 'ORDER_LINE_CREATED', 'ORDER', 'WAITING_PLANNING', 'CF-0001-L2', 'MARKETING', 'Customer file line created.'),
            (item_lookup[(file_ids['CF-0001'], 2)], '2026-02-28 09:20', 'PLANNER_UPDATE', 'PLANNING', 'READY_FOR_PRODUCTION', 'CF-0001-L2', 'PLANNING', 'Reserved slot on Mixing Line B.'),
            (item_lookup[(file_ids['CF-0002'], 1)], '2026-02-28 08:10', 'ORDER_LINE_CREATED', 'ORDER', 'WAITING_PLANNING', 'CF-0002-L1', 'MARKETING', 'Customer file line created.'),
            (item_lookup[(file_ids['CF-0002'], 1)], '2026-02-28 09:25', 'PLANNER_UPDATE', 'PLANNING', 'IN_PRODUCTION', 'CF-0002-L1', 'PLANNING', 'Urgent line released.'),
            (item_lookup[(file_ids['CF-0002'], 2)], '2026-02-28 08:15', 'ORDER_LINE_CREATED', 'ORDER', 'WAITING_PLANNING', 'CF-0002-L2', 'MARKETING', 'Customer file line created.'),
            (item_lookup[(file_ids['CF-0002'], 2)], '2026-02-28 09:30', 'PLANNER_UPDATE', 'PLANNING', 'WAITING_PLANNING', 'CF-0002-L2', 'PLANNING', 'Waiting final planner slot confirmation.'),
        ]
    )
    db.execute(
        '''INSERT INTO jobs(job_no, customer_id, product_id, planned_date, planned_qty_kg, target_line_id, source_customer_file_id, source_customer_file_item_id, remarks)
           VALUES('JOB-0001', ?, ?, date('now'), 500, ?, ?, ?, 'Created from customer file CF-0001 line 1')''',
        (
            customer_ids['CUS-001'],
            product_ids['PRD-001'],
            line_ids['LINE-MX-A'],
            file_ids['CF-0001'],
            item_lookup[(file_ids['CF-0001'], 1)],
        )
    )
    db.execute(
        '''INSERT INTO jobs(job_no, customer_id, product_id, planned_date, planned_qty_kg, target_line_id, source_customer_file_id, source_customer_file_item_id, remarks)
           VALUES('JOB-0002', ?, ?, date('now'), 350, ?, ?, ?, 'Created from customer file CF-0002 line 1')''',
        (
            customer_ids['CUS-002'],
            product_ids['PRD-002'],
            line_ids['LINE-MX-B'],
            file_ids['CF-0002'],
            item_lookup[(file_ids['CF-0002'], 1)],
        )
    )
    db.execute(
        '''INSERT INTO batches(batch_no, job_id, recipe_code, planned_input_kg, current_process, status, barcode_text, assigned_line_id)
           VALUES('BAT-0001', 1, 'RCP-A1', 500, 'VULCANISING', 'OPEN', 'BAT-0001', ?)''',
        (line_ids['LINE-MX-A'],)
    )
    db.execute(
        '''INSERT INTO batches(batch_no, job_id, recipe_code, planned_input_kg, current_process, status, barcode_text, assigned_line_id)
           VALUES('BAT-0002', 2, 'RCP-B2', 350, 'CUTTING', 'OPEN', 'BAT-0002', ?)''',
        (line_ids['LINE-MX-B'],)
    )
    batch_ids = {row[0]: row[1] for row in db.execute('SELECT batch_no, id FROM batches').fetchall()}
    machine_ids = {row[0]: row[1] for row in db.execute('SELECT machine_code, id FROM machines').fetchall()}
    job_ids = {row[0]: row[1] for row in db.execute('SELECT job_no, id FROM jobs').fetchall()}
    db.execute('UPDATE customer_file_items SET planned_job_id = ? WHERE customer_file_id = ? AND line_no = 1', (job_ids['JOB-0001'], file_ids['CF-0001']))
    db.execute('UPDATE customer_file_items SET planned_job_id = ? WHERE customer_file_id = ? AND line_no = 1', (job_ids['JOB-0002'], file_ids['CF-0002']))

    db.executemany(
        '''INSERT INTO item_tracking_events(item_id, event_time, event_type, stage_name, status_label, source_ref, user_role, note)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
        [
            (item_lookup[(file_ids['CF-0001'], 1)], '2026-02-28 09:35', 'JOB_CREATED', 'PLANNING', 'JOB_CREATED', 'JOB-0001', 'PLANNING', 'Job created from customer file line.'),
            (item_lookup[(file_ids['CF-0002'], 1)], '2026-02-28 09:40', 'JOB_CREATED', 'PLANNING', 'JOB_CREATED', 'JOB-0002', 'PLANNING', 'Job created from customer file line.'),
            (item_lookup[(file_ids['CF-0001'], 1)], '2026-02-28 09:45', 'BATCH_CREATED', 'MIXING', 'READY_FOR_PRODUCTION', 'BAT-0001', 'PRODUCTION', 'Batch released to production.'),
            (item_lookup[(file_ids['CF-0002'], 1)], '2026-02-28 09:50', 'BATCH_CREATED', 'MIXING', 'READY_FOR_PRODUCTION', 'BAT-0002', 'PRODUCTION', 'Batch released to production.'),
        ]
    )

    db.execute('UPDATE machines SET current_batch_id = ? WHERE machine_code = ?', (batch_ids['BAT-0001'], 'EX-A-01'))
    db.execute('UPDATE machines SET current_batch_id = ? WHERE machine_code = ?', (batch_ids['BAT-0002'], 'CT-A-01'))
    db.execute('UPDATE machines SET current_batch_id = ? WHERE machine_code = ?', (batch_ids['BAT-0002'], 'PK-A-01'))
    db.execute('UPDATE machines SET current_batch_id = ? WHERE machine_code = ?', (batch_ids['BAT-0001'], 'MX-A-01'))

    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    process_logs = [
        (batch_ids['BAT-0001'], 'MIXING', now, 500, 496, 4, 'Rahman', machine_ids['MX-A-01'], 'Mixer A-01', line_ids['LINE-MX-A'], 'MOVE_NEXT', 'Initial mixing done'),
        (batch_ids['BAT-0001'], 'EXTRUDER', now, 496, 492, 4, 'Kumar', machine_ids['EX-A-01'], 'Extruder A-01', line_ids['LINE-EX-A'], 'MOVE_NEXT', 'Minor trim loss'),
        (batch_ids['BAT-0002'], 'MIXING', now, 350, 348, 2, 'Farid', machine_ids['MX-B-01'], 'Mixer B-01', line_ids['LINE-MX-B'], 'MOVE_NEXT', 'Setup lot'),
        (batch_ids['BAT-0002'], 'EXTRUDER', now, 348, 344, 4, 'Amin', machine_ids['EX-A-01'], 'Extruder A-01', line_ids['LINE-EX-A'], 'MOVE_NEXT', 'Extrusion completed'),
        (batch_ids['BAT-0002'], 'VULCANISING', now, 344, 340, 4, 'Lee', machine_ids['VC-A-01'], 'Vulcaniser A-01', line_ids['LINE-VC-A'], 'MOVE_NEXT', 'Small heat loss'),
    ]
    db.executemany(
        '''INSERT INTO process_logs(batch_id, process_name, scan_time, input_qty_kg, good_qty_kg, reject_qty_kg, operator_name, machine_id, machine_name, line_id, next_action, remarks)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        process_logs,
    )
    db.executemany(
        '''INSERT INTO item_tracking_events(item_id, event_time, event_type, stage_name, status_label, source_ref, user_role, note)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
        [
            (item_lookup[(file_ids['CF-0001'], 1)], now, 'PROCESS_SCAN', 'MIXING', 'IN_PRODUCTION', 'BAT-0001', 'PRODUCTION', 'Mixing scan recorded.'),
            (item_lookup[(file_ids['CF-0001'], 1)], now, 'PROCESS_SCAN', 'EXTRUDER', 'IN_PRODUCTION', 'BAT-0001', 'PRODUCTION', 'Extruder scan recorded.'),
            (item_lookup[(file_ids['CF-0002'], 1)], now, 'PROCESS_SCAN', 'MIXING', 'IN_PRODUCTION', 'BAT-0002', 'PRODUCTION', 'Mixing scan recorded.'),
            (item_lookup[(file_ids['CF-0002'], 1)], now, 'PROCESS_SCAN', 'EXTRUDER', 'IN_PRODUCTION', 'BAT-0002', 'PRODUCTION', 'Extruder scan recorded.'),
            (item_lookup[(file_ids['CF-0002'], 1)], now, 'PROCESS_SCAN', 'VULCANISING', 'IN_PRODUCTION', 'BAT-0002', 'PRODUCTION', 'Vulcanising scan recorded.'),
        ]
    )

    db.executemany(
        '''INSERT INTO ot_logs(section_name, line_id, machine_id, work_date, employees, ot_hours, remarks)
           VALUES(?, ?, ?, date('now'), ?, ?, ?)''',
        [
            ('MIXING', line_ids['LINE-MX-A'], machine_ids['MX-A-01'], 6, 2.5, 'Urgent customer plan'),
            ('PACKING', line_ids['LINE-PK-A'], machine_ids['PK-A-01'], 4, 1.5, 'Need finish urgent lot'),
        ]
    )

    db.execute(
        '''INSERT INTO breakdowns(machine_id, machine_name, section_name, start_time, end_time, technician_name, batch_id, reason, remarks, status)
           VALUES(?, 'Extruder A-02', 'EXTRUDER', datetime('now', '-2 hours'), NULL, 'Tan', ?, 'Temperature unstable', 'Waiting sensor replacement', 'OPEN')''',
        (machine_ids['EX-A-02'], batch_ids['BAT-0001'])
    )



def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip().replace('T', ' ')
    for fmt in ('%Y-%m-%d %H:%M', '%Y-%m-%d %H:%M:%S', '%Y-%m-%d'):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    return None


def record_item_event(item_id: int, event_type: str, status_label: str, stage_name: str | None = None, source_ref: str | None = None, note: str | None = None, user_role: str = 'SYSTEM', event_time: str | None = None) -> None:
    execute(
        '''INSERT INTO item_tracking_events(item_id, event_time, event_type, stage_name, status_label, source_ref, user_role, note)
           VALUES(?, ?, ?, ?, ?, ?, ?, ?)''',
        (item_id, event_time or datetime.now().strftime('%Y-%m-%d %H:%M'), event_type, stage_name, status_label, source_ref, user_role, note),
    )


def upsert_planner_update(item_id: int, planner_status: str, remaining_hours: float | None, ready_at: str | None, note: str | None) -> None:
    now = datetime.now().strftime('%Y-%m-%d %H:%M')
    current = query_one('SELECT id FROM item_planner_updates WHERE item_id = ?', (item_id,))
    if current:
        execute(
            '''UPDATE item_planner_updates
               SET planner_status = ?, remaining_hours = ?, ready_at = ?, note = ?, updated_at = ?
               WHERE item_id = ?''',
            (planner_status, remaining_hours, ready_at, note, now, item_id),
        )
    else:
        execute(
            '''INSERT INTO item_planner_updates(item_id, planner_status, remaining_hours, ready_at, note, updated_at)
               VALUES(?, ?, ?, ?, ?, ?)''',
            (item_id, planner_status, remaining_hours, ready_at, note, now),
        )
    execute('UPDATE customer_file_items SET status = ? WHERE id = ?', (planner_status, item_id))


def get_item_tracking_snapshot(item_id: int) -> dict[str, Any]:
    item = query_one(
        '''
        SELECT i.*, cf.file_no, cf.order_date, cf.due_date, cf.po_no,
               c.code AS customer_code, c.name AS customer_name,
               p.sku, p.name AS product_name,
               j.id AS job_id, j.job_no, j.status AS job_status, j.planned_date, j.planned_qty_kg,
               pl.code AS preferred_line_code, pl.name AS preferred_line_name
        FROM customer_file_items i
        JOIN customer_files cf ON i.customer_file_id = cf.id
        JOIN customers c ON cf.customer_id = c.id
        JOIN products p ON i.product_id = p.id
        LEFT JOIN jobs j ON i.planned_job_id = j.id
        LEFT JOIN production_lines pl ON i.target_line_id = pl.id
        WHERE i.id = ?
        ''',
        (item_id,),
    )
    if not item:
        return {}

    planner = query_one('SELECT * FROM item_planner_updates WHERE item_id = ?', (item_id,))
    batch = None
    latest_log = None
    if item['job_id']:
        batch = query_one(
            '''
            SELECT b.*
            FROM batches b
            WHERE b.job_id = ?
            ORDER BY b.id DESC
            LIMIT 1
            ''',
            (item['job_id'],),
        )
    if batch:
        latest_log = query_one(
            '''
            SELECT pl.*, COALESCE(m.machine_code, pl.machine_name) AS machine_label, l.name AS line_name
            FROM process_logs pl
            LEFT JOIN machines m ON pl.machine_id = m.id
            LEFT JOIN production_lines l ON pl.line_id = l.id
            WHERE pl.batch_id = ?
            ORDER BY datetime(pl.scan_time) DESC, pl.id DESC
            LIMIT 1
            ''',
            (batch['id'],),
        )

    latest_event = query_one(
        'SELECT * FROM item_tracking_events WHERE item_id = ? ORDER BY datetime(event_time) DESC, id DESC LIMIT 1',
        (item_id,),
    )

    current_status = (planner['planner_status'] if planner and planner['planner_status'] else item['status']) or 'WAITING_PLANNING'
    current_stage = 'ORDER RECEIVED'
    progress_pct = 5
    current_batch_no = batch['batch_no'] if batch else None
    current_process = batch['current_process'] if batch else None

    if item['job_id']:
        current_stage = 'JOB CREATED'
        progress_pct = 18
        current_status = 'JOB_CREATED' if current_status in {'WAITING_PLANNING', 'PENDING_JOB'} else current_status

    if batch:
        current_stage = batch['current_process'] or 'MIXING'
        current_status = 'IN_PRODUCTION' if batch['status'] == 'OPEN' else ('HOLD' if batch['status'] == 'HOLD' else 'COMPLETED')
        progress_pct = 30
    if latest_log:
        current_stage = latest_log['process_name']
        if latest_log['process_name'] in PROCESS_FLOW:
            progress_pct = min(100, int(((PROCESS_FLOW.index(latest_log['process_name']) + 1) / len(PROCESS_FLOW)) * 100))
        if latest_log['next_action'] == 'REJECTED':
            current_status = 'HOLD'
    if batch and batch['status'] == 'COMPLETED':
        current_status = 'COMPLETED'
        current_stage = 'STORE_RECEIVING'
        progress_pct = 100

    remaining_hours = planner['remaining_hours'] if planner else None
    ready_at = planner['ready_at'] if planner else None
    planner_note = planner['note'] if planner else None
    planner_updated_at = planner['updated_at'] if planner else None

    due_dt = _parse_dt(item['due_date'])
    ready_dt = _parse_dt(ready_at)
    risk = 'ON_TRACK'
    if due_dt and ready_dt and ready_dt > due_dt:
        risk = 'RISK'
    elif due_dt and ready_dt and ready_dt.date() == due_dt.date():
        risk = 'TIGHT'
    elif due_dt and not ready_dt and current_status not in {'COMPLETED'}:
        risk = 'CHECK'

    last_update = None
    for candidate in [latest_event['event_time'] if latest_event else None, latest_log['scan_time'] if latest_log else None, planner_updated_at, item['created_at']]:
        if candidate:
            last_update = candidate
            break

    return {
        'id': item['id'],
        'customer_file_id': item['customer_file_id'],
        'file_no': item['file_no'],
        'line_no': item['line_no'],
        'customer_code': item['customer_code'],
        'customer_name': item['customer_name'],
        'sku': item['sku'],
        'product_name': item['product_name'],
        'ordered_qty_kg': item['ordered_qty_kg'],
        'remarks': item['remarks'],
        'due_date': item['due_date'],
        'po_no': item['po_no'],
        'status': current_status,
        'current_stage': current_stage,
        'progress_pct': progress_pct,
        'job_id': item['job_id'],
        'job_no': item['job_no'],
        'job_status': item['job_status'],
        'batch_no': current_batch_no,
        'batch_status': batch['status'] if batch else None,
        'current_process': current_process,
        'latest_machine': latest_log['machine_label'] if latest_log else None,
        'latest_scan_time': latest_log['scan_time'] if latest_log else None,
        'remaining_hours': remaining_hours,
        'ready_at': ready_at,
        'planner_note': planner_note,
        'planner_updated_at': planner_updated_at,
        'last_update': last_update,
        'risk': risk,
        'preferred_line_code': item['preferred_line_code'],
        'preferred_line_name': item['preferred_line_name'],
    }


def get_item_tracker_rows() -> list[dict[str, Any]]:
    item_ids = [row['id'] for row in query_all('SELECT id FROM customer_file_items ORDER BY id DESC')]
    rows = [get_item_tracking_snapshot(item_id) for item_id in item_ids]
    return [row for row in rows if row]


def get_item_timeline(item_id: int) -> list[sqlite3.Row]:
    return query_all(
        '''
        SELECT *
        FROM item_tracking_events
        WHERE item_id = ?
        ORDER BY datetime(event_time) DESC, id DESC
        ''',
        (item_id,),
    )


def get_summary() -> dict[str, Any]:
    db = get_db()
    cards = {
        'customers': db.execute('SELECT COUNT(*) FROM customers').fetchone()[0],
        'customer_files_open': db.execute("SELECT COUNT(*) FROM customer_files WHERE status = 'OPEN'").fetchone()[0],
        'customer_file_lines_pending': db.execute("SELECT COUNT(*) FROM customer_file_items WHERE planned_job_id IS NULL").fetchone()[0],
        'jobs_open': db.execute("SELECT COUNT(*) FROM jobs WHERE status = 'OPEN'").fetchone()[0],
        'batches_open': db.execute("SELECT COUNT(*) FROM batches WHERE status = 'OPEN'").fetchone()[0],
        'running_machines': db.execute("SELECT COUNT(*) FROM machines WHERE status = 'RUNNING' AND is_active = 1").fetchone()[0],
        'breakdown_machines': db.execute("SELECT COUNT(*) FROM machines WHERE status = 'BREAKDOWN' AND is_active = 1").fetchone()[0],
        'ot_hours': round(db.execute('SELECT COALESCE(SUM(ot_hours),0) FROM ot_logs').fetchone()[0] or 0, 2),
        'material_value': round(db.execute('SELECT COALESCE(SUM(cost_per_kg * stock_qty),0) FROM materials').fetchone()[0] or 0, 2),
    }
    return cards


def get_batch_variance_rows() -> list[dict[str, Any]]:
    rows = query_all(
        '''
        SELECT b.id, b.batch_no, b.planned_input_kg, b.current_process, b.status,
               j.job_no, c.name AS customer_name, p.name AS product_name,
               pl.name AS line_name
        FROM batches b
        JOIN jobs j ON b.job_id = j.id
        JOIN customers c ON j.customer_id = c.id
        JOIN products p ON j.product_id = p.id
        LEFT JOIN production_lines pl ON b.assigned_line_id = pl.id
        ORDER BY b.id DESC
        '''
    )
    output = []
    for row in rows:
        logs = query_all(
            '''SELECT pl.*, m.machine_code
               FROM process_logs pl
               LEFT JOIN machines m ON pl.machine_id = m.id
               WHERE pl.batch_id = ?
               ORDER BY datetime(pl.scan_time), pl.id''',
            (row['id'],)
        )
        first_input = row['planned_input_kg']
        latest_good = None
        total_reject = 0.0
        process_count = len(logs)
        probable_loss_process = None
        max_loss = -1.0
        if logs:
            first_input = logs[0]['input_qty_kg']
            latest_good = logs[-1]['good_qty_kg']
            total_reject = sum((log['reject_qty_kg'] or 0) for log in logs)
            for log in logs:
                loss = (log['input_qty_kg'] or 0) - (log['good_qty_kg'] or 0)
                if loss > max_loss:
                    max_loss = loss
                    probable_loss_process = log['process_name']
        good_output = latest_good if latest_good is not None else 0.0
        variance = round(first_input - good_output, 2) if latest_good is not None else None
        variance_pct = round((variance / first_input * 100), 2) if latest_good is not None and first_input else None
        output.append({
            'batch_no': row['batch_no'],
            'job_no': row['job_no'],
            'customer_name': row['customer_name'],
            'product_name': row['product_name'],
            'line_name': row['line_name'],
            'current_process': row['current_process'],
            'status': row['status'],
            'input_qty_kg': first_input,
            'good_output_kg': good_output,
            'total_reject_kg': round(total_reject, 2),
            'variance_kg': variance,
            'variance_pct': variance_pct,
            'process_count': process_count,
            'probable_loss_process': probable_loss_process,
        })
    return output


def get_machine_board_rows() -> list[sqlite3.Row]:
    return query_all(
        '''
        SELECT m.*, pl.code AS line_code, pl.name AS line_name,
               b.batch_no,
               j.job_no,
               c.name AS customer_name,
               p.name AS product_name,
               (
                   SELECT scan_time
                   FROM process_logs x
                   WHERE x.machine_id = m.id
                   ORDER BY datetime(x.scan_time) DESC, x.id DESC
                   LIMIT 1
               ) AS latest_scan_time
        FROM machines m
        LEFT JOIN production_lines pl ON m.line_id = pl.id
        LEFT JOIN batches b ON m.current_batch_id = b.id
        LEFT JOIN jobs j ON b.job_id = j.id
        LEFT JOIN customers c ON j.customer_id = c.id
        LEFT JOIN products p ON j.product_id = p.id
        WHERE m.is_active = 1
        ORDER BY m.section_name, COALESCE(pl.name, ''), m.machine_code
        '''
    )


def get_machine_choices_for_process(process_name: str | None = None) -> list[sqlite3.Row]:
    if process_name:
        return query_all(
            '''SELECT m.*, pl.name AS line_name
               FROM machines m
               LEFT JOIN production_lines pl ON m.line_id = pl.id
               WHERE m.is_active = 1 AND m.process_name = ?
               ORDER BY m.machine_code''',
            (process_name,)
        )
    return query_all(
        '''SELECT m.*, pl.name AS line_name
           FROM machines m
           LEFT JOIN production_lines pl ON m.line_id = pl.id
           WHERE m.is_active = 1
           ORDER BY m.process_name, m.machine_code'''
    )


def build_next_process_barcode(batch_no: str, from_process: str, to_process: str) -> str:
    stamp = datetime.now().strftime('%Y%m%d%H%M%S')
    return f"NP-{batch_no}-{from_process[:3]}-{to_process[:3]}-{stamp}"


def create_next_process_label(batch_id: int, from_process: str, to_process: str, issued_qty_kg: float, machine_id: int | None, operator_name: str | None, notes: str | None = None) -> str | None:
    batch_info = query_one(
        """
        SELECT b.batch_no, b.recipe_code, j.job_no, c.name AS customer_name, p.name AS item_name
        FROM batches b
        JOIN jobs j ON b.job_id = j.id
        JOIN customers c ON j.customer_id = c.id
        JOIN products p ON j.product_id = p.id
        WHERE b.id = ?
        """,
        (batch_id,),
    )
    if not batch_info:
        return None

    barcode = build_next_process_barcode(batch_info['batch_no'], from_process, to_process)
    try:
        execute(
            """INSERT INTO next_process_labels(
                   transfer_barcode, batch_id, from_process, to_process, issued_qty_kg, recipe_code, item_name, document_no, customer_name,
                   issued_at, issued_machine_id, issued_by, notes
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                barcode,
                batch_id,
                from_process,
                to_process,
                issued_qty_kg,
                batch_info['recipe_code'],
                batch_info['item_name'],
                batch_info['job_no'],
                batch_info['customer_name'],
                datetime.now().strftime('%Y-%m-%d %H:%M'),
                machine_id,
                operator_name,
                notes,
            ),
        )
    except sqlite3.IntegrityError:
        barcode = f"{barcode}-{datetime.now().strftime('%f')}"
        execute(
            """INSERT INTO next_process_labels(
                   transfer_barcode, batch_id, from_process, to_process, issued_qty_kg, recipe_code, item_name, document_no, customer_name,
                   issued_at, issued_machine_id, issued_by, notes
               ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                barcode,
                batch_id,
                from_process,
                to_process,
                issued_qty_kg,
                batch_info['recipe_code'],
                batch_info['item_name'],
                batch_info['job_no'],
                batch_info['customer_name'],
                datetime.now().strftime('%Y-%m-%d %H:%M'),
                machine_id,
                operator_name,
                notes,
            ),
        )
    return barcode


def receive_next_process_label(transfer_barcode: str, receiving_process: str, received_qty_kg: float, machine_id: int | None, received_by: str | None, notes: str | None = None) -> tuple[bool, str]:
    label = query_one('SELECT * FROM next_process_labels WHERE transfer_barcode = ?', (transfer_barcode,))
    if not label:
        return False, 'Next process barcode not found.'
    if label['status'] != 'ISSUED':
        return False, 'This next process barcode was already received.'
    if receiving_process != label['to_process']:
        return False, f"Barcode is for {label['to_process']}, not {receiving_process}."

    qty_loss_kg = max(0, round((label['issued_qty_kg'] or 0) - received_qty_kg, 2))
    execute(
        """UPDATE next_process_labels
           SET received_qty_kg = ?, qty_loss_kg = ?, status = 'RECEIVED', received_at = ?, received_machine_id = ?, received_by = ?, notes = ?
           WHERE id = ?""",
        (
            received_qty_kg,
            qty_loss_kg,
            datetime.now().strftime('%Y-%m-%d %H:%M'),
            machine_id,
            received_by,
            notes or label['notes'],
            label['id'],
        ),
    )
    execute('UPDATE batches SET current_process = ?, status = ? WHERE id = ?', (receiving_process, 'OPEN', label['batch_id']))
    return True, f"Received {received_qty_kg:.2f} KG (loss {qty_loss_kg:.2f} KG)."


def get_section_wip_summary() -> list[dict[str, Any]]:
    rows = []
    for section in PROCESS_FLOW:
        received = query_one(
            "SELECT COALESCE(SUM(received_qty_kg), 0) AS qty FROM next_process_labels WHERE to_process = ? AND status = 'RECEIVED'",
            (section,),
        )['qty']
        issued = query_one(
            "SELECT COALESCE(SUM(issued_qty_kg), 0) AS qty FROM next_process_labels WHERE from_process = ?",
            (section,),
        )['qty']
        in_section = round((received or 0) - (issued or 0), 2)
        rows.append({'section': section, 'received_kg': round(received or 0, 2), 'issued_kg': round(issued or 0, 2), 'in_section_kg': in_section})
    return rows

def update_batch_stage(batch_id: int, process_name: str, next_action: str) -> None:
    if next_action == 'REJECTED':
        execute(
            'UPDATE batches SET current_process = ?, status = ? WHERE id = ?',
            (process_name, 'HOLD', batch_id)
        )
        return
    next_stage = 'COMPLETED'
    if process_name in PROCESS_FLOW:
        idx = PROCESS_FLOW.index(process_name)
        if idx < len(PROCESS_FLOW) - 1:
            next_stage = PROCESS_FLOW[idx + 1]
    status = 'COMPLETED' if next_stage == 'COMPLETED' else 'OPEN'
    execute(
        'UPDATE batches SET current_process = ?, status = ? WHERE id = ?',
        (next_stage, status, batch_id)
    )


def update_machine_status(machine_id: int, status: str, batch_id: int | None = None, note: str | None = None) -> None:
    execute(
        '''UPDATE machines
           SET status = ?, current_batch_id = ?, current_note = ?, last_activity_at = ?
           WHERE id = ?''',
        (status, batch_id, note, datetime.now().strftime('%Y-%m-%d %H:%M'), machine_id)
    )


@app.route('/')
def dashboard():
    summary = get_summary()
    recent_logs = query_all(
        '''
        SELECT pl.scan_time, pl.process_name, b.batch_no, pl.input_qty_kg, pl.good_qty_kg, pl.reject_qty_kg,
               COALESCE(m.machine_code, pl.machine_name) AS machine_label
        FROM process_logs pl
        JOIN batches b ON pl.batch_id = b.id
        LEFT JOIN machines m ON pl.machine_id = m.id
        ORDER BY datetime(pl.scan_time) DESC, pl.id DESC
        LIMIT 12
        '''
    )
    recent_breakdowns = query_all(
        '''
        SELECT d.*, b.batch_no
        FROM breakdowns d
        LEFT JOIN batches b ON d.batch_id = b.id
        ORDER BY datetime(d.start_time) DESC, d.id DESC
        LIMIT 6
        '''
    )
    variance_rows = get_batch_variance_rows()[:6]
    running_machines = query_all(
        '''
        SELECT m.machine_code, m.machine_name, m.section_name, pl.name AS line_name, b.batch_no, m.current_note
        FROM machines m
        LEFT JOIN production_lines pl ON m.line_id = pl.id
        LEFT JOIN batches b ON m.current_batch_id = b.id
        WHERE m.status = 'RUNNING' AND m.is_active = 1
        ORDER BY m.section_name, m.machine_code
        LIMIT 10
        '''
    )
    section_wip = get_section_wip_summary()
    return render_template(
        'dashboard.html',
        summary=summary,
        recent_logs=recent_logs,
        recent_breakdowns=recent_breakdowns,
        variance_rows=variance_rows,
        running_machines=running_machines,
        section_wip=section_wip,
    )

@app.route('/customers', methods=['GET', 'POST'])
def customers():
    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        name = request.form.get('name', '').strip()
        if code and name:
            try:
                execute('INSERT INTO customers(code, name) VALUES(?, ?)', (code, name))
                flash('Customer added.', 'success')
            except sqlite3.IntegrityError:
                flash('Customer code already exists.', 'danger')
        else:
            flash('Customer code and name are required.', 'warning')
        return redirect(url_for('customers'))
    rows = query_all('SELECT * FROM customers ORDER BY id DESC')
    return render_template('customers.html', rows=rows)


@app.route('/products', methods=['GET', 'POST'])
def products():
    if request.method == 'POST':
        sku = request.form.get('sku', '').strip()
        name = request.form.get('name', '').strip()
        unit = request.form.get('unit', 'KG').strip() or 'KG'
        if sku and name:
            try:
                execute('INSERT INTO products(sku, name, unit) VALUES(?, ?, ?)', (sku, name, unit))
                flash('Product added.', 'success')
            except sqlite3.IntegrityError:
                flash('Product SKU already exists.', 'danger')
        else:
            flash('Product SKU and name are required.', 'warning')
        return redirect(url_for('products'))
    rows = query_all('SELECT * FROM products ORDER BY id DESC')
    return render_template('products.html', rows=rows)


@app.route('/materials', methods=['GET', 'POST'])
def materials():
    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        name = request.form.get('name', '').strip()
        uom = request.form.get('uom', 'KG').strip() or 'KG'
        try:
            cost_per_kg = float(request.form.get('cost_per_kg', 0) or 0)
            stock_qty = float(request.form.get('stock_qty', 0) or 0)
        except ValueError:
            flash('Cost and stock must be numeric.', 'danger')
            return redirect(url_for('materials'))
        if code and name:
            try:
                execute(
                    'INSERT INTO materials(code, name, uom, cost_per_kg, stock_qty) VALUES(?, ?, ?, ?, ?)',
                    (code, name, uom, cost_per_kg, stock_qty),
                )
                flash('Material added.', 'success')
            except sqlite3.IntegrityError:
                flash('Material code already exists.', 'danger')
        else:
            flash('Material code and name are required.', 'warning')
        return redirect(url_for('materials'))
    rows = query_all('SELECT * FROM materials ORDER BY id DESC')
    total_value = round(sum((row['cost_per_kg'] or 0) * (row['stock_qty'] or 0) for row in rows), 2)
    return render_template('materials.html', rows=rows, total_value=total_value)


@app.route('/lines', methods=['GET', 'POST'])
def lines():
    if request.method == 'POST':
        code = request.form.get('code', '').strip()
        name = request.form.get('name', '').strip()
        section_name = request.form.get('section_name', '').strip().upper()
        if code and name and section_name:
            try:
                execute(
                    'INSERT INTO production_lines(code, name, section_name) VALUES(?, ?, ?)',
                    (code, name, section_name),
                )
                flash('Production line added.', 'success')
            except sqlite3.IntegrityError:
                flash('Line code already exists.', 'danger')
        else:
            flash('Code, name and section are required.', 'warning')
        return redirect(url_for('lines'))
    rows = query_all('SELECT * FROM production_lines ORDER BY section_name, code')
    return render_template('lines.html', rows=rows, sections=SECTIONS)


@app.route('/machines', methods=['GET', 'POST'])
def machines():
    line_rows = query_all('SELECT id, code, name, section_name FROM production_lines WHERE is_active = 1 ORDER BY section_name, code')
    if request.method == 'POST':
        machine_code = request.form.get('machine_code', '').strip()
        machine_name = request.form.get('machine_name', '').strip()
        section_name = request.form.get('section_name', '').strip().upper()
        process_name = request.form.get('process_name', '').strip().upper()
        line_id = request.form.get('line_id', '').strip() or None
        note = request.form.get('current_note', '').strip()
        if machine_code and machine_name and section_name and process_name:
            try:
                execute(
                    '''INSERT INTO machines(machine_code, machine_name, section_name, process_name, line_id, status, current_note, last_activity_at)
                       VALUES(?, ?, ?, ?, ?, 'IDLE', ?, ?)''',
                    (machine_code, machine_name, section_name, process_name, int(line_id) if line_id else None, note, datetime.now().strftime('%Y-%m-%d %H:%M')),
                )
                flash('Machine added.', 'success')
            except sqlite3.IntegrityError:
                flash('Machine code already exists.', 'danger')
        else:
            flash('Machine code, machine name, section and process are required.', 'warning')
        return redirect(url_for('machines'))

    rows = get_machine_board_rows()
    return render_template('machines.html', rows=rows, lines=line_rows, sections=SECTIONS, processes=PROCESS_FLOW, statuses=MACHINE_STATUSES)


@app.post('/machines/<int:machine_id>/status')
def machine_status_update(machine_id: int):
    status = request.form.get('status', '').strip().upper()
    note = request.form.get('note', '').strip()
    batch_id_raw = request.form.get('batch_id', '').strip() or None
    if status not in MACHINE_STATUSES:
        flash('Invalid machine status.', 'danger')
        return redirect(url_for('machines'))
    update_machine_status(machine_id, status, int(batch_id_raw) if batch_id_raw else None, note)
    flash('Machine status updated.', 'success')
    return redirect(url_for('machines'))


@app.route('/customer-files', methods=['GET', 'POST'])
def customer_files():
    customers_list = query_all('SELECT id, code, name FROM customers ORDER BY name')
    if request.method == 'POST':
        file_no = request.form.get('file_no', '').strip()
        customer_id = request.form.get('customer_id', '').strip()
        order_date = request.form.get('order_date', '').strip()
        due_date = request.form.get('due_date', '').strip() or None
        po_no = request.form.get('po_no', '').strip()
        remarks = request.form.get('remarks', '').strip()
        if file_no and customer_id and order_date:
            try:
                execute(
                    '''INSERT INTO customer_files(file_no, customer_id, order_date, due_date, po_no, remarks)
                       VALUES(?, ?, ?, ?, ?, ?)''',
                    (file_no, int(customer_id), order_date, due_date, po_no, remarks),
                )
                flash('Customer file created.', 'success')
            except sqlite3.IntegrityError:
                flash('Customer file number already exists.', 'danger')
        else:
            flash('Please complete required customer file fields.', 'warning')
        return redirect(url_for('customer_files'))

    rows = query_all(
        '''
        SELECT cf.*, c.code AS customer_code, c.name AS customer_name,
               COUNT(i.id) AS item_count,
               ROUND(COALESCE(SUM(i.ordered_qty_kg), 0), 2) AS total_qty_kg,
               SUM(CASE WHEN i.planned_job_id IS NULL THEN 1 ELSE 0 END) AS pending_job_lines
        FROM customer_files cf
        JOIN customers c ON cf.customer_id = c.id
        LEFT JOIN customer_file_items i ON i.customer_file_id = cf.id
        GROUP BY cf.id, cf.file_no, cf.customer_id, cf.order_date, cf.due_date, cf.po_no, cf.remarks, cf.status, cf.created_at, c.code, c.name
        ORDER BY cf.id DESC
        '''
    )
    return render_template('customer_files.html', rows=rows, customers=customers_list)


@app.route('/customer-files/<int:file_id>', methods=['GET', 'POST'])
def customer_file_detail(file_id: int):
    file_row = query_one(
        '''
        SELECT cf.*, c.code AS customer_code, c.name AS customer_name
        FROM customer_files cf
        JOIN customers c ON cf.customer_id = c.id
        WHERE cf.id = ?
        ''',
        (file_id,)
    )
    if not file_row:
        flash('Customer file not found.', 'danger')
        return redirect(url_for('customer_files'))

    products_list = query_all('SELECT id, sku, name FROM products ORDER BY name')
    lines_list = query_all('SELECT id, code, name FROM production_lines WHERE is_active = 1 ORDER BY section_name, code')

    if request.method == 'POST':
        product_id = request.form.get('product_id', '').strip()
        target_line_id = request.form.get('target_line_id', '').strip() or None
        remarks = request.form.get('remarks', '').strip()
        try:
            ordered_qty_kg = float(request.form.get('ordered_qty_kg', 0) or 0)
        except ValueError:
            flash('Ordered quantity must be numeric.', 'danger')
            return redirect(url_for('customer_file_detail', file_id=file_id))

        if product_id and ordered_qty_kg > 0:
            next_line_no = (query_one('SELECT COALESCE(MAX(line_no), 0) AS max_line FROM customer_file_items WHERE customer_file_id = ?', (file_id,))['max_line'] or 0) + 1
            new_item_id = execute(
                '''INSERT INTO customer_file_items(customer_file_id, line_no, product_id, ordered_qty_kg, target_line_id, remarks)
                   VALUES(?, ?, ?, ?, ?, ?)''',
                (file_id, next_line_no, int(product_id), ordered_qty_kg, int(target_line_id) if target_line_id else None, remarks),
            )
            record_item_event(
                new_item_id,
                'ORDER_LINE_CREATED',
                'WAITING_PLANNING',
                'ORDER',
                f'{file_row["file_no"]}-L{next_line_no}',
                remarks or 'Customer line created.',
                'MARKETING',
                file_row['order_date'],
            )
            flash('Item line added to customer file.', 'success')
        else:
            flash('Please complete required item fields.', 'warning')
        return redirect(url_for('customer_file_detail', file_id=file_id))

    raw_items = query_all(
        '''
        SELECT i.*, p.sku, p.name AS product_name, pl.code AS line_code, pl.name AS line_name,
               j.job_no
        FROM customer_file_items i
        JOIN products p ON i.product_id = p.id
        LEFT JOIN production_lines pl ON i.target_line_id = pl.id
        LEFT JOIN jobs j ON i.planned_job_id = j.id
        WHERE i.customer_file_id = ?
        ORDER BY i.line_no
        ''',
        (file_id,)
    )
    items = []
    for row in raw_items:
        merged = dict(row)
        tracking = get_item_tracking_snapshot(row['id'])
        merged.update({
            'tracking_status': tracking.get('status'),
            'tracking_stage': tracking.get('current_stage'),
            'tracking_ready_at': tracking.get('ready_at'),
            'tracking_remaining_hours': tracking.get('remaining_hours'),
            'tracking_risk': tracking.get('risk'),
        })
        items.append(merged)
    total_qty = round(sum((row['ordered_qty_kg'] or 0) for row in items), 2)
    return render_template('customer_file_detail.html', file_row=file_row, items=items, products=products_list, lines=lines_list, total_qty=total_qty)


@app.route('/item-tracker')
def item_tracker():
    rows = get_item_tracker_rows()
    summary = {
        'total': len(rows),
        'in_production': sum(1 for row in rows if row.get('status') == 'IN_PRODUCTION'),
        'waiting_planning': sum(1 for row in rows if row.get('status') == 'WAITING_PLANNING'),
        'ready': sum(1 for row in rows if row.get('status') == 'READY_FOR_PRODUCTION'),
        'risk': sum(1 for row in rows if row.get('risk') == 'RISK'),
    }
    return render_template('item_tracker.html', rows=rows, summary=summary)


@app.route('/item-tracker/<int:item_id>', methods=['GET', 'POST'])
def item_tracker_detail(item_id: int):
    snapshot = get_item_tracking_snapshot(item_id)
    if not snapshot:
        flash('Customer item not found.', 'danger')
        return redirect(url_for('item_tracker'))

    if request.method == 'POST':
        planner_status = request.form.get('planner_status', 'WAITING_PLANNING').strip().upper()
        ready_at = normalize_dt(request.form.get('ready_at', '').strip()) or None
        note = request.form.get('note', '').strip()
        remaining_raw = request.form.get('remaining_hours', '').strip()
        remaining_hours = None
        if remaining_raw:
            try:
                remaining_hours = float(remaining_raw)
            except ValueError:
                flash('Remaining hours must be numeric.', 'danger')
                return redirect(url_for('item_tracker_detail', item_id=item_id))
        if planner_status not in ITEM_STATUS_CHOICES:
            flash('Invalid planner status.', 'danger')
            return redirect(url_for('item_tracker_detail', item_id=item_id))

        upsert_planner_update(item_id, planner_status, remaining_hours, ready_at, note)
        detail_note = note or 'Planner updated current ETA.'
        if remaining_hours is not None:
            detail_note = f'{detail_note} Remaining hours: {remaining_hours}.'
        if ready_at:
            detail_note = f'{detail_note} Ready at: {ready_at}.'
        record_item_event(item_id, 'PLANNER_UPDATE', planner_status, 'PLANNING', snapshot.get('file_no'), detail_note, 'PLANNING')
        flash('Planner update saved.', 'success')
        return redirect(url_for('item_tracker_detail', item_id=item_id))

    snapshot = get_item_tracking_snapshot(item_id)
    timeline = get_item_timeline(item_id)
    process_rows = []
    if snapshot.get('batch_no'):
        process_rows = query_all(
            '''
            SELECT pl.*, COALESCE(m.machine_code, pl.machine_name) AS machine_label, l.name AS line_name
            FROM process_logs pl
            JOIN batches b ON pl.batch_id = b.id
            LEFT JOIN machines m ON pl.machine_id = m.id
            LEFT JOIN production_lines l ON pl.line_id = l.id
            WHERE b.batch_no = ?
            ORDER BY datetime(pl.scan_time) DESC, pl.id DESC
            ''',
            (snapshot['batch_no'],),
        )
    return render_template('item_tracker_detail.html', item=snapshot, timeline=timeline, process_rows=process_rows, status_choices=ITEM_STATUS_CHOICES)



@app.route('/jobs', methods=['GET', 'POST'])
def jobs():
    customers_list = query_all('SELECT id, code, name FROM customers ORDER BY name')
    products_list = query_all('SELECT id, sku, name FROM products ORDER BY name')
    lines_list = query_all('SELECT id, code, name FROM production_lines WHERE is_active = 1 ORDER BY section_name, code')
    customer_file_items = query_all(
        '''
        SELECT i.id, i.line_no, i.ordered_qty_kg, i.remarks AS line_remarks,
               cf.file_no, cf.order_date, cf.due_date,
               c.id AS customer_id, c.name AS customer_name,
               p.id AS product_id, p.name AS product_name, p.sku,
               pl.id AS target_line_id, pl.name AS line_name
        FROM customer_file_items i
        JOIN customer_files cf ON i.customer_file_id = cf.id
        JOIN customers c ON cf.customer_id = c.id
        JOIN products p ON i.product_id = p.id
        LEFT JOIN production_lines pl ON i.target_line_id = pl.id
        WHERE i.planned_job_id IS NULL
        ORDER BY cf.id DESC, i.line_no
        '''
    )
    if request.method == 'POST':
        job_no = request.form.get('job_no', '').strip()
        customer_file_item_id = request.form.get('customer_file_item_id', '').strip() or None
        remarks = request.form.get('remarks', '').strip()

        if not job_no:
            flash('Job number is required.', 'warning')
            return redirect(url_for('jobs'))

        if customer_file_item_id:
            source_item = query_one(
                '''
                SELECT i.*, cf.file_no, cf.customer_id, cf.order_date, cf.due_date
                FROM customer_file_items i
                JOIN customer_files cf ON i.customer_file_id = cf.id
                WHERE i.id = ? AND i.planned_job_id IS NULL
                ''',
                (int(customer_file_item_id),)
            )
            if not source_item:
                flash('Selected customer file line is not available.', 'danger')
                return redirect(url_for('jobs'))
            new_job_id = execute(
                '''INSERT INTO jobs(job_no, customer_id, product_id, planned_date, planned_qty_kg, target_line_id, source_customer_file_id, source_customer_file_item_id, remarks)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (
                    job_no,
                    source_item['customer_id'],
                    source_item['product_id'],
                    source_item['due_date'] or source_item['order_date'],
                    source_item['ordered_qty_kg'],
                    source_item['target_line_id'],
                    source_item['customer_file_id'],
                    source_item['id'],
                    remarks or f"Created from customer file {source_item['file_no']} line {source_item['line_no']}",
                ),
            )
            execute(
                "UPDATE customer_file_items SET planned_job_id = ?, status = 'JOB_CREATED' WHERE id = ?",
                (new_job_id, source_item['id'])
            )
            record_item_event(
                source_item['id'],
                'JOB_CREATED',
                'JOB_CREATED',
                'PLANNING',
                job_no,
                remarks or f"Job {job_no} created from {source_item['file_no']} line {source_item['line_no']}.",
                'PLANNING',
            )
            flash('Job created from customer file line.', 'success')
            return redirect(url_for('jobs'))

        customer_id = request.form.get('customer_id', '').strip()
        product_id = request.form.get('product_id', '').strip()
        planned_date = request.form.get('planned_date', '').strip()
        target_line_id = request.form.get('target_line_id', '').strip() or None
        try:
            planned_qty_kg = float(request.form.get('planned_qty_kg', 0) or 0)
        except ValueError:
            flash('Planned qty must be numeric.', 'danger')
            return redirect(url_for('jobs'))

        if all([job_no, customer_id, product_id, planned_date]) and planned_qty_kg > 0:
            try:
                execute(
                    '''INSERT INTO jobs(job_no, customer_id, product_id, planned_date, planned_qty_kg, target_line_id, remarks)
                       VALUES(?, ?, ?, ?, ?, ?, ?)''',
                    (job_no, int(customer_id), int(product_id), planned_date, planned_qty_kg, int(target_line_id) if target_line_id else None, remarks),
                )
                flash('Job created.', 'success')
            except sqlite3.IntegrityError:
                flash('Job number already exists.', 'danger')
        else:
            flash('Please complete all required job fields, or select one customer file line.', 'warning')
        return redirect(url_for('jobs'))
    rows = query_all(
        '''
        SELECT j.*, c.name AS customer_name, p.name AS product_name, pl.name AS line_name,
               cf.file_no, cfi.line_no
        FROM jobs j
        JOIN customers c ON j.customer_id = c.id
        JOIN products p ON j.product_id = p.id
        LEFT JOIN production_lines pl ON j.target_line_id = pl.id
        LEFT JOIN customer_files cf ON j.source_customer_file_id = cf.id
        LEFT JOIN customer_file_items cfi ON j.source_customer_file_item_id = cfi.id
        ORDER BY j.id DESC
        '''
    )
    return render_template('jobs.html', rows=rows, customers=customers_list, products=products_list, lines=lines_list, customer_file_items=customer_file_items)


@app.route('/batches', methods=['GET', 'POST'])
def batches():
    jobs_list = query_all(
        '''SELECT j.id, j.job_no, c.name AS customer_name, p.name AS product_name, j.planned_qty_kg, pl.name AS line_name
           FROM jobs j
           JOIN customers c ON j.customer_id = c.id
           JOIN products p ON j.product_id = p.id
           LEFT JOIN production_lines pl ON j.target_line_id = pl.id
           ORDER BY j.id DESC'''
    )
    lines_list = query_all('SELECT id, code, name FROM production_lines WHERE is_active = 1 ORDER BY section_name, code')
    if request.method == 'POST':
        batch_no = request.form.get('batch_no', '').strip()
        job_id = request.form.get('job_id', '').strip()
        recipe_code = request.form.get('recipe_code', '').strip()
        barcode_text = request.form.get('barcode_text', '').strip() or batch_no
        assigned_line_id = request.form.get('assigned_line_id', '').strip() or None
        try:
            planned_input_kg = float(request.form.get('planned_input_kg', 0) or 0)
        except ValueError:
            flash('Planned input kg must be numeric.', 'danger')
            return redirect(url_for('batches'))
        if batch_no and job_id and planned_input_kg > 0:
            try:
                new_batch_id = execute(
                    '''INSERT INTO batches(batch_no, job_id, recipe_code, planned_input_kg, current_process, status, barcode_text, assigned_line_id)
                       VALUES(?, ?, ?, ?, 'MIXING', 'OPEN', ?, ?)''',
                    (batch_no, int(job_id), recipe_code, planned_input_kg, barcode_text, int(assigned_line_id) if assigned_line_id else None),
                )
                source_job = query_one('SELECT source_customer_file_item_id FROM jobs WHERE id = ?', (int(job_id),))
                if source_job and source_job['source_customer_file_item_id']:
                    item_id = source_job['source_customer_file_item_id']
                    execute("UPDATE customer_file_items SET status = 'READY_FOR_PRODUCTION' WHERE id = ?", (item_id,))
                    record_item_event(
                        item_id,
                        'BATCH_CREATED',
                        'READY_FOR_PRODUCTION',
                        'MIXING',
                        batch_no,
                        f'Batch {batch_no} released to production.',
                        'PRODUCTION',
                    )
                flash('Batch created.', 'success')
            except sqlite3.IntegrityError:
                flash('Batch number already exists.', 'danger')
        else:
            flash('Please complete all required batch fields.', 'warning')
        return redirect(url_for('batches'))
    rows = query_all(
        '''
        SELECT b.*, j.job_no, c.name AS customer_name, p.name AS product_name, pl.name AS line_name
        FROM batches b
        JOIN jobs j ON b.job_id = j.id
        JOIN customers c ON j.customer_id = c.id
        JOIN products p ON j.product_id = p.id
        LEFT JOIN production_lines pl ON b.assigned_line_id = pl.id
        ORDER BY b.id DESC
        '''
    )
    return render_template('batches.html', rows=rows, jobs=jobs_list, lines=lines_list)


@app.route('/scan', methods=['GET', 'POST'])
def scan():
    batch_lookup = None
    barcode = request.args.get('barcode', '').strip()
    if request.method == 'POST':
        action_type = request.form.get('action_type', 'process_scan').strip()

        if action_type == 'receive_next':
            transfer_barcode = request.form.get('transfer_barcode', '').strip()
            receiving_process = request.form.get('receiving_process', '').strip().upper()
            receiver_name = request.form.get('receiver_name', '').strip()
            receive_note = request.form.get('receive_note', '').strip()
            machine_id_raw = request.form.get('receive_machine_id', '').strip()
            try:
                received_qty_kg = float(request.form.get('received_qty_kg', 0) or 0)
            except ValueError:
                flash('Received quantity must be numeric.', 'danger')
                return redirect(url_for('scan'))
            if receiving_process not in PROCESS_FLOW:
                flash('Invalid receiving process.', 'danger')
                return redirect(url_for('scan'))
            if received_qty_kg <= 0:
                flash('Received quantity must be more than zero.', 'danger')
                return redirect(url_for('scan'))

            machine_id = int(machine_id_raw) if machine_id_raw else None
            ok, message = receive_next_process_label(transfer_barcode, receiving_process, received_qty_kg, machine_id, receiver_name, receive_note)
            flash(message, 'success' if ok else 'danger')
            return redirect(url_for('scan', barcode=transfer_barcode))

        barcode_text = request.form.get('barcode_text', '').strip()
        process_name = request.form.get('process_name', '').strip().upper()
        scan_time = normalize_dt(request.form.get('scan_time', '').strip()) or datetime.now().strftime('%Y-%m-%d %H:%M')
        operator_name = request.form.get('operator_name', '').strip()
        machine_id_raw = request.form.get('machine_id', '').strip()
        next_action = request.form.get('next_action', 'MOVE_NEXT').strip().upper()
        remarks = request.form.get('remarks', '').strip()
        try:
            input_qty_kg = float(request.form.get('input_qty_kg', 0) or 0)
            good_qty_kg = float(request.form.get('good_qty_kg', 0) or 0)
            reject_qty_kg = float(request.form.get('reject_qty_kg', 0) or 0)
        except ValueError:
            flash('Quantities must be numeric.', 'danger')
            return redirect(url_for('scan', barcode=barcode_text))

        batch = query_one('SELECT * FROM batches WHERE batch_no = ? OR barcode_text = ?', (barcode_text, barcode_text))
        if not batch:
            flash('Batch/barcode not found.', 'danger')
            return redirect(url_for('scan'))
        if process_name not in PROCESS_FLOW:
            flash('Invalid process name.', 'danger')
            return redirect(url_for('scan', barcode=barcode_text))
        if next_action not in {'MOVE_NEXT', 'REJECTED', 'REWORK'}:
            flash('Invalid next action.', 'danger')
            return redirect(url_for('scan', barcode=barcode_text))

        machine_row = None
        line_id = None
        machine_name = None
        if machine_id_raw:
            machine_row = query_one('SELECT * FROM machines WHERE id = ?', (int(machine_id_raw),))
            if not machine_row:
                flash('Selected machine not found.', 'danger')
                return redirect(url_for('scan', barcode=barcode_text))
            if machine_row['process_name'] != process_name:
                flash('Selected machine does not belong to that process.', 'danger')
                return redirect(url_for('scan', barcode=barcode_text))
            line_id = machine_row['line_id']
            machine_name = machine_row['machine_name']

        execute(
            '''INSERT INTO process_logs(batch_id, process_name, scan_time, input_qty_kg, good_qty_kg, reject_qty_kg, operator_name, machine_id, machine_name, line_id, next_action, remarks)
               VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (batch['id'], process_name, scan_time, input_qty_kg, good_qty_kg, reject_qty_kg, operator_name, int(machine_id_raw) if machine_id_raw else None, machine_name, line_id, next_action, remarks),
        )
        update_batch_stage(batch['id'], process_name, next_action)

        issued_transfer_barcode = None
        if next_action == 'MOVE_NEXT' and process_name in PROCESS_FLOW and process_name != PROCESS_FLOW[-1]:
            to_process = PROCESS_FLOW[PROCESS_FLOW.index(process_name) + 1]
            issued_transfer_barcode = create_next_process_label(
                batch['id'],
                process_name,
                to_process,
                good_qty_kg,
                int(machine_id_raw) if machine_id_raw else None,
                operator_name,
                remarks,
            )


        batch_after = query_one('SELECT * FROM batches WHERE id = ?', (batch['id'],))
        linked_item = query_one(
            '''
            SELECT j.source_customer_file_item_id AS item_id
            FROM jobs j
            JOIN batches b ON b.job_id = j.id
            WHERE b.id = ?
            ''',
            (batch['id'],),
        )
        if linked_item and linked_item['item_id']:
            item_id = linked_item['item_id']
            item_status = 'IN_PRODUCTION'
            if next_action == 'REJECTED' or (batch_after and batch_after['status'] == 'HOLD'):
                item_status = 'HOLD'
            elif batch_after and batch_after['status'] == 'COMPLETED':
                item_status = 'COMPLETED'
            execute('UPDATE customer_file_items SET status = ? WHERE id = ?', (item_status, item_id))
            note_parts = [f'Input {input_qty_kg} KG', f'Good {good_qty_kg} KG', f'Reject {reject_qty_kg} KG']
            if issued_transfer_barcode:
                note_parts.append(f'Next process barcode: {issued_transfer_barcode}')            
            
            if remarks:
                note_parts.append(remarks)
            record_item_event(
                item_id,
                'PROCESS_SCAN',
                item_status,
                process_name,
                batch['batch_no'],
                ' | '.join(note_parts),
                'PRODUCTION',
                scan_time,
            )

        if machine_row:
            status = 'RUNNING' if next_action in {'MOVE_NEXT', 'REWORK'} else 'IDLE'
            note = f'Last scan for {batch["batch_no"]} / {process_name}'
            update_machine_status(machine_row['id'], status, batch['id'] if status == 'RUNNING' else None, note)
        message = 'Process scan recorded.'
        if issued_transfer_barcode:
            message += f' Next process barcode issued: {issued_transfer_barcode}'
        flash(message, 'success')
        return redirect(url_for('scan', barcode=barcode_text))

    if barcode:
        batch_lookup = query_one(
            '''
            SELECT b.*, j.job_no, c.name AS customer_name, p.name AS product_name, pl.name AS line_name
            FROM batches b
            JOIN jobs j ON b.job_id = j.id
            JOIN customers c ON j.customer_id = c.id
            JOIN products p ON j.product_id = p.id
            LEFT JOIN production_lines pl ON b.assigned_line_id = pl.id
            WHERE b.batch_no = ? OR b.barcode_text = ?
            ''',
            (barcode, barcode),
        )
    recent_logs = query_all(
        '''
        SELECT pl.*, b.batch_no, COALESCE(m.machine_code, pl.machine_name) AS machine_label, l.name AS line_name
        FROM process_logs pl
        JOIN batches b ON pl.batch_id = b.id
        LEFT JOIN machines m ON pl.machine_id = m.id
        LEFT JOIN production_lines l ON pl.line_id = l.id
        ORDER BY datetime(pl.scan_time) DESC, pl.id DESC
        LIMIT 25
        '''
    )
    machines_list = get_machine_choices_for_process()
    next_labels_pending = query_all(
        '''
        SELECT npl.*, b.batch_no
        FROM next_process_labels npl
        JOIN batches b ON npl.batch_id = b.id
        WHERE npl.status = 'ISSUED'
        ORDER BY datetime(npl.issued_at) DESC, npl.id DESC
        LIMIT 20
        '''
    )
    recent_transfers = query_all(
        '''
        SELECT npl.*, b.batch_no
        FROM next_process_labels npl
        JOIN batches b ON npl.batch_id = b.id
        ORDER BY datetime(npl.issued_at) DESC, npl.id DESC
        LIMIT 25
        '''
    )
    return render_template(
        'scan.html',
        batch_lookup=batch_lookup,
        recent_logs=recent_logs,
        processes=PROCESS_FLOW,
        machines=machines_list,
        next_labels_pending=next_labels_pending,
        recent_transfers=recent_transfers,
    )


@app.route('/ot', methods=['GET', 'POST'])
def ot():
    line_rows = query_all('SELECT id, code, name FROM production_lines WHERE is_active = 1 ORDER BY section_name, code')
    machine_rows = query_all('SELECT id, machine_code, machine_name FROM machines WHERE is_active = 1 ORDER BY machine_code')
    if request.method == 'POST':
        section_name = request.form.get('section_name', '').strip().upper()
        line_id = request.form.get('line_id', '').strip() or None
        machine_id = request.form.get('machine_id', '').strip() or None
        work_date = request.form.get('work_date', '').strip()
        remarks = request.form.get('remarks', '').strip()
        try:
            employees = int(request.form.get('employees', 0) or 0)
            ot_hours = float(request.form.get('ot_hours', 0) or 0)
        except ValueError:
            flash('Employees and OT hours must be numeric.', 'danger')
            return redirect(url_for('ot'))
        if section_name and work_date and employees > 0:
            execute(
                'INSERT INTO ot_logs(section_name, line_id, machine_id, work_date, employees, ot_hours, remarks) VALUES(?, ?, ?, ?, ?, ?, ?)',
                (section_name, int(line_id) if line_id else None, int(machine_id) if machine_id else None, work_date, employees, ot_hours, remarks),
            )
            flash('OT record added.', 'success')
        else:
            flash('Please complete OT fields.', 'warning')
        return redirect(url_for('ot'))
    rows = query_all(
        '''
        SELECT o.*, l.name AS line_name, m.machine_code
        FROM ot_logs o
        LEFT JOIN production_lines l ON o.line_id = l.id
        LEFT JOIN machines m ON o.machine_id = m.id
        ORDER BY o.work_date DESC, o.id DESC
        '''
    )
    return render_template('ot.html', rows=rows, sections=SECTIONS, lines=line_rows, machines=machine_rows)


@app.route('/breakdowns', methods=['GET', 'POST'])
def breakdowns():
    machines_list = query_all(
        '''SELECT m.id, m.machine_code, m.machine_name, m.section_name, pl.name AS line_name
           FROM machines m
           LEFT JOIN production_lines pl ON m.line_id = pl.id
           WHERE m.is_active = 1
           ORDER BY m.section_name, m.machine_code'''
    )
    batches_list = query_all('SELECT id, batch_no FROM batches ORDER BY id DESC')
    if request.method == 'POST':
        machine_id = request.form.get('machine_id', '').strip()
        start_time = normalize_dt(request.form.get('start_time', '').strip())
        end_time = normalize_dt(request.form.get('end_time', '').strip()) or None
        technician_name = request.form.get('technician_name', '').strip()
        batch_id = request.form.get('batch_id', '').strip() or None
        reason = request.form.get('reason', '').strip()
        remarks = request.form.get('remarks', '').strip()
        machine = query_one('SELECT * FROM machines WHERE id = ?', (int(machine_id),)) if machine_id else None
        if machine and start_time and reason:
            status = 'CLOSED' if end_time else 'OPEN'
            execute(
                '''INSERT INTO breakdowns(machine_id, machine_name, section_name, start_time, end_time, technician_name, batch_id, reason, remarks, status)
                   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                (machine['id'], machine['machine_name'], machine['section_name'], start_time, end_time, technician_name, int(batch_id) if batch_id else None, reason, remarks, status),
            )
            update_machine_status(machine['id'], 'IDLE' if end_time else 'BREAKDOWN', None if end_time else machine['current_batch_id'], remarks or reason)
            flash('Breakdown record added.', 'success')
        else:
            flash('Please complete required breakdown fields.', 'warning')
        return redirect(url_for('breakdowns'))
    rows = query_all(
        '''
        SELECT d.*, b.batch_no, m.machine_code
        FROM breakdowns d
        LEFT JOIN batches b ON d.batch_id = b.id
        LEFT JOIN machines m ON d.machine_id = m.id
        ORDER BY datetime(d.start_time) DESC, d.id DESC
        '''
    )
    return render_template('breakdowns.html', rows=rows, machines=machines_list, batches=batches_list)


@app.route('/machine-board')
def machine_board():
    rows = get_machine_board_rows()
    grouped: dict[str, list[sqlite3.Row]] = {}
    for row in rows:
        grouped.setdefault(row['section_name'], []).append(row)
    return render_template('machine_board.html', grouped=grouped)


@app.route('/reports')
def reports():
    variance_rows = get_batch_variance_rows()
    process_stage_rows = query_all(
        '''
        SELECT process_name,
               COUNT(*) AS scans,
               ROUND(SUM(input_qty_kg), 2) AS total_input,
               ROUND(SUM(good_qty_kg), 2) AS total_good,
               ROUND(SUM(reject_qty_kg), 2) AS total_reject,
               ROUND(SUM(input_qty_kg - good_qty_kg), 2) AS total_variance
        FROM process_logs
        GROUP BY process_name
        ORDER BY CASE process_name
            WHEN 'MIXING' THEN 1
            WHEN 'EXTRUDER' THEN 2
            WHEN 'VULCANISING' THEN 3
            WHEN 'CUTTING' THEN 4
            WHEN 'FINISHING' THEN 5
            WHEN 'PACKING' THEN 6
            WHEN 'STORE_RECEIVING' THEN 7
            ELSE 99 END
        '''
    )
    ot_rows = query_all(
        '''
        SELECT o.section_name,
               COUNT(*) AS entries,
               SUM(o.employees) AS total_employees,
               ROUND(SUM(o.ot_hours), 2) AS total_ot_hours
        FROM ot_logs o
        GROUP BY o.section_name
        ORDER BY total_ot_hours DESC
        '''
    )
    machine_rows = query_all(
        '''
        SELECT m.machine_code, m.machine_name, m.section_name, m.status, pl.name AS line_name,
               COUNT(x.id) AS scans,
               ROUND(COALESCE(SUM(x.good_qty_kg), 0), 2) AS total_good_kg,
               ROUND(COALESCE(SUM(x.reject_qty_kg), 0), 2) AS total_reject_kg
        FROM machines m
        LEFT JOIN production_lines pl ON m.line_id = pl.id
        LEFT JOIN process_logs x ON x.machine_id = m.id
        WHERE m.is_active = 1
        GROUP BY m.id, m.machine_code, m.machine_name, m.section_name, m.status, pl.name
        ORDER BY m.section_name, m.machine_code
        '''
    )
    return render_template('reports.html', variance_rows=variance_rows, process_stage_rows=process_stage_rows, ot_rows=ot_rows, machine_rows=machine_rows)


init_db()

if __name__ == '__main__':
    app.run(debug=True)
