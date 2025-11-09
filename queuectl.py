#!/usr/bin/env python3
import click
import json
import uuid
import multiprocessing
import os
from datetime import datetime
from db import init_db, insert_job, list_jobs, get_job, get_conn
from worker import worker_loop
from config import load_config, save_config
from utils import now_iso


@click.group()
def cli():
"""queuectl — background job queue CLI"""
pass


@cli.command()
@click.argument('job_json')
def enqueue(job_json):
"""Enqueue a job with JSON payload (string).
Example: queuectl enqueue '{"id":"job1","command":"echo hi"}'
"""
init_db()
job = json.loads(job_json)
if "id" not in job:
job["id"] = str(uuid.uuid4())
now = now_iso()
job.setdefault("created_at", now)
job.setdefault("updated_at", now)
if "max_retries" not in job:
job["max_retries"] = load_config().get("default_max_retries", 3)
insert_job(job)
click.echo(f"Enqueued {job['id']}")


@cli.group()
def worker():
"""Worker management commands"""
pass


@worker.command('start')
@click.option('--count', default=None, type=int, help='Number of worker processes to start')
@click.option('--base', default=None, type=int, help='Backoff base (overrides config)')
def worker_start(count, base):
init_db()
cfg = load_config()
if count is None:
count = cfg.get('worker_count', 1)
base_backoff = base if base is not None else cfg.get('backoff_base', 2)
procs = []
for _ in range(count):
p = multiprocessing.Process(target=worker_loop, args=(str(uuid.uuid4()), base_backoff))
p.start()
procs.append(p)
click.echo(f"Started {count} workers (PIDs: {', '.join(str(p.pid) for p in procs)})")
try:
for p in procs:
p.join()
except KeyboardInterrupt:
click.echo("Stopping workers...")
for p in procs:
p.terminate()
for p in procs:
p.join()


@cli.command()
@click.option('--state', default=None, help='Filter by job state')
def list(state):
init_db()
rows = list_jobs(state)
for r in rows:
click.echo(f"{r['id']} | {r['state']} | attempts={r['attempts']} | cmd={r['command']}")


@cli.command()
def status():
init_db()
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT state, COUNT(*) as c FROM jobs GROUP BY state")
rows = cur.fetchall()
click.echo("Job counts
