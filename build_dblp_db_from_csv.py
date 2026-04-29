#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import re
import sqlite3
import sys
import time

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=DELETE;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")

    cur.execute("""
    CREATE VIRTUAL TABLE IF NOT EXISTS papers_fts
    USING fts5(
        title,
        title_norm,
        authors,
        dblp_key UNINDEXED,
        year UNINDEXED,
        doi UNINDEXED,
        tokenize='unicode61'
    );
    """)

    conn.commit()

def detect_columns(fieldnames):
    fields = {f.lower(): f for f in fieldnames}

    key_col = None
    if "key" in fields:
        key_col = fields["key"]

    title_col = None
    if "title" in fields:
        title_col = fields["title"]

    authors_col = None
    if "authors" in fields:
        authors_col = fields["authors"]

    year_col = None
    if "year" in fields:
        year_col = fields["year"]

    doi_col = None
    if "doi" in fields:
        doi_col = fields["doi"]

    return key_col, title_col, authors_col, year_col, doi_col

def build_from_csv(csv_path, db_path, encoding="utf-8", batch_size=10000):
    conn = sqlite3.connect(db_path)
    init_db(conn)
    cur = conn.cursor()

    total = 0
    t0 = time.time()

    with open(csv_path, "r", encoding=encoding, newline="") as f:
        reader = csv.DictReader(f)

        if not reader.fieldnames:
            raise ValueError("CSV 没有表头")

        key_col, title_col, authors_col, year_col, doi_col = detect_columns(reader.fieldnames)

        if not title_col:
            raise ValueError(f"找不到 title 列，现有列: {reader.fieldnames}")

        print(f"[info] detected columns:", file=sys.stderr)
        print(f"       key={key_col}, title={title_col}, authors={authors_col}, year={year_col}, doi={doi_col}", file=sys.stderr)

        batch = []

        for row in reader:
            title = (row.get(title_col) or "").strip()
            if not title:
                continue

            dblp_key = (row.get(key_col) or "").strip() if key_col else ""
            authors = (row.get(authors_col) or "").strip() if authors_col else ""
            year = (row.get(year_col) or "").strip() if year_col else ""
            doi = (row.get(doi_col) or "").strip() if doi_col else ""
            title_norm = normalize_text(title)

            batch.append((title, title_norm, authors, dblp_key, year, doi))
            total += 1

            if len(batch) >= batch_size:
                cur.executemany("""
                    INSERT INTO papers_fts (title, title_norm, authors, dblp_key, year, doi)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, batch)
                conn.commit()
                batch.clear()
                print(f"[build] inserted={total} elapsed={time.time() - t0:.1f}s", file=sys.stderr)

        if batch:
            cur.executemany("""
                INSERT INTO papers_fts (title, title_norm, authors, dblp_key, year, doi)
                VALUES (?, ?, ?, ?, ?, ?)
            """, batch)
            conn.commit()

    print(f"[done] total={total} elapsed={time.time() - t0:.1f}s db={db_path}", file=sys.stderr)
    conn.close()

def main():
    ap = argparse.ArgumentParser(description="Build SQLite FTS5 DB from DBLP CSV (FTS-only, safer)")
    ap.add_argument("csv", help="Input CSV file")
    ap.add_argument("-o", "--output", default="dblp_fts.db", help="Output sqlite db path")
    ap.add_argument("--encoding", default="utf-8", help="CSV encoding")
    ap.add_argument("--batch-size", type=int, default=100000, help="Batch insert size")
    args = ap.parse_args()

    build_from_csv(args.csv, args.output, encoding=args.encoding, batch_size=args.batch_size)

if __name__ == "__main__":
    main()
