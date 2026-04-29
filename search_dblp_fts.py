#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import sqlite3
from rapidfuzz import fuzz

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def make_fts_query(query: str) -> str:
    tokens = normalize_text(query).split()
    if not tokens:
        return ""
    seen = set()
    uniq = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return " OR ".join(uniq)

def search(conn, query, candidates=100, top_k=10, year=None):
    q_norm = normalize_text(query)
    fts_q = make_fts_query(query)

    sql = """
    SELECT title, title_norm, authors, dblp_key, year, doi, bm25(papers_fts) as bm
    FROM papers_fts
    WHERE papers_fts MATCH ?
    """
    params = [fts_q]

    if year:
        sql += " AND year = ?"
        params.append(str(year))

    sql += " ORDER BY bm LIMIT ?"
    params.append(candidates)

    rows = conn.execute(sql, params).fetchall()

    results = []
    for title, title_norm, authors, dblp_key, y, doi, bm in rows:
        s1 = fuzz.token_set_ratio(q_norm, title_norm)
        s2 = fuzz.partial_ratio(q_norm, title_norm)
        s3 = fuzz.ratio(q_norm, title_norm)

        # bm25 越小越好，转成一个轻微加分项
        bm_bonus = max(0, 20 + (-bm))
        score = max(s1, s2 * 0.98, s3 * 0.95) + min(bm_bonus, 5)

        if year and str(y) == str(year):
            score += 2

        results.append({
            "score": score,
            "title": title,
            "authors": authors,
            "key": dblp_key,
            "year": y,
            "doi": doi or "",
            "bm25": bm,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]

def main():
    ap = argparse.ArgumentParser(description="Search DBLP titles via SQLite FTS5 + BM25 + RapidFuzz")
    ap.add_argument("db", help="Path to sqlite db")
    ap.add_argument("query", help="Paper title query")
    ap.add_argument("-k", "--top-k", type=int, default=10)
    ap.add_argument("-c", "--candidates", type=int, default=100)
    ap.add_argument("--year", type=int, default=None)
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    results = search(conn, args.query, candidates=args.candidates, top_k=args.top_k, year=args.year)
    conn.close()

    for i, r in enumerate(results, 1):
        print(f"[{i}] score={r['score']:.2f} bm25={r['bm25']:.4f}")
        print(f"    title  : {r['title']}")
        print(f"    year   : {r['year']}")
        print(f"    authors: {r['authors']}")
        print(f"    key    : {r['key']}")
        print(f"    doi    : {r['doi']}")
        print()

if __name__ == "__main__":
    main()
