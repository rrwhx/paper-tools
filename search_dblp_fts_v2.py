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

def tokenize(s: str):
    return normalize_text(s).split()

def make_or_query(query: str) -> str:
    tokens = tokenize(query)
    if not tokens:
        return ""
    seen = set()
    uniq = []
    for t in tokens:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return " OR ".join(uniq)

def fetch_candidates(conn, query, candidates=200, year=None):
    q_norm = normalize_text(query)
    phrase_q = f'"{q_norm}"'
    or_q = make_or_query(query)

    rows = []

    # 1) 先做 phrase query
    sql1 = """
    SELECT title, title_norm, authors, dblp_key, year, bm25(papers_fts) AS bm
    FROM papers_fts
    WHERE papers_fts MATCH ?
    """
    params1 = [phrase_q]
    if year is not None:
        sql1 += " AND year = ?"
        params1.append(str(year))
    sql1 += " ORDER BY bm LIMIT ?"
    params1.append(candidates)

    try:
        rows.extend(conn.execute(sql1, params1).fetchall())
    except sqlite3.OperationalError:
        pass

    # 2) 再做 OR query 补充候选
    sql2 = """
    SELECT title, title_norm, authors, dblp_key, year, bm25(papers_fts) AS bm
    FROM papers_fts
    WHERE papers_fts MATCH ?
    """
    params2 = [or_q]
    if year is not None:
        sql2 += " AND year = ?"
        params2.append(str(year))
    sql2 += " ORDER BY bm LIMIT ?"
    params2.append(candidates)

    rows.extend(conn.execute(sql2, params2).fetchall())

    # 去重
    seen = set()
    uniq_rows = []
    for row in rows:
        key = (row[0], row[3], row[4])
        if key not in seen:
            seen.add(key)
            uniq_rows.append(row)

    return uniq_rows

def score_candidate(query, title, title_norm, bm, year=None, cand_year=None):
    q_norm = normalize_text(query)
    q_tokens = q_norm.split()
    t_tokens = title_norm.split()

    # 1) 完全一致：最高优先级
    if title_norm == q_norm:
        return 10000.0

    # 2) 各种相似度
    s_ratio = fuzz.ratio(q_norm, title_norm)
    s_sort = fuzz.token_sort_ratio(q_norm, title_norm)
    s_set = fuzz.token_set_ratio(q_norm, title_norm)
    s_partial = fuzz.partial_ratio(q_norm, title_norm)

    # 3) 基础分：更强调顺序一致的匹配
    score = (
        0.45 * s_ratio +
        0.30 * s_sort +
        0.20 * s_set +
        0.05 * s_partial
    )

    # 4) 长度惩罚：标题长很多/短很多都扣分
    len_diff = abs(len(q_tokens) - len(t_tokens))
    score -= 2.5 * len_diff

    char_len_diff = abs(len(q_norm) - len(title_norm))
    score -= min(char_len_diff * 0.15, 10)

    # 5) 若 query 是 candidate 的严格子串，说明 candidate 可能是“扩展标题”
    #    例如 "Attention Is All You Need Until You Need Retention"
    if q_norm in title_norm and title_norm != q_norm:
        score -= 8

    # 6) 如果 token 完全包含但 candidate 多很多词，也惩罚
    q_set = set(q_tokens)
    t_set = set(t_tokens)
    if q_set.issubset(t_set) and len(t_set) > len(q_set):
        score -= 6

    # 7) BM25 微调（bm 越小越相关）
    score += max(0, min(8, -bm / 10.0))

    # 8) 年份加分（可选）
    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 2

    return score

def search(conn, query, candidates=200, top_k=10, year=None):
    rows = fetch_candidates(conn, query, candidates=candidates, year=year)

    results = []
    for title, title_norm, authors, dblp_key, y, bm in rows:
        score = score_candidate(query, title, title_norm, bm, year=year, cand_year=y)
        results.append({
            "score": score,
            "title": title,
            "authors": authors,
            "key": dblp_key,
            "year": y,
            "bm25": bm,
        })

    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:top_k]

def main():
    ap = argparse.ArgumentParser(description="Improved DBLP title search")
    ap.add_argument("db", help="Path to sqlite db")
    ap.add_argument("query", help="Paper title query")
    ap.add_argument("-k", "--top-k", type=int, default=10)
    ap.add_argument("-c", "--candidates", type=int, default=200)
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
        print()

if __name__ == "__main__":
    main()
