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

def venue_bonus_from_key(dblp_key: str) -> float:
    """偏向正式发表版本，而不是 corr/arxiv 版本"""
    if not dblp_key:
        return 0.0
    k = dblp_key.lower()
    if k.startswith("conf/"):
        return 2.0
    if k.startswith("journals/"):
        return 1.0
    if "/corr/" in k or k.startswith("journals/corr/"):
        return 0.0
    return 0.5

def fetch_candidates(conn, query, candidates=200, year=None):
    q_norm = normalize_text(query)
    phrase_q = f'"{q_norm}"'
    or_q = make_or_query(query)

    rows = []

    sql1 = """
    SELECT title, title_norm, authors, dblp_key, year, doi, bm25(papers_fts) AS bm
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

    sql2 = """
    SELECT title, title_norm, authors, dblp_key, year, doi, bm25(papers_fts) AS bm
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

    # 去重（title_norm + key）
    seen = set()
    uniq_rows = []
    for row in rows:
        dedup_key = (row[1], row[3])
        if dedup_key not in seen:
            seen.add(dedup_key)
            uniq_rows.append(row)

    return uniq_rows

def aligned_token_penalty(q_tokens, t_tokens):
    """
    对等长标题按位置比较 token。
    如果词位对应但不同，给予惩罚。
    """
    if len(q_tokens) != len(t_tokens):
        return 0.0

    penalty = 0.0
    mismatch_count = 0
    for i, (q, t) in enumerate(zip(q_tokens, t_tokens)):
        if q != t:
            mismatch_count += 1
            p = 6.0
            # 第一词通常最关键
            if i == 0:
                p += 6.0
            # 长词替换通常更严重
            if len(q) >= 5 and len(t) >= 5:
                p += 2.0
            penalty += p

    # 多个位置不一致时再额外罚
    if mismatch_count >= 2:
        penalty += 4.0 * (mismatch_count - 1)

    return penalty

def score_candidate(query, title, title_norm, bm, dblp_key="", year=None, cand_year=None):
    q_norm = normalize_text(query)
    q_tokens = q_norm.split()
    t_tokens = title_norm.split()

    # 1) 完全一致，直接置顶
    if title_norm == q_norm:
        return 10000.0 + venue_bonus_from_key(dblp_key)

    # 2) token 完全一致（理论上和上面差不多，这里留作稳妥处理）
    if q_tokens == t_tokens:
        return 9990.0 + venue_bonus_from_key(dblp_key)

    s_ratio = fuzz.ratio(q_norm, title_norm)
    s_sort = fuzz.token_sort_ratio(q_norm, title_norm)
    s_set = fuzz.token_set_ratio(q_norm, title_norm)
    s_partial = fuzz.partial_ratio(q_norm, title_norm)

    score = (
        0.50 * s_ratio +
        0.25 * s_sort +
        0.15 * s_set +
        0.10 * s_partial
    )

    # 长度差惩罚
    len_diff = abs(len(q_tokens) - len(t_tokens))
    score -= 3.0 * len_diff

    char_len_diff = abs(len(q_norm) - len(title_norm))
    score -= min(char_len_diff * 0.12, 8)

    # query 是 candidate 真子串 -> 扩展标题惩罚
    if q_norm in title_norm and title_norm != q_norm:
        score -= 10.0

    # token 超集惩罚
    q_set = set(q_tokens)
    t_set = set(t_tokens)
    if q_set.issubset(t_set) and len(t_set) > len(q_set):
        score -= 8.0

    # 等长时，位置对齐 token 不同 -> 明显重罚
    score -= aligned_token_penalty(q_tokens, t_tokens)

    # 第一词不同，额外重罚
    if q_tokens and t_tokens and q_tokens[0] != t_tokens[0]:
        score -= 8.0

    # 最后一个词不同，也罚一点
    if q_tokens and t_tokens and q_tokens[-1] != t_tokens[-1]:
        score -= 3.0

    # BM25 微调
    score += max(0.0, min(6.0, -bm / 12.0))

    # 年份微调
    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 2.0

    # 偏向正式发表版本
    score += venue_bonus_from_key(dblp_key)

    return score

def dedup_results(results):
    """
    按标准化标题去重，只保留得分最高的一条。
    """
    best = {}
    for r in results:
        tnorm = normalize_text(r["title"])
        if tnorm not in best or r["score"] > best[tnorm]["score"]:
            best[tnorm] = r
    return sorted(best.values(), key=lambda x: x["score"], reverse=True)

def search(conn, query, candidates=200, top_k=10, year=None):
    rows = fetch_candidates(conn, query, candidates=candidates, year=year)

    results = []
    for title, title_norm, authors, dblp_key, y, doi, bm in rows:
        score = score_candidate(
            query=query,
            title=title,
            title_norm=title_norm,
            bm=bm,
            dblp_key=dblp_key,
            year=year,
            cand_year=y
        )
        results.append({
            "score": score,
            "title": title,
            "authors": authors,
            "key": dblp_key,
            "year": y,
            "doi": doi,
            "bm25": bm,
        })

    results = dedup_results(results)
    return results[:top_k]

def main():
    ap = argparse.ArgumentParser(description="Improved DBLP title search v3")
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
        print(f"    doi    : {r['doi']}")
        print()

if __name__ == "__main__":
    main()
