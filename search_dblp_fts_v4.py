#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import re
import sqlite3
from rapidfuzz import fuzz

STOPWORDS = {"a", "an", "the", "of", "and", "or", "in", "on", "at", "to", "for", "by", "with"}

def normalize_text(s: str) -> str:
    if not s:
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def tokenize(s: str):
    return normalize_text(s).split()

def content_tokens(s: str):
    return [t for t in tokenize(s) if t not in STOPWORDS]

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
    # Use prefix matching (token*) so e.g. "simpoints" also matches "simpoint"
    return " OR ".join(t + "*" for t in uniq)

def venue_bonus_from_key(dblp_key: str) -> float:
    if not dblp_key:
        return 0.0
    k = dblp_key.lower()
    if k.startswith("conf/"):
        return 2.0
    if k.startswith("journals/") and "/corr/" not in k:
        return 1.2
    if "/corr/" in k or k.startswith("journals/corr/"):
        return 0.0
    return 0.5

def fetch_candidates(conn, query, candidates=200, year=None, mode="title"):
    q_norm = normalize_text(query)
    phrase_q = f'"{q_norm}"'
    or_q = make_or_query(query)

    rows = []

    base_sql = """
    SELECT title, title_norm, authors, dblp_key, year, doi, bm25(papers_fts) AS bm
    FROM papers_fts
    WHERE papers_fts MATCH ?
    """

    def _exec_query(match_expr):
        sql = base_sql
        params = [match_expr]
        if year is not None:
            sql += " AND year = ?"
            params.append(str(year))
        sql += " ORDER BY bm LIMIT ?"
        params.append(candidates)
        return conn.execute(sql, params).fetchall()

    if mode == "exact":
        # Exact mode: only phrase query
        try:
            rows.extend(_exec_query(phrase_q))
        except sqlite3.OperationalError:
            pass
    else:
        # All other modes: phrase + OR query
        try:
            rows.extend(_exec_query(phrase_q))
        except sqlite3.OperationalError:
            pass
        rows.extend(_exec_query(or_q))

    # Deduplicate by (title_norm, dblp_key)
    seen = set()
    uniq_rows = []
    for row in rows:
        dedup_key = (row[1], row[3])
        if dedup_key not in seen:
            seen.add(dedup_key)
            uniq_rows.append(row)

    return uniq_rows

def aligned_token_penalty(q_tokens, t_tokens):
    if len(q_tokens) != len(t_tokens):
        return 0.0

    penalty = 0.0
    mismatch_count = 0
    for i, (q, t) in enumerate(zip(q_tokens, t_tokens)):
        if q != t:
            mismatch_count += 1
            p = 5.0
            if i == 0:
                p += 8.0
            if len(q) >= 5 and len(t) >= 5:
                p += 2.0
            penalty += p

    if mismatch_count >= 2:
        penalty += 4.0 * (mismatch_count - 1)

    return penalty

def _score_title_mode(q_norm, q_tokens, q_content, t_tokens, t_content, title_norm, bm, dblp_key, year, cand_year):
    """Score for title mode: precise title matching with strict penalties."""
    # 全词相似度
    s_ratio = fuzz.ratio(q_norm, title_norm)
    s_sort = fuzz.token_sort_ratio(q_norm, title_norm)
    s_set = fuzz.token_set_ratio(q_norm, title_norm)

    # 内容词相似度
    q_content_str = " ".join(q_content)
    t_content_str = " ".join(t_content)
    s_content = fuzz.ratio(q_content_str, t_content_str) if q_content and t_content else 0.0

    score = (
        0.30 * s_ratio +
        0.15 * s_sort +
        0.10 * s_set +
        0.45 * s_content
    )

    # 内容词覆盖率
    q_content_set = set(q_content)
    t_content_set = set(t_content)
    if q_content_set:
        coverage = len(q_content_set & t_content_set) / len(q_content_set)
        score += 20.0 * coverage

    # 内容词首词不一致，重罚
    if q_content and t_content and q_content[0] != t_content[0]:
        score -= 10.0

    # 位置对齐惩罚
    score -= aligned_token_penalty(q_tokens, t_tokens)

    # 长度差惩罚（严格）
    score -= 2.5 * abs(len(q_tokens) - len(t_tokens))
    score -= min(abs(len(q_norm) - len(title_norm)) * 0.10, 8.0)

    # query 是 candidate 的扩展标题
    if q_norm in title_norm and title_norm != q_norm:
        score -= 10.0

    # 候选是 query 的超集
    if q_content_set and q_content_set.issubset(t_content_set) and len(t_content_set) > len(q_content_set):
        score -= 6.0

    # BM25 微调
    score += max(0.0, min(6.0, -bm / 12.0))

    # 年份微调
    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 2.0

    score += venue_bonus_from_key(dblp_key)
    return score


def _score_keyword_mode(q_norm, q_tokens, q_content, t_tokens, t_content, title_norm, bm, dblp_key, year, cand_year):
    """Score for keyword mode: topic exploration, emphasizing coverage and BM25."""
    q_content_set = set(q_content)
    t_content_set = set(t_content)

    if q_content_set:
        coverage = len(q_content_set & t_content_set) / len(q_content_set)
    else:
        coverage = 0.0

    s_set = fuzz.token_set_ratio(q_norm, title_norm)
    s_sort = fuzz.token_sort_ratio(q_norm, title_norm)

    score = (
        0.25 * s_set +
        0.15 * s_sort +
        40.0 * coverage
    )

    score += max(0.0, min(15.0, -bm / 6.0))

    token_diff = max(0, len(t_tokens) - len(q_tokens) * 4)
    score -= 0.5 * token_diff

    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 3.0

    score += venue_bonus_from_key(dblp_key)
    return score


def _score_fuzzy_mode(q_norm, q_tokens, q_content, t_tokens, t_content, title_norm, bm, dblp_key, year, cand_year):
    """Score for fuzzy mode: tolerant of typos and word order differences."""
    s_set = fuzz.token_set_ratio(q_norm, title_norm)
    s_sort = fuzz.token_sort_ratio(q_norm, title_norm)
    s_partial = fuzz.partial_ratio(q_norm, title_norm)
    s_ratio = fuzz.ratio(q_norm, title_norm)

    # Emphasize order-insensitive and partial matching
    score = (
        0.35 * s_set +
        0.30 * s_sort +
        0.20 * s_partial +
        0.15 * s_ratio
    )

    # Content token coverage (order-insensitive)
    q_content_set = set(q_content)
    t_content_set = set(t_content)
    if q_content_set:
        coverage = len(q_content_set & t_content_set) / len(q_content_set)
        score += 15.0 * coverage

    # Very light length penalty (fuzzy should be tolerant)
    score -= 0.8 * abs(len(q_tokens) - len(t_tokens))

    # BM25 moderate weight
    score += max(0.0, min(8.0, -bm / 10.0))

    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 2.0

    score += venue_bonus_from_key(dblp_key)
    return score


def _score_exact_mode(q_norm, q_tokens, q_content, t_tokens, t_content, title_norm, bm, dblp_key, year, cand_year):
    """Score for exact mode: strict phrase matching, BM25-dominated."""
    # Phrase match quality via strict ratio
    s_ratio = fuzz.ratio(q_norm, title_norm)

    # How well the query appears as a contiguous phrase in the title
    s_partial = fuzz.partial_ratio(q_norm, title_norm)

    score = (
        0.40 * s_ratio +
        0.30 * s_partial
    )

    # BM25 is king in exact mode (phrase query already filters well)
    score += max(0.0, min(20.0, -bm / 5.0))

    # Penalize titles much longer than query (we want tight matches)
    score -= 1.5 * max(0, len(t_tokens) - len(q_tokens))

    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 2.0

    score += venue_bonus_from_key(dblp_key)
    return score


def _score_similar_mode(q_norm, q_tokens, q_content, t_tokens, t_content, title_norm, bm, dblp_key, year, cand_year):
    """Score for similar mode: find papers with similar topics, exclude exact match."""
    # Skip exact self-match (we want *similar* papers, not the same one)
    if title_norm == q_norm:
        return -1.0

    q_content_set = set(q_content)
    t_content_set = set(t_content)

    # Content word overlap is the core signal
    if q_content_set:
        overlap = q_content_set & t_content_set
        coverage = len(overlap) / len(q_content_set)
        # Jaccard similarity rewards mutual relevance
        union = q_content_set | t_content_set
        jaccard = len(overlap) / len(union) if union else 0.0
    else:
        coverage = 0.0
        jaccard = 0.0

    s_set = fuzz.token_set_ratio(q_norm, title_norm)
    s_sort = fuzz.token_sort_ratio(q_norm, title_norm)

    score = (
        0.20 * s_set +
        0.10 * s_sort +
        30.0 * coverage +
        20.0 * jaccard
    )

    # BM25 significant weight
    score += max(0.0, min(12.0, -bm / 8.0))

    # Light length penalty (similar papers can vary in length)
    score -= 0.5 * abs(len(q_tokens) - len(t_tokens))

    # Penalize very high string similarity (too close = likely same paper variant)
    if fuzz.ratio(q_norm, title_norm) > 92:
        score -= 15.0

    if year is not None and cand_year is not None and str(year) == str(cand_year):
        score += 2.0

    score += venue_bonus_from_key(dblp_key)
    return score


def score_candidate(query, title, title_norm, bm, dblp_key="", year=None, cand_year=None, mode="title"):
    q_norm = normalize_text(query)
    q_tokens = tokenize(query)
    t_tokens = tokenize(title)

    q_content = content_tokens(query)
    t_content = content_tokens(title)

    # 完全匹配快通道（两种模式通用）
    if title_norm == q_norm:
        return 10000.0 + venue_bonus_from_key(dblp_key)
    if q_tokens == t_tokens:
        return 9990.0 + venue_bonus_from_key(dblp_key)

    score_funcs = {
        "title": _score_title_mode,
        "keyword": _score_keyword_mode,
        "fuzzy": _score_fuzzy_mode,
        "exact": _score_exact_mode,
        "similar": _score_similar_mode,
    }
    score_func = score_funcs.get(mode, _score_title_mode)
    return score_func(
        q_norm, q_tokens, q_content, t_tokens, t_content,
        title_norm, bm, dblp_key, year, cand_year,
    )

def dedup_results(results):
    best = {}
    for r in results:
        tnorm = normalize_text(r["title"])
        if tnorm not in best or r["score"] > best[tnorm]["score"]:
            best[tnorm] = r
    return sorted(best.values(), key=lambda x: x["score"], reverse=True)

def search(conn, query, candidates=200, top_k=10, year=None, mode="title"):
    rows = fetch_candidates(conn, query, candidates=candidates, year=year, mode=mode)

    results = []
    for title, title_norm, authors, dblp_key, y, doi, bm in rows:
        score = score_candidate(
            query=query,
            title=title,
            title_norm=title_norm,
            bm=bm,
            dblp_key=dblp_key,
            year=year,
            cand_year=y,
            mode=mode,
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
    ap = argparse.ArgumentParser(description="Improved DBLP title search v4")
    ap.add_argument("db", help="Path to sqlite db")
    ap.add_argument("query", help="Paper title query")
    ap.add_argument("-k", "--top-k", type=int, default=10)
    ap.add_argument("-c", "--candidates", type=int, default=200)
    ap.add_argument("--year", type=int, default=None)

    mode_group = ap.add_mutually_exclusive_group()
    mode_group.add_argument(
        "--keyword", action="store_const", const="keyword", dest="mode",
        help="Keyword/topic exploration mode (broad matching, BM25 heavy)",
    )
    mode_group.add_argument(
        "--fuzzy", action="store_const", const="fuzzy", dest="mode",
        help="Fuzzy mode (tolerant of typos and word order differences)",
    )
    mode_group.add_argument(
        "--exact", action="store_const", const="exact", dest="mode",
        help="Exact phrase mode (strict phrase matching, BM25 dominated)",
    )
    mode_group.add_argument(
        "--similar", action="store_const", const="similar", dest="mode",
        help="Similar paper discovery (find papers with similar topics, excludes exact match)",
    )
    ap.set_defaults(mode="title")

    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    results = search(conn, args.query, candidates=args.candidates, top_k=args.top_k, year=args.year, mode=args.mode)
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
