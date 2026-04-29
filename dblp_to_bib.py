#!/usr/bin/env python3
"""Convert DBLP XML records to BibTeX format.

This script reads a DBLP XML file and converts each publication record
(article, inproceedings, book, etc.) into a BibTeX entry, preserving
as much metadata as possible.
"""

import argparse
import gzip
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from typing import Optional

from dblp_parser import load_dtd_entities

logger = logging.getLogger(__name__)

# DBLP record types to BibTeX entry types mapping
DBLP_TO_BIBTEX = {
    "article": "article",
    "inproceedings": "inproceedings",
    "proceedings": "proceedings",
    "book": "book",
    "incollection": "incollection",
    "phdthesis": "phdthesis",
    "mastersthesis": "mastersthesis",
    "www": "misc",
    "person": "misc",
    "data": "misc",
}

# BibTeX field mapping from DBLP XML tags
FIELD_MAPPING = {
    "title": "title",
    "author": "author",
    "editor": "editor",
    "year": "year",
    "journal": "journal",
    "volume": "volume",
    "number": "number",
    "pages": "pages",
    "publisher": "publisher",
    "address": "address",
    "school": "school",
    "series": "series",
    "booktitle": "booktitle",
    "month": "month",
    "isbn": "isbn",
    "note": "note",
    "chapter": "chapter",
}


def normalize_bibtex_key(key: str) -> str:
    """Convert DBLP key to a valid BibTeX citation key.

    DBLP keys look like ``conf/iccv/SmithJ14`` or ``reference/vision/Ren14``.
    Just taking the last segment is ambiguous because:
      * proceedings entries end with the year (``conf/iccv/2014``) and would
        all collapse to the same key.
      * the same surname+year suffix can appear under different venues
        (``reference/vision/Ren14`` vs ``journals/xxx/Ren14``).

    We therefore keep the venue token as a prefix and join with an underscore,
    e.g. ``conf/iccv/SmithJ14`` -> ``iccv_SmithJ14``,
    ``reference/vision/Ren14`` -> ``vision_Ren14``,
    ``conf/iccv/2014`` -> ``iccv_2014``.

    Any character not allowed in a BibTeX cite key is replaced by ``_``.
    """
    parts = key.split("/")
    if len(parts) >= 3:
        # <type>/<venue>/<suffix>  -> <venue>_<suffix>
        cite = f"{parts[-2]}_{parts[-1]}"
    elif len(parts) == 2:
        cite = f"{parts[0]}_{parts[1]}"
    else:
        cite = parts[0]
    # BibTeX cite keys must avoid whitespace and a few reserved chars.
    cite = re.sub(r"[^A-Za-z0-9_\-:.]", "_", cite)
    return cite


def escape_bibtex(text: str) -> str:
    """Escape special characters for ordinary BibTeX text fields."""
    if not text:
        return ""
    # Escape common special characters
    text = text.replace("&", "\\&")
    text = text.replace("%", "\\%")
    text = text.replace("#", "\\#")
    text = text.replace("_", "\\_")
    text = text.replace("{", "\\{")
    text = text.replace("}", "\\}")
    text = text.replace("~", "\\textasciitilde{}")
    text = text.replace("^", "\\textasciicircum{}")
    return text

def escape_bibtex_url(text: str) -> str:
    """Escape only LaTeX-fatal characters for URL/DOI fields.

    LaTeX URL renderers (``\\url{}``, ``\\href{}{}``) treat their argument as
    verbatim, so characters like ``_ ~ ^`` are *already* safe inside a URL and
    must NOT be backslash-escaped (doing so would corrupt the link). However a
    handful of characters still terminate parsing early even inside a URL
    macro and must be escaped:

    * ``%`` - LaTeX comment start, swallows the rest of the line.
    * ``#`` - LaTeX macro parameter marker.
    * ``&`` - LaTeX alignment tab.

    DBLP DOI links are usually safe, but non-DOI ``ee`` URLs (e.g. publisher
    pages with ``#`` anchors or ``&`` query strings) require this escaping.
    """
    if not text:
        return ""
    text = text.replace("\\", "\\\\")  # rare but be defensive
    text = text.replace("%", "\\%")
    text = text.replace("#", "\\#")
    text = text.replace("&", "\\&")
    return text


def get_element_text(element: ET.Element, tag: str) -> str:
    """Safely extract text from a child element, including nested tags."""
    node = element.find(tag)
    if node is None:
        return ""
    return "".join(node.itertext()).strip()


# DBLP appends a 4-digit homonym index to disambiguate authors with identical
# names, e.g. "Gaurav Srivastava 0004", "Manish Singh 0001". The number is not
# part of the actual name and must be stripped before formatting.
_DBLP_HOMONYM_RE = re.compile(r"\s+\d{4}$")

def _format_person_name(text: str) -> str:
    """Convert a DBLP person name to BibTeX ``Last, First`` form.

    Strips the trailing 4-digit DBLP homonym index (e.g. ``Foo Bar 0001``)
    before splitting first/last names.
    """
    text = _DBLP_HOMONYM_RE.sub("", text).strip()
    if not text:
        return ""
    parts = text.split()
    if len(parts) >= 2:
        last = parts[-1]
        first = " ".join(parts[:-1])
        return f"{last}, {first}"
    return text

def extract_authors(element: ET.Element) -> str:
    """Extract authors as a BibTeX author list."""
    authors = []
    for author in element.findall("author"):
        text = "".join(author.itertext()).strip()
        formatted = _format_person_name(text)
        if formatted:
            authors.append(formatted)
    return " and ".join(authors)

def extract_editors(element: ET.Element) -> str:
    """Extract editors as a BibTeX editor list."""
    editors = []
    for editor in element.findall("editor"):
        text = "".join(editor.itertext()).strip()
        formatted = _format_person_name(text)
        if formatted:
            editors.append(formatted)
    return " and ".join(editors)


def element_to_bibtex(element: ET.Element) -> Optional[str]:
    """Convert a DBLP XML element to a BibTeX entry."""
    tag = element.tag
    if tag not in DBLP_TO_BIBTEX:
        return None

    entry_type = DBLP_TO_BIBTEX[tag]
    dblp_key = element.get("key", "")
    if not dblp_key:
        return None

    cite_key = normalize_bibtex_key(dblp_key)

    # Extract fields
    fields = {}

    # Title (required for most entries)
    title = get_element_text(element, "title")
    if title:
        fields["title"] = f"{{{escape_bibtex(title)}}}"

    # Authors/Editors
    authors = extract_authors(element)
    if authors:
        fields["author"] = authors

    editors = extract_editors(element)
    if editors:
        fields["editor"] = editors

    # Year
    year = get_element_text(element, "year")
    if year:
        fields["year"] = year

    # Journal (for articles)
    if tag == "article":
        journal = get_element_text(element, "journal")
        if journal:
            fields["journal"] = f"{{{escape_bibtex(journal)}}}"

    # Booktitle (for inproceedings, incollection)
    if tag in ("inproceedings", "incollection"):
        booktitle = get_element_text(element, "booktitle")
        if not booktitle:
            # Fallback to proceedings' booktitle
            booktitle = get_element_text(element, "title")
        if booktitle:
            fields["booktitle"] = f"{{{escape_bibtex(booktitle)}}}"

    # Other fields
    for xml_tag, bib_field in FIELD_MAPPING.items():
        if xml_tag in ("title", "author", "editor", "year", "journal", "booktitle"):
            continue
        text = get_element_text(element, xml_tag)
        if text:
            fields[bib_field] = escape_bibtex(text)

    # Page ranges in BibTeX should use an en-dash (--) so that LaTeX renders
    # them as a proper "–" instead of a hyphen. DBLP stores them with a single
    # hyphen (e.g. "36-41"); rewrite numeric ranges in place. Comma-separated
    # ranges such as "12-15, 20-22" are handled because the regex matches each
    # numeric pair independently.
    if "pages" in fields:
        fields["pages"] = re.sub(
            r"(\d+)\s*-\s*(\d+)", r"\1--\2", fields["pages"]
        )

    # Handle DOI / electronic edition links. DBLP stores all electronic
    # locators in <ee>; when the value is a doi.org URL we want both:
    #   * a bare ``doi`` field (BibTeX/BibLaTeX convention is to store DOIs
    #     without the ``https://doi.org/`` prefix; styles add it back when
    #     rendering, otherwise links can become ``https://https://doi.org/``).
    #   * the original URL in ``url`` so plain BibTeX styles that ignore
    #     ``doi`` still produce a clickable link.
    # For non-DOI EE values we only emit ``url``.
    DOI_URL_PREFIXES = ("https://doi.org/", "http://doi.org/",
                        "https://dx.doi.org/", "http://dx.doi.org/")
    for ee_node in element.findall("ee"):
        ee = "".join(ee_node.itertext()).strip()
        if not ee:
            continue
        matched_prefix = next(
            (p for p in DOI_URL_PREFIXES if ee.startswith(p)), None
        )
        if matched_prefix:
            # DOI itself rarely contains LaTeX-fatal chars, but escape
            # defensively for safety.
            fields.setdefault("doi", escape_bibtex_url(ee[len(matched_prefix):]))
            fields.setdefault("url", escape_bibtex_url(ee))
        else:
            fields.setdefault("url", escape_bibtex_url(ee))

    # Add DBLP key as note
    fields["note"] = f"DBLP: {dblp_key}"

    # Build BibTeX entry. The closing brace lives on its own line so that
    # downstream tools (diff, grep, biber pretty printers) can detect entry
    # boundaries reliably and the file remains easy for humans to scan.
    lines = [f"@{entry_type}{{{cite_key},"]
    for field, value in fields.items():
        lines.append(f"  {field} = {{{value}}},")
    lines[-1] = lines[-1].rstrip(",")
    lines.append("}")
    lines.append("")

    return "\n".join(lines)


def convert_dblp_to_bibtex(
    xml_path: str,
    output_path: Optional[str] = None,
    entry_type: Optional[str] = None,
    limit: Optional[int] = None,
) -> None:
    """Convert DBLP XML to BibTeX."""
    # Load DTD entities
    dtd_path = os.path.join(os.path.dirname(xml_path) or ".", "dblp.dtd")
    if not os.path.exists(dtd_path):
        dtd_path = "dblp.dtd"
    entities = load_dtd_entities(dtd_path)

    parser = ET.XMLParser()
    for name, val in entities.items():
        parser.entity[name] = val

    # Open XML file
    open_func = gzip.open if xml_path.endswith(".gz") else open
    source = xml_path if not xml_path.endswith(".gz") else open_func(xml_path, "rb")

    try:
        context = ET.iterparse(source, events=("end",), parser=parser)
    finally:
        if xml_path.endswith(".gz") and hasattr(source, 'close'):
            source.close()

    # Determine output
    if output_path is None:
        output_path = sys.stdout
    else:
        output_path = open(output_path, "w", encoding="utf-8")

    count = 0
    matched_count = 0

    try:
        for _, element in context:
            if element.tag not in DBLP_TO_BIBTEX:
                continue

            # Stop before counting/processing the next record once the limit
            # has been reached, otherwise ``count`` would be one ahead of the
            # records actually processed.
            if limit and matched_count >= limit:
                break

            count += 1

            # Filter by entry type if specified
            if entry_type and element.tag != entry_type:
                continue

            bibtex = element_to_bibtex(element)
            if bibtex:
                output_path.write(bibtex)
                matched_count += 1

            element.clear()

        logger.info("Processed %d records, wrote %d BibTeX entries.", count, matched_count)

    finally:
        if output_path != sys.stdout:
            output_path.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    ap = argparse.ArgumentParser(description="Convert DBLP XML to BibTeX format")
    ap.add_argument("xml", help="Path to dblp.xml or dblp.xml.gz")
    ap.add_argument("-o", "--output", help="Output .bib file (default: stdout)")
    ap.add_argument(
        "-t", "--type",
        choices=list(DBLP_TO_BIBTEX.keys()),
        help="Filter by entry type (article, inproceedings, etc.)",
    )
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Only convert first N records (for testing)",
    )
    args = ap.parse_args()

    if not os.path.exists(args.xml):
        logger.error("XML file not found: %s", args.xml)
        sys.exit(1)

    convert_dblp_to_bibtex(
        xml_path=args.xml,
        output_path=args.output,
        entry_type=args.type,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
