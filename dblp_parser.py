#!/usr/bin/env python3
"""Extract papers from DBLP XML database by venue and export to CSV."""

import argparse
import collections
import csv
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from typing import Optional

logger = logging.getLogger(__name__)

# DBLP record types that represent papers (excluding www, person, data)
PAPER_TAGS = frozenset({'article', 'inproceedings', 'proceedings', 'book', 'incollection'})

ALL_FIELDS = [
    'type', 'key', 'year', 'venue', 'title', 'authors',
    'volume', 'number', 'pages', 'doi', 'url',
    'publisher', 'isbn', 'series', 'month',
]

DEFAULT_FIELDS = ['year', 'key', 'title', 'doi']

PROGRESS_INTERVAL = 100_000

# DTD entity pattern: <!ENTITY name "value">
DTD_ENTITY_PATTERN = re.compile(r'<!ENTITY\s+(\w+)\s+"([^"]+)"\s*>')


def load_dtd_entities(dtd_path: str) -> dict[str, str]:
    """Parse a DTD file to extract entity definitions.

    Falls back to standard HTML entities if the DTD file is not found.
    """
    if not os.path.exists(dtd_path):
        logger.warning("DTD file not found at %s, falling back to standard HTML entities.", dtd_path)
        import html.entities
        return {name: chr(codepoint) for name, codepoint in html.entities.name2codepoint.items()}

    logger.info("Loading entities from %s ...", dtd_path)
    with open(dtd_path, 'r', encoding='utf-8') as dtd_file:
        content = dtd_file.read()

    entities: dict[str, str] = {}
    for name, raw_value in DTD_ENTITY_PATTERN.findall(content):
        if raw_value.startswith('&#') and raw_value.endswith(';'):
            try:
                entities[name] = chr(int(raw_value[2:-1]))
            except ValueError:
                logger.debug("Skipping unparseable entity: %s -> %s", name, raw_value)
        else:
            entities[name] = raw_value
    return entities


def _resolve_dtd_path(xml_path: str) -> str:
    """Locate the dblp.dtd file next to the XML or in the current directory."""
    candidate = os.path.join(os.path.dirname(xml_path), 'dblp.dtd')
    if os.path.exists(candidate):
        return candidate
    return 'dblp.dtd'


def _get_element_text(element: ET.Element, tag: str) -> str:
    """Safely extract the full inner text of a child element."""
    node = element.find(tag)
    if node is None:
        return ""
    return "".join(node.itertext())


def _extract_authors(element: ET.Element) -> str:
    """Extract author names from an element, falling back to editors."""
    authors = [
        "".join(author.itertext())
        for author in element.findall('author')
        if author.text or len(author) > 0
    ]
    if not authors:
        authors = [
            "".join(editor.itertext()) + " (Ed.)"
            for editor in element.findall('editor')
            if editor.text or len(editor) > 0
        ]
    return "; ".join(authors)


def _extract_record(element: ET.Element, venue_name: str) -> dict[str, str]:
    """Build a record dict from a matched XML element."""
    return {
        'type': element.tag,
        'key': element.get('key', ''),
        'year': _get_element_text(element, 'year') or "0",
        'title': _get_element_text(element, 'title') or "N/A",
        'authors': _extract_authors(element),
        'venue': venue_name,
        'volume': _get_element_text(element, 'volume'),
        'number': _get_element_text(element, 'number'),
        'pages': _get_element_text(element, 'pages'),
        'doi': _get_element_text(element, 'ee'),
        'url': _get_element_text(element, 'url'),
        'publisher': _get_element_text(element, 'publisher'),
        'isbn': _get_element_text(element, 'isbn'),
        'series': _get_element_text(element, 'series'),
        'month': _get_element_text(element, 'month'),
    }


def _sort_records(records: list[dict[str, str]]) -> None:
    """Sort records in-place by year (numerically, ascending), then by title (alphabetically)."""
    records.sort(key=lambda record: (
        int(record['year']) if record['year'].isdigit() else 0,
        record.get('title', '').lower(),
    ))


def _validate_fields(fields: Optional[list[str]]) -> list[str]:
    """Return a validated list of output field names."""
    if not fields:
        return list(ALL_FIELDS)

    valid_fields = [field for field in fields if field in ALL_FIELDS]
    if not valid_fields:
        logger.warning("No valid fields in %s. Using all fields.", fields)
        return list(ALL_FIELDS)

    invalid_fields = set(fields) - set(ALL_FIELDS)
    if invalid_fields:
        logger.warning("Ignoring unknown fields: %s", ", ".join(sorted(invalid_fields)))
    return valid_fields


def _create_xml_parser(xml_path: str) -> ET.XMLParser:
    """Create an XMLParser pre-loaded with DTD entity definitions."""
    dtd_path = _resolve_dtd_path(xml_path)
    entities = load_dtd_entities(dtd_path)

    xml_parser = ET.XMLParser()
    for name, value in entities.items():
        xml_parser.entity[name] = value
    return xml_parser


def _extract_venue_from_key(record_key: str) -> str:
    """Extract the venue prefix (first two path segments) from a DBLP key.

    Example: 'conf/ispass/SmithJ23' → 'conf/ispass'
             'journals/taco/Doe2024' → 'journals/taco'
    """
    parts = record_key.split('/')
    if len(parts) >= 2:
        return parts[0] + '/' + parts[1]
    return record_key


def _scan_records(
    xml_path: str,
    venue_pattern: str,
    limit: Optional[int] = None,
) -> dict[str, list[dict[str, str]]]:
    """Iterate over the DBLP XML and collect records whose key matches the venue pattern.

    The venue_pattern is matched against the venue prefix of each record key
    (e.g. 'conf/ispass', 'journals/taco'). It can be a plain prefix or a regex.

    If limit is set, stop after collecting that many matched records.

    Returns a dict mapping each matched venue to its list of records.
    """
    xml_parser = _create_xml_parser(xml_path)
    context = ET.iterparse(xml_path, events=('end',), parser=xml_parser)

    try:
        compiled_pattern = re.compile(venue_pattern)
        is_regex = True
    except re.error:
        logger.warning("Invalid regex '%s', treating as literal prefix.", venue_pattern)
        compiled_pattern = None
        is_regex = False

    grouped_results: dict[str, list[dict[str, str]]] = collections.defaultdict(list)
    total_count = 0
    matched_count = 0

    for _event, element in context:
        if element.tag not in PAPER_TAGS:
            continue

        total_count += 1
        if total_count % PROGRESS_INTERVAL == 0:
            logger.info("Processed %d records, matched %d ...", total_count, matched_count)

        record_key = element.get('key', '')
        venue_key = _extract_venue_from_key(record_key)

        matched = False
        if is_regex and compiled_pattern is not None:
            matched = compiled_pattern.fullmatch(venue_key) is not None
        else:
            matched = record_key.startswith(venue_pattern)

        if matched:
            grouped_results[venue_key].append(_extract_record(element, venue_key))
            matched_count += 1
            if limit is not None and matched_count >= limit:
                logger.info("Reached limit of %d matched records, stopping early.", limit)
                element.clear()
                break

        element.clear()

    venues_found = sorted(grouped_results.keys())
    logger.info(
        "Scanned %d records, matched %d across %d venue(s): %s",
        total_count, matched_count, len(venues_found), ", ".join(venues_found) or "(none)",
    )
    return dict(grouped_results)


def _venue_to_filename(venue_key: str) -> str:
    """Convert a venue key to a safe CSV filename.

    Replaces path separators with underscores to preserve the full venue path.
    Example: 'conf/ispass' → 'conf_ispass.csv', 'journals/taco' → 'journals_taco.csv'
    """
    safe_name = venue_key.strip('/').replace('/', '_')
    return safe_name + '.csv'


def write_csv(records: list[dict[str, str]], output_path: str, fieldnames: list[str]) -> None:
    """Write records to a CSV file."""
    with open(output_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    logger.info("Wrote %d records to %s", len(records), output_path)


def parse_dblp(
    xml_path: str,
    venue_pattern: str,
    output_path: Optional[str] = None,
    output_dir: Optional[str] = None,
    fields: Optional[list[str]] = None,
    limit: Optional[int] = None,
) -> None:
    """Main pipeline: scan DBLP XML for venue(s), sort by year, and write CSV(s).

    If the venue_pattern matches multiple venues, each venue is written to a
    separate CSV file under output_dir (default: current directory).
    If only one venue matches and output_path is given, that path is used.
    """
    logger.info("Processing %s for venue pattern '%s' ...", xml_path, venue_pattern)

    grouped_records = _scan_records(xml_path, venue_pattern, limit=limit)

    if not grouped_records:
        logger.warning("No records matched venue pattern '%s'.", venue_pattern)
        return

    fieldnames = _validate_fields(fields)
    resolved_output_dir = output_dir or '.'

    if len(grouped_records) == 1 and output_path:
        venue_key = next(iter(grouped_records))
        records = grouped_records[venue_key]
        _sort_records(records)
        write_csv(records, output_path, fieldnames)
    else:
        if output_dir:
            os.makedirs(resolved_output_dir, exist_ok=True)

        for venue_key in sorted(grouped_records):
            records = grouped_records[venue_key]
            _sort_records(records)
            filename = _venue_to_filename(venue_key)
            filepath = os.path.join(resolved_output_dir, filename)
            write_csv(records, filepath, fieldnames)

    logger.info("Done.")


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    argument_parser = argparse.ArgumentParser(
        description='Extract papers from DBLP XML by venue.',
    )
    argument_parser.add_argument(
        'venue',
        help=(
            'Venue key prefix or regex pattern to match against DBLP keys. '
            'Examples: "conf/ispass" (exact), "conf/(ispass|iiswc)" (regex), '
            '"journals/t.*" (regex matching all journals starting with t)'
        ),
    )
    argument_parser.add_argument(
        '--xml', default='dblp.xml',
        help='Path to dblp.xml (default: dblp.xml)',
    )
    argument_parser.add_argument(
        '--output', default=None,
        help=(
            'Output CSV filename (only used when a single venue matches). '
            'Default: derived from venue name, e.g. conf/ispass → ispass.csv'
        ),
    )
    argument_parser.add_argument(
        '--output-dir', default=None,
        help=(
            'Output directory for CSV files when multiple venues match. '
            'Each venue gets its own file (e.g. ispass.csv, iiswc.csv). '
            'Default: current directory'
        ),
    )
    argument_parser.add_argument(
        '--fields',
        default=",".join(DEFAULT_FIELDS),
        help=(
            'Comma-separated list of fields to output. '
            f'Default: {", ".join(DEFAULT_FIELDS)}. '
            'Use "all" for all fields. '
            f'Available: {", ".join(ALL_FIELDS)}'
        ),
    )
    argument_parser.add_argument(
        '--limit', type=int, default=None,
        help='Maximum number of matched records to collect (default: no limit). Useful for testing.',
    )
    return argument_parser

def main() -> None:
    """CLI entry point."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    argument_parser = _build_argument_parser()
    args = argument_parser.parse_args()

    if not os.path.exists(args.xml):
        logger.error("XML file not found: %s", args.xml)
        sys.exit(1)

    selected_fields = None
    if args.fields:
        selected_fields = [field.strip() for field in args.fields.split(',')]

    parse_dblp(
        xml_path=args.xml,
        venue_pattern=args.venue,
        output_path=args.output,
        output_dir=args.output_dir,
        fields=selected_fields,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
