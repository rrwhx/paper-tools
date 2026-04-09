#!/usr/bin/env python3
"""Extract papers from DBLP XML database by venue and export to CSV."""

import argparse
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

DEFAULT_FIELDS = ['year', 'title', 'doi']

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


def _sort_by_year(records: list[dict[str, str]]) -> None:
    """Sort records in-place by year (numerically)."""
    records.sort(key=lambda record: int(record['year']) if record['year'].isdigit() else 0)


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


def _scan_records(xml_path: str, venue_name: str) -> list[dict[str, str]]:
    """Iterate over the DBLP XML and collect records matching the venue."""
    xml_parser = _create_xml_parser(xml_path)
    context = ET.iterparse(xml_path, events=('end',), parser=xml_parser)

    results: list[dict[str, str]] = []
    total_count = 0
    matched_count = 0

    for _event, element in context:
        if element.tag not in PAPER_TAGS:
            continue

        total_count += 1
        if total_count % PROGRESS_INTERVAL == 0:
            logger.info("Processed %d records ...", total_count)

        record_key = element.get('key', '')
        if record_key.startswith(venue_name):
            results.append(_extract_record(element, venue_name))
            matched_count += 1

        element.clear()

    logger.info("Scanned %d records, matched %d for venue '%s'.", total_count, matched_count, venue_name)
    return results


def write_csv(records: list[dict[str, str]], output_path: str, fieldnames: list[str]) -> None:
    """Write records to a CSV file."""
    with open(output_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(records)
    logger.info("Wrote %d records to %s", len(records), output_path)


def parse_dblp(
    xml_path: str,
    venue_name: str,
    output_csv: str,
    fields: Optional[list[str]] = None,
) -> None:
    """Main pipeline: scan DBLP XML for a venue, sort by year, and write CSV."""
    logger.info("Processing %s for venue '%s' ...", xml_path, venue_name)

    records = _scan_records(xml_path, venue_name)
    _sort_by_year(records)

    fieldnames = _validate_fields(fields)
    write_csv(records, output_csv, fieldnames)

    logger.info("Done.")


def _build_argument_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    argument_parser = argparse.ArgumentParser(
        description='Extract papers from DBLP XML by venue.',
    )
    argument_parser.add_argument(
        'venue',
        help='DBLP key prefix for the venue (e.g., "conf/ispass", "journals/taco")',
    )
    argument_parser.add_argument(
        '--xml', default='dblp.xml',
        help='Path to dblp.xml (default: dblp.xml)',
    )
    argument_parser.add_argument(
        '--output', default=None,
        help='Output CSV filename (default: derived from venue, e.g. conf/ispass → ispass.csv)',
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

    if args.output is None:
        # "conf/ispass" → "ispass.csv", "journals/taco" → "taco.csv"
        venue_basename = args.venue.rstrip('/').split('/')[-1]
        args.output = f"{venue_basename}.csv"

    selected_fields = None
    if args.fields:
        selected_fields = [field.strip() for field in args.fields.split(',')]

    parse_dblp(args.xml, args.venue, args.output, selected_fields)


if __name__ == "__main__":
    main()
