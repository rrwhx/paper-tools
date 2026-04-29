#!/usr/bin/env python3
"""Stream DBLP XML records matching a venue pattern directly to CSV.

Unlike dblp_parser.py which groups records by venue and sorts them,
this tool writes matched records immediately in discovery order —
ideal for large-scale exports where you don't need per-venue files.
"""

import argparse
import csv
import logging
import os
import re
import sys
import xml.etree.ElementTree as ET
from typing import Optional

from dblp_parser import (
    ALL_FIELDS,
    DEFAULT_FIELDS,
    PAPER_TAGS,
    PROGRESS_INTERVAL,
    _create_xml_parser,
    _extract_record,
    _extract_venue_from_key,
)

logger = logging.getLogger(__name__)

BATCH_SIZE = 1000


def stream_dblp(
    xml_path: str,
    venue_pattern: str,
    output_csv: str,
    fields: Optional[list[str]] = None,
    limit: Optional[int] = None,
) -> None:
    """Scan DBLP XML and stream matching records to a CSV file."""
    fieldnames = fields or DEFAULT_FIELDS
    invalid = [f for f in fieldnames if f not in ALL_FIELDS]
    if invalid:
        logger.warning("Ignoring unknown fields: %s", ", ".join(invalid))
        fieldnames = [f for f in fieldnames if f in ALL_FIELDS]
    if not fieldnames:
        fieldnames = list(DEFAULT_FIELDS)

    try:
        compiled_pattern = re.compile(venue_pattern)
        is_regex = True
    except re.error:
        logger.warning("Invalid regex '%s', treating as literal prefix.", venue_pattern)
        compiled_pattern = None
        is_regex = False

    xml_parser = _create_xml_parser(xml_path)
    context = ET.iterparse(xml_path, events=('end',), parser=xml_parser)

    total_count = 0
    matched_count = 0
    batch: list[dict[str, str]] = []

    with open(output_csv, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for _event, element in context:
            if element.tag not in PAPER_TAGS:
                continue

            total_count += 1
            record_key = element.get('key', '')
            venue_key = _extract_venue_from_key(record_key)

            is_match = False
            if is_regex and compiled_pattern is not None:
                is_match = compiled_pattern.fullmatch(venue_key) is not None
            else:
                is_match = record_key.startswith(venue_pattern)

            if is_match:
                batch.append(_extract_record(element, venue_key))
                matched_count += 1

            if total_count % PROGRESS_INTERVAL == 0:
                logger.info("Processed %d records, matched %d ...", total_count, matched_count)

                if len(batch) >= BATCH_SIZE:
                    writer.writerows(batch)
                    batch.clear()

                if limit is not None and matched_count >= limit:
                    logger.info("Reached limit of %d records, stopping.", limit)
                    element.clear()
                    break

            element.clear()

        if batch:
            writer.writerows(batch)

    logger.info(
        "Done. Scanned %d records, wrote %d to %s",
        total_count, matched_count, output_csv,
    )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S',
    )

    argument_parser = argparse.ArgumentParser(
        description='Stream DBLP records matching a venue pattern to a single CSV file.',
    )
    argument_parser.add_argument(
        'venue',
        help=(
            'Venue key prefix or regex. '
            'Examples: "conf/ispass", "conf/(ispass|iiswc)", "journals/t.*"'
        ),
    )
    argument_parser.add_argument(
        '--xml', default='dblp.xml',
        help='Path to dblp.xml (default: dblp.xml)',
    )
    argument_parser.add_argument(
        '-o', '--output', default=None,
        help='Output CSV file (default: <venue_basename>.csv)',
    )
    argument_parser.add_argument(
        '--fields',
        default=",".join(DEFAULT_FIELDS),
        help=(
            'Comma-separated list of fields. '
            f'Default: {", ".join(DEFAULT_FIELDS)}. '
            'Use "all" for all fields. '
            f'Available: {", ".join(ALL_FIELDS)}'
        ),
    )
    argument_parser.add_argument(
        '--limit', type=int, default=None,
        help='Stop after N matched records (for testing).',
    )

    args = argument_parser.parse_args()

    if not os.path.exists(args.xml):
        logger.error("XML file not found: %s", args.xml)
        sys.exit(1)

    if args.output is None:
        venue_basename = args.venue.strip('/').replace('/', '_')
        # Sanitize regex chars for filename
        venue_basename = re.sub(r'[^a-zA-Z0-9_\-]', '', venue_basename)
        args.output = (venue_basename or 'output') + '.csv'

    if args.fields == 'all':
        selected_fields = list(ALL_FIELDS)
    else:
        selected_fields = [f.strip() for f in args.fields.split(',')]

    stream_dblp(
        xml_path=args.xml,
        venue_pattern=args.venue,
        output_csv=args.output,
        fields=selected_fields,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
