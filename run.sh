#!/usr/bin/env bash
parallel -j 10 ./dblp_parser.py --fields type,year,key,title,doi {} :::: extract_list
