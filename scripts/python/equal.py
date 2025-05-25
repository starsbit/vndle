#!/usr/bin/env python3
import json, difflib, pathlib, sys

if len(sys.argv) != 3:
    sys.exit("Usage: diff_json.py FILE1.json FILE2.json")

file1, file2 = map(pathlib.Path, sys.argv[1:])

# Canonicalise → pretty, sorted, consistent spacing
a_lines = json.dumps(json.load(file1.open()),
                     indent=2, sort_keys=True).splitlines()
b_lines = json.dumps(json.load(file2.open()),
                     indent=2, sort_keys=True).splitlines()

for line in difflib.unified_diff(
        a_lines, b_lines,
        fromfile=file1.name, tofile=file2.name,
        lineterm=""
):
    print(line)
