#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import re
import json
from functools import reduce


def filter_gen(file_name):
	lines = [line.strip() for line in open(file_name, 'r')]
	cnt = 0
	pref = "filter_%08d"
	d = {}
	for line in lines:
		code_file, err_str = [e for e in line.split(':', 1) if e]
		pattern = '(.*?)'.join(re.findall(r'"([^"]*)"', err_str)).strip()
		pattern = '(.*?)'.join(re.split('%v|%s|%d', pattern))
		pattern = reduce(lambda x, y: x.replace(y, '(.*?)'), ['[', ']'], pattern)

		def squeeze(char, s):
			while char * 2 in s:
				s = s.replace(char * 2, char)
			return s

		pattern = squeeze('(.*?)', pattern)

		if pattern:
			key = pref % (cnt,)
			d[key] = {
				"file": code_file,
				"severity": "critical",
				"pattern": pattern,
				"ignore": 0
			}
			cnt += 1

	with open(file_name + '.json', 'w') as f:
		f.write(json.dumps(d, indent=4, sort_keys=True))


def usage():
	print("""
	Generate filter rules by using: 
		python %s file_name
	file_name contains the code lines that might have error messages.
	the code lines are roughly extracted by entering the project folder and doing `grep -r "Error(" * > file_name` 
	""") % (sys.argv[0],)

if __name__ == '__main__':
	try:
		filter_gen(sys.argv[1])
	except:
		usage()
		exit(1)


