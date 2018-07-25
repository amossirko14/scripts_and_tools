#!/usr/bin/env python
# -*- coding:utf-8 -*-

import os
import time


def tail_file(file_name, keywords, ignores):
	current = open(file_name, "a+")
	current.seek(0, os.SEEK_END)
	cur_ino = os.fstat(current.fileno()).st_ino
	while True:
		while True:
			if not keywords:
				# do nothing, just let it run
				yield None
				time.sleep(10)
			line = current.readline()
			if not line:
				break
			if any(kw in line for kw in ignores):
				yield None
			elif any(kw in line for kw in keywords):
				yield line
			else:
				yield None

		try:
			if os.stat(file_name).st_ino != cur_ino:
				new = open(file_name, "a+")
				current.close()
				current = new
				current.seek(0, os.SEEK_END)
				cur_ino = os.fstat(current.fileno()).st_ino
				continue
			else:
				time.sleep(1)
		except:
			pass
		yield None
		time.sleep(0.001)


if __name__ == '__main__':
	cnt = 0
	log_file = 'tmpfile.log'
	patterns = ["bingo", "yummy", "oops"]
	ignore_list = ["bingo0", "yummy0"]
	for result in tail_file(log_file, patterns, ignore_list):
		if result:
			cnt += 1
			print(result.strip(), cnt)
