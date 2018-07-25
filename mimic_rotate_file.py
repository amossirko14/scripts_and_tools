#!/usr/bin/env python
# -*- coding:utf-8 -*-

import time
import string
import random

import logging
from logging.handlers import RotatingFileHandler


def create_rotating_log(log_file, times, keywords):
	logger = logging.getLogger("test_rotating")
	logger.setLevel(logging.INFO)

	# add a rotating handler
	handler = RotatingFileHandler(
		log_file,
		maxBytes=10000,
		backupCount=100)
	logger.addHandler(handler)

	cnt = 0
	for i in range(times):
		rand_str = ''.join(random.choice(string.ascii_letters) for _ in range(30))
		for index, val in enumerate(keywords):
			if i % (21 + index) == 0:
				rand_str = val + str(random.randint(0, 4)) + rand_str
				cnt += 1
				print(rand_str, cnt)
				break

		logger.info(rand_str)
		time.sleep(0.01)


if __name__ == '__main__':
	log_file, times = "tmpfile.log", 2000
	patterns = ["bingo", "yummy", "oops", "authenticate_fail", "serve_update_time(.*?)get_white_list_err"]
	create_rotating_log(log_file, times, patterns)
