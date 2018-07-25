#!/usr/bin/env python
# -*- coding:utf-8 -*-

import datetime
import getopt
import heapq
import json
import re
import sys

import requests


def pretty_print_dict(d):
	print(json.dumps(d, indent=4, ensure_ascii=False, encoding='utf-8'))


def do_crawl(keyword, page_cnt=10, result_cnt=10):
	if not keyword or not isinstance(keyword, str):
		return []

	page_cnt = (isinstance(page_cnt, int) and page_cnt > 0 and page_cnt) or 10
	result_cnt = (isinstance(result_cnt, int) and result_cnt > 0 and result_cnt) or 10
	today = datetime.date.today()

	# make fake headers
	headers = {
		"content-encoding": 'gzip, deflate, br',
		"content-language": "zh-CN,zh;q=0.8,en;q=0.6",
		"content-type": "application/json;charset=UTF-8",
		"Connection": "keep-alive",
		"Accept-Encoding": "gzip, deflate",
		"Accept": "*/*",
		"user-agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.86 Safari/537.36",
		"Referer": "https://www.taobao.com",  # might change
		# more header fields(cookies etc) might need to be added
	}

	# make fake request params
	req_params = {
		"imgfile": "",
		"commend": "all",
		"ssid": "s5-e",
		"search_type": "item",
		"sourceId": "tb.index",
		"ie": "utf8",
		"spm": "a21bo.50862.201856-taobao-item.1",
		"initiative_id": "tbindexz_%04d%02d%02d" % (today.year, today.month, today.day),
		"bcoffset": 4,
		"ntoffset": 4,
		"p4ppushleft": "6,48",
		"q": keyword,
		"s": "%d",  # 44 * pageNo, pageNo=0,1,2,...
	}

	params = "&".join("%s=%s" % (k, str(v)) for k, v in req_params.items())
	url_fmt = "https://s.taobao.com/search?" + params

	sellers_d = {}

	client = requests.session()
	client.get("https://www.taobao.com")
	client.headers.update(headers)

	for i in range(page_cnt):
		url = url_fmt % (i * 44,)
		if i > 0:
			client.headers.update({"Referer": url_fmt % ((i - 1) * 44,)})

		# add retry times?
		r = client.get(url, timeout=10)
		result = re.search(r'g_page_config = (.*?);\n', r.text)
		if not result:
			continue
		g_page_config = json.loads(result.group(1))
		auctions = g_page_config.get('mods', {}).get('itemlist', {}).get('data', {}).get('auctions', {})

		# pretty_print_dict(auctions)
		for e in auctions:
			seller_id, seller_nick, view_sales = e.get("user_id", {}), e.get("nick", {}), e.get("view_sales", {})
			if not seller_nick or not seller_id or not view_sales:
				continue

			re_sell_cnt = re.search(ur"(.*?)人付款", view_sales)
			if not re_sell_cnt or not re_sell_cnt.group(1).isdigit():
				continue

			sell_cnt = int(re_sell_cnt.group(1))
			if seller_nick in sellers_d.keys():
				sellers_d[seller_nick] += sell_cnt
			else:
				sellers_d[seller_nick] = sell_cnt

			# time.sleep(1) # lower down frequency

	results = heapq.nlargest(result_cnt, sellers_d.items(), key=lambda x: x[1])
	print("\n".join(["%s %d" % (e[0], e[1]) for e in results]))
	if len(results) == 0:
		print("oops, no results found for %s. Maybe try something more specific?" % (keyword,))
	return results


def usage(default_page_cnt, default_result_cnt):
	print(
		"""
	Usage:
		run with the command python %s -k keyword -p page_cnt -r result_cnt
		-k  the keyword you want to search in taobao.com, keyword MUST be non-empty
		-p  [optional] search the first p pages, default value is %d
		-r  [optional] return the top r results with the largest sales volume, default value is %d
	Example:
		python %s -k iphone6sp -p 2 -r 10
		python %s -k 苹果
		python %s -k "apple iphone"

		""" % (__file__, default_page_cnt, default_result_cnt, __file__, __file__, __file__))


if __name__ == '__main__':
	default_page_cnt, default_result_cnt = 10, 10

	try:
		opts, args = getopt.getopt(sys.argv[1:], "hk:p:r:", ["help", "-help"])
	except getopt.GetoptError:
		usage(default_page_cnt, default_result_cnt)
		sys.exit(2)

	keyword, page_cnt, result_cnt = "", default_page_cnt, default_result_cnt
	for op, val in opts:
		if op in ('-h', '-help', '--help'):
			usage(default_page_cnt, default_result_cnt)
			sys.exit(0)
		elif op in ('-k',):
			keyword = val
		elif op in ('-p',):
			if val.isdigit():
				page_cnt = int(val)
		elif op in ('-r',):
			if val.isdigit():
				result_cnt = int(val)
		else:
			usage(default_page_cnt, default_result_cnt)
			sys.exit(2)

	if not keyword:
		usage(default_page_cnt, default_result_cnt)
		sys.exit(2)

	# print(keyword, page_cnt, result_cnt)
	do_crawl(keyword, page_cnt, result_cnt)
