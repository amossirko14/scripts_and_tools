#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
A very simple script used for crawling dev jobs on hr.tencent.com/social.php.
Note that the data is extracted by parsing the html tags and attrs, which may change.
Also, more fields might need to be added to the headers should the crawling fail.
"""

import codecs
import re
import time

import requests
from bs4 import BeautifulSoup


def do_crawl(location="", job_type="", keyword=""):
    domain = "http://hr.tencent.com/"
    # print(location, job_type, keyword)
    url_home = domain + "/social.php"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36",

    }
    r = requests.get(url_home, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")
    # print(r.text)
    search_op = soup.find("div", attrs={"id": "socia_search", })
    # print(search_op)
    ops = search_op.find_all("div", attrs={"class": "options"})
    # print(ops)
    lid_ops = ops[0].find_all("div", attrs={"class": "option"})
    tid_ops = ops[1].find_all("div", attrs={"class": "option"})

    lids, tids = [], []
    for e in lid_ops:
        id, loc = e.get("val"), e.text.strip()  # .decode("utf-8")
        if id and loc:
            lids.append((id, loc))

    tids = []
    for e in tid_ops:
        id, jtype = e.get("val"), e.text.strip()
        if id and loc:
            tids.append((id, jtype))

    lid, tid = "", ""
    if location:
        for e in lids:
            if location in e[1]:
                lid = e[0]
                break
    if job_type:
        for e in tids:
            if job_type in e[1]:
                tid = e[0]
                break

    print(lid, tid)

    url_fmt = domain + "/position.php?keywords=%s&lid=%s&tid=%s&start=%d#a"
    url_begin = url_fmt % (keyword, lid, tid, 0)
    # print(url_begin)
    r = requests.get(url_begin, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")

    cnt = int(soup.find("span", attrs={"class": "lightblue total"}).text.strip() or 0)
    print("total {}".format(cnt))

    step = 10
    with codecs.open("tx_dev_jobs.txt", "a+", encoding="utf-8") as f:
        for index in range(0, cnt, step):
            print("current page index: %d" % ((index // step) + 1,))
            url = url_fmt % (keyword, lid, tid, index)
            r = requests.get(url, headers=headers)
            soup = BeautifulSoup(r.text, "lxml")
            items = soup.find_all("td", attrs={"class": "l square"})
            for item in items:
                title = item.text.strip()
                print("\t" + title)
                ref = item.find("a").get("href")
                item_url = domain + ref
                item_r = requests.get(item_url, headers=headers)
                item_soup = BeautifulSoup(item_r.text, "lxml")
                job_info = item_soup.find_all("ul", attrs={"class": "squareli"})
                job_duty = "\n\t".join([re.sub(ur"^\d、(.*?)", ur"\1", e.text.strip()) for e in job_info[0]])
                job_skill = "\n\t".join([re.sub(ur"^\d、(.*?)", ur"\1", e.text.strip()) for e in job_info[1]])

                # print(title)
                # print(job_duty)
                # print(job_skill)

                f.write(title + "\n")
                f.write(item_url + "\n")
                f.write(u"职责\n\t")
                f.write(job_duty + "\n")
                f.write(u"要求\n\t")
                f.write(job_skill + "\n\n")
            time.sleep(3)


if __name__ == '__main__':
    do_crawl(u"深圳", u"技术", "python")  # u"开发")
