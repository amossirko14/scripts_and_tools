#!/usr/bin/env python
# -*- coding: utf-8 -*-

import datetime
import os

import MySQLdb

from fnvhash import fnv1a_32

"""
this script is used for selectively loading data from mysql to redis shardings.
the major steps are:
1. select mysql's data into tmp files
2. read these files, and re-sort these files using fnv1a_32 and save them into redis's mass insert format
3. feed the generated files in step 2 to redis-cli with --pipe
"""

# configs
redis_conf = (("127.0.0.1", "6400", "0"), ("127.0.0.1", "6401", "0"), ("127.0.0.1", "6402", "0"),)
redis_num = len(redis_conf)

host = "localhost"
user = "root"
passwd = "yourpass"
port = 3306
unix_socket = "/tmp/mysql.sock"
file_path = "/usr/local/migratedata/exported/"  # mysql:mysql
select_into_fmt = " select %s into outfile %s from %s "


def remove_file_if_exists(f):
    if os.path.isfile(f):
        os.remove(f)


# key-value
def gen_insert_item(key_fmt, elts):
    # skipped param checking
    key = key_fmt % (str(elts[0]),)
    return "*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n" % (len("set"), "set", len(key), key, len(elts[1]), elts[1])


def add_quote(src):
    return "\"" + src + "\""


def do_n2i_load(db_name, table_fmt, table_num, fields, key_fmt, where_cond=""):
    conn = MySQLdb.connect(host=host,
                           user=user,
                           passwd=passwd,
                           db=db_name,
                           port=port,
                           unix_socket=unix_socket)
    begin = datetime.datetime.today()
    rows_affected = 0
    with conn:
        fields = ",".join(str(e) for e in fields)
        cur = conn.cursor()
        for table_index in range(table_num):
            table = table_fmt % (table_index,)
            # print(table)
            tmpf = file_path + "tmp" + table
            remove_file_if_exists(tmpf)
            sql = (select_into_fmt % (fields, add_quote(tmpf), table)) + where_cond
            # print("sql:", sql)
            rows_affected += cur.execute(sql)
            # print("rows affected:", rows_affected)

        for redis_index in range(redis_num):
            remove_file_if_exists(file_path + db_name + str(redis_index))

        for table_index in range(table_num):
            table = table_fmt % (table_index,)
            tmpf = file_path + "tmp" + table
            with open(tmpf, "r") as f:
                for line in f.readlines():
                    elts = (line.split("\n")[0]).split("\t")
                    # print("elts:", elts)
                    shard = fnv1a_32(elts[0]) % redis_num
                    redisf_name = file_path + db_name + str(shard)
                    with open(redisf_name, "ab+") as redis_file:
                        insert_item = gen_insert_item(key_fmt, elts)
                        if len(insert_item) != 0:
                            redis_file.write(insert_item)

        for redis_index in range(redis_num):
            redisf_name = file_path + db_name + str(redis_index)
            cmd = "cat " + redisf_name + " | redis-cli " + " -h " + redis_conf[redis_index][0] + \
                  " -p " + redis_conf[redis_index][1] + " -n " + redis_conf[redis_index][2] + " --pipe"
            fp = os.popen(cmd)
            # print(fp.read())

        end = datetime.datetime.today()
        delta = end - begin
        print("db:", db_name, "table_fmt:", table_fmt, "table_num:", table_num, "fields:", fields, "key_fmt:", key_fmt)
        print("begin:", begin, "end:", end, "delta:", delta.seconds + delta.days * 86400)
        print("rows_affected:", rows_affected)


do_n2i_load("LoginEmail_Db", "Email2UserNo%d_Tbl", 200, ["UserEmail", "UserNo"], "N2I_4_%s")

do_n2i_load("SecureMobile_Db", "MobileNo2UserID%d_Tbl", 200, ["MobileNo", "UserID"], "N2I_5_%s")

do_n2i_load("reg_dispatcher", "relative%d", 1000, ["usrname", "usrno"], "N2I_0_%s")

do_n2i_load("reg_usrinfor", "onlineinfor%d", 500, ["usernewno", "usrno"], "N2I_1_%s", " where usernewno != 0 ")
