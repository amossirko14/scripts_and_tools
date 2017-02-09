#!/usr/bin/env python
# -*- coding: utf-8 -*-

import inspect
import os
from datetime import datetime

import MySQLdb

db_num = 500
cur_limit = 0
target_db_name = "yourdbname"
desttblprefix = "basicInfo%d"
destfolder = "/usr/local/migratedata/exportednew/"

host = "localhost"
user = "root"
passwd = "yourpass"
port = 3306


def args_and_values():
    caller = inspect.stack()[1][0]
    args, _, _, values = inspect.getargvalues(caller)
    return ["%s:%s" % (str(i), str(values[i])) for i in args]


def do_remove_file(f):
    if os.path.isfile(f):
        os.remove(f)


def get_now_secs():
    dt = datetime.now()
    print("current_min:" + str(dt.minute), "current sec:" + str(dt.second), "current microsec:" + str(dt.microsecond))
    return dt


def calc_elapsed_time(dt_1, spec):
    dt_2 = get_now_secs()
    dt = dt_2 - dt_1
    print("elapsed time for %s: %d seconds" % (spec, dt.seconds + 86400 * dt.days))


def do_chown_dest_folder(dest_folder):
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
    cmd = "chown -R mysql:mysql " + dest_folder
    os.popen(cmd)
    cmd = "rm -f " + dest_folder + "/*"
    os.popen(cmd)


def handle_text(src_num, src_pref, dest_num, dest_pref):
    if src_num <= 0 or dest_num <= 0 or len(src_pref) == 0 or len(dest_pref) == 0:
        print("invalid params: %d %s %d %s" % (src_num, src_pref, dest_num, dest_pref))
        return

    for index in range(src_num):
        src_f = src_pref % (str(index),)
        if not os.path.isfile(src_f):
            print("src file:%s does not exist" % (src_f,))
            return

        elts = [tuple(line.rstrip('\n').split('\t')) for line in open(src_f, 'r')]
        divided_list = []
        for i in range(dest_num):
            divided_list.append([])

        for item in elts:
            divided_list[int(item[0]) % dest_num].append(item)

        for i in range(dest_num):
            target_f = dest_pref % (str(i),)
            with open(target_f, 'ab+') as f:
                for item in divided_list[i]:
                    f.write('\t'.join([e for e in item]) + '\n')


def do_select_ext(src_db_num, db_name, table_pref, uid, items, items_hex, target_db_num):
    print(args_and_values())
    start_time = get_now_secs()
    items.insert(0, uid)
    select_fields = join_str(join1(items, " %s ", ","), join1(items_hex, " hex(%s) ", ","), " , ")
    # print(select_fields)

    rows_affected, tmp = 0, "tmp"
    pref, pref_tmp = destfolder + table_pref, destfolder + table_pref + tmp

    for index in range(target_db_num):
        do_remove_file(destfolder + table_pref % (str(index),))

    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db_name, port=port,
                           unix_socket="/tmp/mysql.sock", local_infile=True)
    with conn:
        cur = conn.cursor()
        for index in range(src_db_num):
            tbl = table_pref % (str(index),)
            output = destfolder + tbl if src_db_num == target_db_num else destfolder + tbl + tmp
            do_remove_file(output)
            sql = "select %s into outfile \"%s\" from %s where %s > %s" % \
                  (select_fields, output, tbl, str(uid), str(cur_limit))
            # print(sql)
            rows_affected += cur.execute(sql)

    calc_elapsed_time(start_time, inspect.stack()[0][3])
    if src_db_num != target_db_num:
        handle_text(src_db_num, pref_tmp, target_db_num, pref)

    calc_elapsed_time(start_time, inspect.stack()[0][3])
    print("total affected row:%d" % (rows_affected,))


# five phases for loading the data
# phase 1:create temporary file
# phase 2:(optional) drop all indices from temporary table to speed things up
# phase 3: load src file into temporary table
# phase 4: copy the data using : on duplicate key update
# phase 5: remove temporary table
def do_load_ext(db_num, dest_db_name, src_tbl_pref, dest_tbl_pref, uid, items, items_hex):
    print(args_and_values())
    start_time = get_now_secs()
    items.insert(0, uid)
    conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=dest_db_name, port=port,
                           unix_socket="/tmp/mysql.sock", local_infile=True)
    with conn:
        cur = conn.cursor()
        for index in range(db_num):
            src_f = destfolder + src_tbl_pref % (str(index),)
            assert os.path.isfile(src_f) and os.path.exists(src_f), "file:%s non-existent" % (src_f,)

            dest_tbl, tmp_tbl = dest_tbl_pref % (str(index),), dest_tbl_pref % (str(index),) + "tmp"

            sql1 = "create temporary table if not exists %s like %s " % (tmp_tbl, dest_tbl)
            # print(sql1)
            cur.execute(sql1)

            # phase 2 skipped

            combine1 = join_str(join1(items, " %s ", ","), join1(items_hex, " %s ", ","), " , ")
            sql3 = " load data infile \"%s\" into table %s ( %s ) " % (src_f, tmp_tbl, combine1)
            # print(sql3)
            cur.execute(sql3)
            # cur.commit()

            # insert into basicInfo1 select * from basicInfo1tmp on duplicate key update nickname =  unhex(basicInfo1tmp.nickname)

            tmp_pref = tmp_tbl + "."
            no_hex_fmt = " %s = " + tmp_pref + "%s"
            with_hex_fmt = " %s = unhex(" + tmp_pref + "%s)"
            combine2 = join_str(join2(items, no_hex_fmt, " , "), join2(items_hex, with_hex_fmt, " , "), " , ")

            # print(combine2)
            sql4 = "insert into %s select * from %s on duplicate key update %s" % (dest_tbl, tmp_tbl, combine2)
            # print(sql4)
            cur.execute(sql4)

            sql5 = "drop temporary table %s" % (tmp_tbl,)
            # print(sql5)
            cur.execute(sql5)

    calc_elapsed_time(start_time, inspect.stack()[0][3])


def migrate_data():
    do_chown_dest_folder(destfolder)
    do_migrate()


def do_migrate():
    start_time = get_now_secs()
    (dest_db, dest_db_num, dest_tb_pref, dest_uid) = ("yourdbname", 500, "basicInfo%s", "userid")
    # # load from onlineinfor%s, load fields: usrno [usernewno, shapsw,] [usrname, nickname]
    # do_select_ext(500, "reg_usrinfor", "onlineinfor%s", "usrno", ["usernewno", "shapsw"], ["usrname", "nickname"], dest_db_num)
    # do_load_ext(dest_db_num, dest_db, "onlineinfor%s", dest_tb_pref, dest_uid, ["usernewno", "shapsw"], ["usrname", "nickname"])
    # # do_select_ext(2, "reg_usrinfor", "onlineinfor%s", "usrno", ["usernewno", "shapsw", "nickname"], ["usrname",], dest_db_num)
    # # do_load_ext(dest_db_num, dest_db, "onlineinfor%s", dest_tb_pref, dest_uid, ["usernewno", "shapsw", "nickname"], ["usrname", ])
    #
    #
    # # load from privateinfor%s, load fields: usrno [registerdate, registertype] []
    # do_select_ext(500, "reg_usrinfor", "privateinfor%s", "usrno", ["registerdate", "registertype"], [], dest_db_num)
    # do_load_ext(dest_db_num, dest_db, "privateinfor%s", dest_tb_pref, dest_uid, ["registerdate", "registertype"], [])
    #
    # # load from security_info_%s, load fields: userno, [idcardno, idcardno_status, truename_status, mailbox_status,]
    # # hex fields:[truename, question_X, answer_X]
    # select_sec_no_hex = ["idcardno", "idcardno_status", "truename_status", "mailbox_status"]
    # load_sec_no_hex   = ["idcardno", "idcardno_status", "truename_status", "mail_status"]
    # select_sec_hex    = ["truename", "question_1", "question_2", "question_3", "answer_1", "answer_2", "answer_3"]
    # load_sec_hex      = ["truename", "question1", "question2", "question3", "answer1", "answer2", "answer3"]
    # do_select_ext(500, "reg_usrinfor", "security_info_%s", "userno", select_sec_no_hex, select_sec_hex, dest_db_num)
    # do_load_ext(dest_db_num, dest_db, "security_info_%s", dest_tb_pref, dest_uid, load_sec_no_hex, load_sec_hex)

    # load from SecureMobile_Db fields: UserID, [MobileNo]
    # do_select_ext(200, "SecureMobile_Db", "MobileNo2UserID%s_Tbl", "UserID", ["MobileNo"], [], dest_db_num)
    # do_load_ext(dest_db_num, dest_db, "MobileNo2UserID%s_Tbl", dest_tb_pref, dest_uid, ["mobile"], [])

    # load from SecureMobile_Db UserID2MobileNo500_Tbl fields UserID, [MobileNo]
    do_select_ext(500, "SecureMobile_Db", "UserID2MobileNo%s_Tbl", "UserID", ["MobileNo"], [], dest_db_num)
    do_load_ext(dest_db_num, dest_db, "UserID2MobileNo%s_Tbl", dest_tb_pref, dest_uid, ["mobile"], [])

    # # load from LoginEmail_Db field: UserNo [UserEmail]
    # do_select_ext(200, "LoginEmail_Db", "Email2UserNo%s_Tbl", "UserNo", ["UserEmail"], [], dest_db_num)
    # do_load_ext(dest_db_num, dest_db, "Email2UserNo%s_Tbl", dest_tb_pref, dest_uid, ["mail"], [])

    # load from LoginEmail_Db  UserNo2Email199_Tbl field: UserNo [UserEmail]
    do_select_ext(500, "LoginEmail_Db", "UserNo2Email%s_Tbl", "UserNo", ["UserEmail"], [], dest_db_num)
    do_load_ext(dest_db_num, dest_db, "UserNo2Email%s_Tbl", dest_tb_pref, dest_uid, ["mail"], [])

    # output total elapsed time
    calc_elapsed_time(start_time, inspect.stack()[0][3])


def join_str(str1, str2, delim=" , "):
    return str1 if len(str2) == 0 else str1 + delim + str2


def join1(elts, pref_fmt, join_str):
    return join_str.join([pref_fmt % (e,) for e in elts])


def join2(elts, pref_fmt, join_str):
    return join_str.join([pref_fmt % (e, e) for e in elts])


if __name__ == "__main__":
    migrate_data()
