#!/bin/bash

###################################################################################
#This script is a demo for loading mysql data into redis singleton/shardings,
#with each table's row mapping to redis's hash, row's primary key to the hash key
#
#There's a possibility that one wants to load mysql data into redis cluster rather
#than into redis singleton/shardings, if so, based on this script, one might just
#go one step further to achieve this: loading the data from the singleton/shardings
#to the cluster by using tools like: github.com/vipshop/redis-migrate-tool.
###################################################################################


list_ip=(127.0.0.1 127.0.0.1 127.0.0.1 127.0.0.1)
list_port=(6400 6401 6402 6403 6404)
list_db_name=(7  7  7  7  7 )

arr_len=${#list_ip[@]}

cur_min=0
cur_max=2147483647 #2**31 -1


#redis-cli initial settings before embarking on the migration
#caution! this should be placed before where the sql_loader() function executes
for((index=0;index<$arr_len;++index))
do
    # caution when executing flushall
	# echo "flushall" | redis-cli  -h ${list_ip[$index]} -p ${list_port[$index]}

	#fix the error: MISCONF Redis is configured to save RDB snapshots, but is currently not able to persist on disk.
	#               Commands that may modify the data set are disabled. Please check Redis logs for details about the error.
	echo "config set stop-writes-on-bgsave-error no" | redis-cli  -h ${list_ip[$index]} -p ${list_port[$index]}
done

start_time=`date +%s`

#this function loads mysql data into redis, altogether five parameters.
#para1 dbnum, should be a number
#para2 dbname
#para3 tablename, string format like "getuserinfo_%d_online"
#para4 userno(userid) name
#para5 sql field part, as the last param

function sql_loader()
{
	tmpfile="tmp"
	p1=$1; p2=$2; p3=$3; p4=$4
	shift 4; p5=$*

	for((modval=0;modval<$arr_len;++modval))
	do
		for((tableindex=0;tableindex<$p1;++tableindex))
		do
			SQL_NAME=${tmpfile}${i}.txt
			table_name=`printf $p3 $tableindex `
			cond=`printf " FROM %s where %s >= %d and %s %% %d = %d ) AS t"  $table_name $p4 $cur_min  $p4 ${arr_len}  ${modval}`
			sql_total=${p5}${cond}

			echo $sql_total > $SQL_NAME
			mysql -hmysqlip -uroot -pyourpass $p2 --skip-column-names --raw < $SQL_NAME | redis-cli  -h ${list_ip[$modval]} -p ${list_port[$modval]} -n ${list_db_name[$modval]}  --pipe
			rm -f $SQL_NAME
			#usleep 200000
		done
		#sleep 1
	done
}

# fields that needs to be put into redis:
# key format: u{uid}
# usernewno, usrname, nickname, shapsw, mail, mobile,
# registertype, registerdate,

dbnum=500
sql="
    select concat(
    '*18\r\n',
    '$', length(redis_cmd), '\r\n', redis_cmd,  '\r\n',
    '$', length(redis_key), '\r\n', redis_key,  '\r\n',
    '$', length(f_newno),   '\r\n', f_newno,    '\r\n',
    '$', length(v_newno),   '\r\n', v_newno,    '\r\n',
    '$', length(f_usrname), '\r\n', f_usrname,  '\r\n',
    '$', length(v_usrname), '\r\n', v_usrname,  '\r\n',
    '$', length(f_nickname),'\r\n', f_nickname, '\r\n',
    '$', length(v_nickname),'\r\n', v_nickname, '\r\n',
    '$', length(f_shapsw),  '\r\n', f_shapsw,   '\r\n',
    '$', length(v_shapsw),  '\r\n', v_shapsw,   '\r\n',
    '$', length(f_mail),    '\r\n', f_mail,     '\r\n',
    '$', length(v_mail),    '\r\n', v_mail,     '\r\n',
    '$', length(f_mobile),  '\r\n', f_mobile,   '\r\n',
    '$', length(v_mobile),  '\r\n', v_mobile,   '\r\n',
    '$', length(f_regtype), '\r\n', f_regtype,  '\r\n',
    '$', length(v_regtype), '\r\n', v_regtype,  '\r\n',
    '$', length(f_regdate), '\r\n', f_regdate,  '\r\n',
    '$', length(v_regdate), '\r\n', v_regdate,  '\r'
    ) from (
    select
    'hmset' as redis_cmd, concat('u{', userid, '}') as redis_key,
    'usernewno'     as f_newno,     usernewno   as v_newno,
    'usrname'       as f_usrname,   usrname     as v_usrname,
    'nickname'      as f_nickname,  nickname    as v_nickname,
    'shapsw'        as f_shapsw,    shapsw      as v_shapsw,
    'mail'          as f_mail,      mail        as v_mail,
    'mobile'        as f_mobile,    mobile      as v_mobile,
    'registertype'  as f_regtype,   registertype    as v_regtype,
    'registerdate'  as f_regdate,   registerdate    as v_regdate
    "

sql_loader $dbnum "dbname" "basicInfo%d" "userid" ${sql}

end_time=`date +%s`
delta=`expr $end_time - $start_time`

echo "$start_time $end_time $delta"

#when the migration process stops, we need to restore the original redis settings.
#this should be placed at the end
for((index=0;index<$arr_len;++index))
do
	echo "config set stop-writes-on-bgsave-error yes" | redis-cli  -h ${list_ip[$index]} -p ${list_port[$index]}
done