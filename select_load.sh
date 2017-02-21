#!/bin/bash


###################################################################################
# This script is a demo for selectively loading mysql data from one table to another
# table with changes like column names and character set by using two SQL commands:
# (1)select ... into outfile... (2)load data infile ... into table ...
# Attention should be paid to (1)select-into-outfile folder permission and
# (2)load-data-infile permission when loading from one host to another
###################################################################################


#commonfields
dbnum=500
curlimit=0
destdbname="yourdbname"
desttblprefix="basicInfo%d"
destfolder="/usr/local/migratedata/exported/"

if [ ! -d "$destfolder" ]; then echo "$destfolder not exists. exiting ...";	exit; fi

#p1: an array of strings. eg: ("hello") or ("hello" "world")
#p2: 
#	0: delim1="" 			delim2=", "
#	1: delim1=" hex("		delim2="), "
#	2: delim1=" unhex(" 	delim2="), "
#p3: isduplicate(1), default is 0
#p4: tblprefix
function make_param()
{
	#if [ $# -lt 2 ]; then echo "param < 10" ; return;  fi
	#echo "$FUNCNAME param num: $#"		#echo "$@"

	declare -a p1=("${!1}") ; p2=$2; p3=$3 ; p4=$4
	#echo "makeparam: ${p1[@]} $p2 $p3"

	items=""; comma=","

	delim1="";	delim2=""; needdup=0
	case "$p2" in 
		"1") delim1=" hex(";		delim2=") ";				;;
		"2") delim1=" unhex(";		delim2=") "; 				;;
		"3") delim1=" and ";		delim2=" != ''"; comma=" "	;;
		*)	;; #default
	esac

	dupdelim1=""; dupdelim2=""
	if [ "$p3" = "1" ]; then dupdelim1=" = values("; dupdelim2=") "; 
	elif [ "$p3" = "2" ]; then dupdelim1=" = "; dupdelim2=" "; fi 

	for i in "${p1[@]}"; 
	do
		if [ "$p3" = "1" ] || [ "$p3" = "2" ] ; then	items=${items}${i}${dupdelim1}${delim1}${p4}${i}${delim2}${dupdelim2}${comma}; 
		else						                    items=${items}${dupdelim1}${delim1}${i}${delim2}${dupdelim2}${comma} ; fi
	done
	result=""
	if [ ! -z "$items" ];	then	result=`echo "$items" | cut -d',' -f -${#p1[@]}`; fi
	echo "$result"
}



#p1:srcdbnum 	#p2:dbname 	#p3:tblprefix
#p4:uid		#p5:select items(skip uid) 
#p6:targetdbnum #p7:make param type
function do_select()
{
	p1=$1; p2=$2; p3=$3; p4=$4; 
	declare -a p5=("${!5}")
	p6=$6; p7=$7
	#echo "$p1 $p2 $p3 $p4 ${p5[@]} $6 $p7"

	andcond=""
	if [ "$p7" = "1" ]; then andcond=`make_param p5[@] "3" "0"` ; fi
	#echo "andcond: $andcond"
	if [ -z "$p6" ]; then echo "$FUNCNAME param error: p6 is lost !!!"; return; fi
	if [ -z "$p7" ]; then p7="0"; fi

	combine1=`make_param p5[@] "$p7" "0" `
	if [ -z "$combine1" ]; then echo "$FUNCNAME :empty combine "; return; fi
	#echo "combine: $combine1"

	for((index=0;index<$p6;++index)); do rm -f ${destfolder}`printf "$p3" $index` ; done

	for((index=0;index<$p1;++index))
	do
		tbl=`printf "$p3" $index`
		output=${destfolder}${tbl}
		if [ "$p1" != "$p6" ]; then	output=${destfolder}${tbl}tmp; fi
		rm -f $output
		sql="select ${p4}, ${combine1}  into outfile \"$output\" from $tbl where $p4 > $curlimit $andcond"
		#echo "$sql"
		echo "$sql" | mysql -hlocalhost -uroot -pyourpass $2 -N
	done	

	if [ "$p1" != "$p6" ];
	then 
		./go_textproc -srcNum=$p1 -srcPref=${destfolder}${p3}tmp  -destNum=$p6 -destPref=${destfolder}${p3}
	fi

}

#p1:dbnum		#p2:destdbname		#p3:srctblpref
#p4:desttblpref	#p5:sql uid			#p6:sql load items(skip uid)
function do_load()
{
	p1=$1; p2=$2; p3=$3; p4=$4; p5=$5
	declare -a p6=("${!6}")
	p7=$7 
	if [ -z "$7" ]; then p7="0"; fi

	#echo "$p1 $p2 $p3 $p4 $p5 ${p6[@]} $6 $p7"
	combine1=`make_param p6[@] "0" "0"`
	#if [ -z "$combine1" ] || [ -z "$combine2" ] ; then echo "$FUNCNAME :empty combine1:$combine1 or combine2:$combine2 "; return; fi
	if [ -z "$combine1" ]; then echo "$FUNCNAME :empty combine1 "; return; fi

	#echo "$combine1 --------- $combine2"

	for((index=0;index<${p1};++index))
	do
		srcf=${destfolder}`printf "$p3" $index`	
		#echo "$srcf"
		if [ ! -f "$srcf" ]; then echo "$srcf not exists, cannot load data"; continue; fi

		desttbl=`printf "$p4" $index`
		#echo "$desttbl"

		tmptbl=tmp$desttbl

		#phase 1: create temporary table
		sql1="create table $tmptbl like $desttbl" #temporary
		#echo "sql1:$sql1"
		echo "$sql1" | mysql -hlocalhost -uroot -pyourpass $p2 -N

		#phase 2: (optional) drop all indices from temporary table to speed things up
		#skipped

		#phase 3: load src file into temporary table
		
		sql3="load data infile \"$srcf\" into table $tmptbl ( ${p5}, ${combine1} )"
		#echo "sql3:$sql3"
		echo "$sql3" | mysql -hlocalhost -uroot -pyourpass $p2 -N
	
		combine2=`make_param p6[@] "$p7" "2" ${tmptbl}.`
		#phase 4: copy the data using: on duplicate key update
		sql4="insert into $desttbl select * from $tmptbl on duplicate key update $combine2"
		#echo "sql4:$sql4"
		echo "$sql4" | mysql -hlocalhost -uroot -pyourpass $p2 -N


		#phase 5: remove temporary table
		sql5="drop table $tmptbl" #temporary
		#echo "sql5:$sql5"
		echo "$sql5" | mysql -hlocalhost -uroot -pyourpass $p2 -N
	done
}


#userid, usernewno, shapsw
function migrate1()
{
	starttime=`date +%s`; srcdbnum=500;	
	srcdbname="reg_usrinfor" ; srctblprefix="onlineinfor%d"
	select_fields=("usernewno" "shapsw" "usrname") ;	select_uid="usrno"
	do_select $srcdbnum $srcdbname $srctblprefix "$select_uid" select_fields[@] $dbnum 0  

	load_fields=("usernewno" "shapsw" "usrname") ;	load_uid="userid"
	do_load $dbnum "$destdbname" "$srctblprefix" "$desttblprefix" "$load_uid" load_fields[@]
	endtime=`date +%s` ;	delta=`expr $endtime - $starttime`
	echo "$FUNCNAME start:$starttime end:$endtime elapsed:$delta"
}



#registertype registerdate
function migrate2()
{
	starttime=`date +%s` ; srcdbnum=500; 
	srcdbname="reg_usrinfor" ;	srctblprefix="privateinfor%d"
	select_fields=("registertype" "registerdate") ;	select_uid="usrno"
	do_select $srcdbnum $srcdbname $srctblprefix "$select_uid" select_fields[@] $dbnum 0

	load_fields=("registertype" "registerdate") ; load_uid="userid"
	do_load $dbnum "$destdbname" "$srctblprefix" "$desttblprefix" "$load_uid" load_fields[@]
	endtime=`date +%s` ; 	delta=`expr $endtime - $starttime`
	echo "$FUNCNAME start:$starttime end:$endtime elapsed:$delta"
}

#registerapplyid
function migrate3()
{
	starttime=`date +%s` ; srcdbnum=500; 
	srcdbname="reg_reginfor" ;	srctblprefix="reginfor_%d"
	select_fields=("registerapplyid") ; select_uid="userno"
	do_select $srcdbnum $srcdbname $srctblprefix "$select_uid" select_fields[@] $dbnum 0

	load_fields=("registerapplyid") ; 	load_uid="userid"
	do_load $dbnum "$destdbname" "$srctblprefix" "$desttblprefix" "$load_uid" load_fields[@]
	endtime=`date +%s` ; 	delta=`expr $endtime - $starttime`
	echo "$FUNCNAME start:$starttime end:$endtime elapsed:$delta"
}

#mobile 
function migrate4()
{
	starttime=`date +%s` ; srcdbnum=200; 
	srcdbname="SecureMobile_Db" ;	srctblprefix="MobileNo2UserID%d_Tbl"
	select_fields=("MobileNo") ; 	select_uid="UserID"
	do_select $srcdbnum $srcdbname $srctblprefix "$select_uid" select_fields[@] $dbnum 0

	load_fields=("mobile") ; 	load_uid="userid"
	do_load $dbnum "$destdbname" "$srctblprefix" "$desttblprefix" "$load_uid" load_fields[@]
	endtime=`date +%s` ; 	delta=`expr $endtime - $starttime`
	echo "$FUNCNAME start:$starttime end:$endtime elapsed:$delta"
}

#mail 
function migrate5()
{
	starttime=`date +%s` ; 	srcdbnum=200 ;	
	srcdbname="LoginEmail_Db" ;	srctblprefix="Email2UserNo%d_Tbl"
	select_fields=("UserEmail") ;	select_uid="UserNo"
	do_select $srcdbnum $srcdbname $srctblprefix "$select_uid" select_fields[@] $dbnum 0

	load_fields=("mail") ;	load_uid="userid"
	do_load $dbnum "$destdbname" "$srctblprefix" "$desttblprefix" "$load_uid" load_fields[@]
	endtime=`date +%s` ; delta=`expr $endtime - $starttime`
	echo "$FUNCNAME start:$starttime end:$endtime elapsed:$delta"
}

#nickname, need to deal with non-ascii characters (hex, unhex)
function migrate6()
{
	starttime=`date +%s` ; 	srcdbnum=500 ; srcdbname="reg_usrinfor" ;	srctblprefix="onlineinfor%d"
	select_fields=("nickname") ; 	select_uid="usrno"
	do_select $srcdbnum $srcdbname $srctblprefix "$select_uid" select_fields[@] $dbnum 1

	load_fields=("nickname") ; 	load_uid="userid"
	do_load $dbnum "$destdbname" "$srctblprefix" "$desttblprefix" "$load_uid" load_fields[@] 2
	endtime=`date +%s` ; 	delta=`expr $endtime - $starttime`
	echo "$FUNCNAME start:$starttime end:$endtime elapsed:$delta"

#	for((index=0;index<$dbnum;++index))
#	do
#		sql="update basicInfo${index} set nickname = unhex(nickname)"
#		echo "$sql" | mysql -hlocalhost -uroot -pyourpass yourdbname -N
#	done
}


migrate1
migrate2
migrate3

migrate4
migrate5
migrate6
