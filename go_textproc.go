package main

import (
	"os"
	"bufio"
	"strings"
	"fmt"
	"strconv"
	"flag"
)

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func main() {
	var srcNum = flag.Int("srcNum", 0, "source file num")
	var srcPref = flag.String("srcPref", "", "source file prefix")
	var destNum = flag.Int("destNum", 0, "dest file num")
	var destPref = flag.String("destPref", "", "dest file prefix")
	flag.Parse()

	fmt.Println(*srcNum, *srcPref, *destNum, *destPref)
	HandleText(*srcNum, *srcPref, *destNum, *destPref)
}

func HandleText(srcNum int, srcPref string, destNum int, destPref string) {
	if srcNum < 0 || destNum < 0 || len(srcPref) == 0 || len(destPref) == 0 {
		return
	}

	fileList := []*os.File{}
	buffwriters := []*bufio.Writer{}
	for i := 0; i < destNum; i++ {
		tmpfile, err := os.OpenFile(fmt.Sprintf(destPref, i), os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		check(err)

		fileList = append(fileList, tmpfile)
		buffwriters = append(buffwriters, bufio.NewWriter(tmpfile))

		defer fileList[i].Close()
	}

	for i := 0; i < srcNum; i++ {
		filename := fmt.Sprintf(srcPref, i)
		//fmt.Println("file:", filename)
		//readLines(filename)
		file, e := os.Open(filename)
		check(e)
		defer file.Close()

		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			tmpline := scanner.Text()
			elts := strings.Split(tmpline, "\t")
			//fmt.Println(len(elts), elts)
			if len(elts) < 2 {
				continue
			}
			uid, _ := strconv.Atoi(elts[0])
			remnant := uid % destNum
			buffwriters[remnant].WriteString(tmpline + "\n")
		}
	}

	for i := 0; i < len(fileList); i++ {
		buffwriters[i].Flush()
	}
}


