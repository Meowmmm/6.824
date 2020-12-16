#!/bin/bash
i=1
while [ $i -le 100 ]
do
    echo "test "$i "---------------------------------------"
    go test -run TestFailAgree2B  > ./$1
    grep Pass ./$1 
    grep FAIL ./$1  && echo "failed in test "$i && exit 1
    let i+=1
done
