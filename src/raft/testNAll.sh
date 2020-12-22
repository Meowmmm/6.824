#!/bin/bash
i=1
while [ $i -le 100 ]
do
    echo "\n\n\ntest "$i "---------------------------------------"
    echo "TestInitialElection2A"
    go test -run TestInitialElection2A  > ./$1
    grep Pass ./$1 || echo "failed" | exit 1

    echo "TestReElection2A"
    go test -run TestReElection2A  > ./$1
    grep Pass ./$1 || echo "failed" | exit 1

    echo "TestBasicAgree2B"
    go test -run TestBasicAgree2B > ./$1
    grep Pass ./$1 || echo "failed" | exit 1

    echo "TestFailAgree2B"
    go test -run TestFailAgree2B > ./$1
    grep Pass ./$1 || echo "failed" | exit 1
   
    echo "TestFailNoAgree2B"
    go test -run TestFailNoAgree2B  > ./$1
    grep Pass ./$1 || echo "failed" | exit 1

    let i+=1
done
