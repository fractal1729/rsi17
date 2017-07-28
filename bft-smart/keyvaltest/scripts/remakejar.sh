#!/bin/bash

javac -classpath ../bin/BFT-SMaRt.jar:../lib/* ../src/keyvaltest/*.java
echo Classes compiled.
cd ../src
jar cvf keyvaltest.jar keyvaltest
echo Jar file made.
mv keyvaltest.jar ../dist/keyvaltest.jar
cd ../scripts
echo Jar file moved to dist/, script complete.