#!/bin/bash
#script function:1.cd libsdk and compile libcfs.so
#                2.put libcfs.so under the src/main/resource directory
#                3.package jar
cd ../libsdk
./build.sh
cp libcfs.so ../java/src/main/resources/
#when mvn package ,skipping test
cd ../java
mvn clean package -Dmaven.test.skip=true
