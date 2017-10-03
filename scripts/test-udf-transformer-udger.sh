#! /usr/bin/env bash

echo $'a\tb\tc\tMozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/52.0.2743.116 Safari/537.36\te\tf' | \
java -Xmx4G -cp "${BASEDIR}/target/udf-transformer-ds9-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Udger \
--udger-database "${BASEDIR}/resources/udgerdb_v3.dat" \
--udger-inmem \
--udger-cache 100000 \
--select 1 2 3 Udger.parseUa[4] 5 6 \
--output-sep "|"
