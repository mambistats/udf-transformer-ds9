#! /usr/bin/env bash

echo $'a\tb\tc\t127.126.125.124\t223.123.124.129\tf\tg\th\ti\tj\tk\tl\tm\tn\to\tp\tq\tr' | \
java -Xmx4G -cp "${BASEDIR}/target/udf-transformer-ds9-0.0.1-SNAPSHOT-jar-with-dependencies.jar" \
Use Basic,IPv6 \
--ipv6-ranges "${BASEDIR}/resources/geoip2_city_ipv6_network_ranges-pdate=2017-09-04.gz" \
--select 1 2 3 4 5 IPv6.ip[4] IPv6.ip[5] 6 7 8 9 10 11 12 13 14 15 16 17 Basic.concat[1,2] Basic.concat_v[6,7,8]
