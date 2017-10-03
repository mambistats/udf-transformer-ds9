#! /usr/bin/env bash
cd "${BASEDIR}/resources"
curl "https://github.com/udger/test-data/blob/master/data_v3/udgerdb_v3.dat" -o udgerdb_v3.dat

echo "Warning: the current test data is too old and DOES NOT WORK!"
echo "You need a commercial uptodate [udgerdb_v3.dat] database"
