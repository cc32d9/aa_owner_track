NETWORK=$1

if [ x${NETWORK} = x ]; then echo "Network required" 1>&2; exit 1; fi

sed -e 's,\%\%,'${NETWORK}',g' aa_owner_track_tables.psql | mysql

if [ $? -eq 0 ]; then echo "Done"; else echo "Errors encountered"; fi

                             
