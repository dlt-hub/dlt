if [[ $# -gt 0 ]]
then

i=1;
for t in "$@"
do

    dbt build -t $t

done

else
    echo "Please specify one or more targets as command-line arguments, i.e. test.sh bq snowflake"
fi
