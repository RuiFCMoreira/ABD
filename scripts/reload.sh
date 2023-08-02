DB_NAME=${1:-dbdump}
DUMP_FILE_FOLDER=${2:-/home} 

echo "Dropping DB" && sudo -u postgres psql -U postgres -c "drop database $DB_NAME;"
echo "Creating DB" && sudo -u postgres psql -U postgres -c "create database $DB_NAME;"
echo "Restoring DB" && sudo -u postgres pg_restore -j 8 -d $DB_NAME -U postgres -Fc $DUMP_FILE_FOLDER/dump_file