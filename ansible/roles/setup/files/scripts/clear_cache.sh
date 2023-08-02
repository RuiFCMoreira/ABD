DB_NAME=${1:-dbdump} 

echo "Stopping postgres service" && systemctl stop postgresql && \
echo "Clearing system cache" && sh -c "echo 3 > /proc/sys/vm/drop_caches" && \
echo "Restarting postgres service" && systemctl start postgresql && \
echo "Postgres vacuum" && sh -c "sudo -u postgres psql -U postgres -d $DB_NAME -c \"vacuum analyze;\""