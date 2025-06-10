
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="synthea"
DB_USER="admin"
DB_PASS="adminpassword"
CSV_DIR="/home/petriscyril/Desktop/Agent_ETL/Synthea"

for file in "$CSV_DIR"/*.csv; do
  if [ -f "$file" ]; then
    filename=$(basename "$file")
    tablename="${filename%.*}"
    echo "Importing $filename into table $tablename..."
    
    csvsql --db "postgresql://$DB_USER:$DB_PASS@$DB_HOST:$DB_PORT/$DB_NAME" \
      --insert "$file" \
      --overwrite \
      --no-constraints \
      --blanks \
      --delimiter ','
    
    if [ $? -eq 0 ]; then
      echo "Successfully imported $filename"
    else
      echo "Failed to import $filename"
    fi
  fi
done

echo "Import process completed"