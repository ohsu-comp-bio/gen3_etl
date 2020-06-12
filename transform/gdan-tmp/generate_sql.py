import sys
sql = "\nUNION\n".join([f"select distinct sample_id from {table_name}" for table_name in [line.rstrip('\n') for line in sys.stdin]] )
print(sql)
