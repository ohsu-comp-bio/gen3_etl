# run on bmeg.io
sqlite3 /mnt/data2/gdan-tmp-webdash/data/features_by_cancer.sqlite \
 "SELECT name  FROM sqlite_master  where type = 'table' and name not like '%features' and name not like 'sql%' order by name ;" \
 | python3 generate_sql.py \
 | sqlite3 /mnt/data2/gdan-tmp-webdash/data/features_by_cancer.sqlite > gdan-tmp-sample_ids.txt
