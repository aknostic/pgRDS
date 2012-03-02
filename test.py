import os, sys

os.system("sudo -u postgres psql -t -c \"select '\\\"'||rolname||'\\\"'||' \\\"'||rolpassword||'\\\"' from pg_authid ;\" | sed 's/^\s*//' | sed '/^$/d' > /etc/pgbouncer/userlist.txt")
