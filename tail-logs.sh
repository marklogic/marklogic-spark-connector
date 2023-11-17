# Helps with development. You may need to run 'chmod 755 tail-logs.sh' first.
# Run this via ./tail-logs.sh 8016, or change the port number to whatever you wish.
tail -f logs/bootstrap_3n/$1_ErrorLog.txt logs/node2/$1_ErrorLog.txt logs/node3/$1_ErrorLog.txt logs/bootstrap_3n/$1_AccessLog.txt logs/node2/$1_AccessLog.txt logs/node3/$1_AccessLog.txt
