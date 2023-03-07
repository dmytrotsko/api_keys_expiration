# Update last usage

python update_last_usage.py -rh 0.0.0.0 -rp 6379 -rdi 0 -rkp \*LAST_USAGE\* -du user -dup pass -dh 0.0.0.0 -dp 13306 -dn epidata

# Remove outdated keys
python remove_outdated_keys.py -du user -dup pass -dh 0.0.0.0 -dp 13306 -dn epidata