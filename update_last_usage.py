import argparse

from utils import get_redis_instance, get_cnx_cur, close_cnx

# REDIS_HOST = "delphi_redis_instance"
# REDIS_PORT = 6379
# REDIS_DB_INDEX = 0
# REDIS_KEY_PATTERN = "*LAST_USAGE*"

# DB_USER = "user"
# DB_PASS = "pass"
# DB_HOST = "0.0.0.0"
# DB_PORT = 13306
# DB_NAME = "epidata"


def parse_args():
    parser = argparse.ArgumentParser(
        prog="Update api_user last_time_used column",
        description="Update api_user last_time_used based on records in Redis"
    )

    parser.add_argument("-rh", "--redis_host", help="Redis host")
    parser.add_argument("-rp", "--redis_port", help="Redis port")
    parser.add_argument("-rdi", "--redis_db_index", help="Redis DB index")
    parser.add_argument("-rkp", "--redis_key_pattern", help="Redis Key pattern")

    parser.add_argument("-du", "--db_user", help="DB user")
    parser.add_argument("-dup", "--db_user_password", help="DB user password")
    parser.add_argument("-dh", "--db_host", help="DB host")
    parser.add_argument("-dp", "--db_port", help="DB port")
    parser.add_argument("-dn", "--db_name", help="DB name")
    args = parser.parse_args()
    return args


def get_key_val_pairs(redis_cli, pattern: str):
    keys = redis_cli.keys(pattern=pattern)
    key_val_pairs = {key.split("/")[1]: redis_cli.get(key) for key in keys}
    return key_val_pairs


def update_database(api_key: str, last_time_used: str, cur):
    cur.execute(
        f"""
        UPDATE
            api_user
        SET last_time_used = "{last_time_used}"
        WHERE api_key = "{api_key}"
    """
    )


def main():
    args = parse_args()
    redis_cli = get_redis_instance(host=args.redis_host, port=args.redis_port, db_index=args.redis_db_index)
    key_val_pairs = get_key_val_pairs(redis_cli, args.redis_key_pattern)
    cnx, cur = get_cnx_cur(db_user=args.db_user, db_user_password=args.db_user_password, db_host=args.db_host, db_name=args.db_name, db_port=args.db_port)
    for k, v in key_val_pairs.items():
        update_database(k, v, cur)
    close_cnx(cnx)


if __name__ == "__main__":
    main()
