import argparse
from collections import namedtuple
from utils import get_cnx_cur, close_cnx

API_USER_RECORD = namedtuple("APIUser", ("api_key", "email", "date_diff"))

# DB_USER = "user"
# DB_PASS = "pass"
# DB_HOST = "0.0.0.0"
# DB_NAME = "epidata"
# DB_PORT = 13306


def parse_args():
    parser = argparse.ArgumentParser(
        prog="Update api_user last_time_used column",
        description="Update api_user last_time_used based on records in Redis"
    )

    parser.add_argument("-du", "--db_user", help="DB user")
    parser.add_argument("-dup", "--db_user_password", help="DB user password")
    parser.add_argument("-dh", "--db_host", help="DB host")
    parser.add_argument("-dp", "--db_port", help="DB port")
    parser.add_argument("-dn", "--db_name", help="DB name")
    args = parser.parse_args()
    return args


def get_outdated_keys(cur):
    cur.execute(
        """
            SELECT
                diff.api_key,
                diff.email,
                diff.date_diff
            FROM (
                SELECT
                    api_key,
                    email,
                    created,
                    last_time_used,
                    ABS(TIMESTAMPDIFF(MONTH, last_time_used, created)) as date_diff
                FROM api_user
            ) diff
            WHERE diff.date_diff >= 5;

        """
    )
    outdated_keys = cur.fetchall()
    return outdated_keys


def remove_outdated_key(cur, api_key):
    cur.execute(
        f"""
            DELETE FROM api_user WHERE api_key = "{api_key}"
        """
    )


def send_notification(api_key, email):
    pass


def main():
    args = parse_args()
    cnx, cur = get_cnx_cur(db_user=args.db_user, db_user_password=args.db_user_password, db_host=args.db_host, db_name=args.db_name, db_port=args.db_port)
    outdated_keys_list = [API_USER_RECORD(*item) for item in get_outdated_keys(cur)]
    for item in outdated_keys_list:
        if item.date_diff == 5:
            send_notification(item.email)
        else:
            remove_outdated_key(cur, item.api_key)
    close_cnx(cnx)


if __name__ == "__main__":
    main()
