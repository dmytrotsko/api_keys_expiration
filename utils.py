import redis

import mysql.connector


def get_redis_instance(host: str, port: int, db_index: int):
    redis_cli = redis.Redis(host=host, port=port, db=db_index, charset="utf-8", decode_responses=True)
    return redis_cli


def get_cnx_cur(db_user, db_user_password, db_host, db_name, db_port):
    cnx = mysql.connector.connect(
        user=db_user, password=db_user_password, host=db_host, database=db_name, port=db_port
    )
    cur = cnx.cursor()
    return cnx, cur


def close_cnx(cnx):
    cnx.commit()
    cnx.close()
