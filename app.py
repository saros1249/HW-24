import os
import re
from typing import Iterator, Any

from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")


def do_cmd(cmd: str, value: Any, it: Iterator) -> Iterator:
    """
    Обрабатывает запрос при помощи Query.
    :param cmd: команда query
    :param value: аргумент команды query
    :param it: данные для обработки.
    :return: Iterable
    """

    if cmd == 'filter':
        cmd_res = filter(lambda record: value in record, it)
    elif cmd == 'map':
        col_num: int = int(value)
        if col_num == 0:
            cmd_res = map(lambda record: record.split()[col_num], it)
        elif col_num == 1:
            cmd_res = map(lambda record: record.split()[3] + record.split()[4], it)
        elif col_num == 2:
            cmd_res = map(lambda record: ' '.join(record.split()[5:]), it)
        else:
            raise BadRequest
    elif cmd == 'unique':
        cmd_res = list(set(it))
    elif cmd == 'sort':
        reverse = (value == 'desc')
        cmd_res = sorted(it, reverse=reverse)
    elif cmd == 'limit':
        cmd_res = it[:int(value)]
    elif cmd == 'regex':
        cmd_res = re.findall(value, it)
    else:
        raise BadRequest
    return cmd_res


def do_query(param):
    """
    Обрабатывает запрос, получает json, возвращает результат в виде списка.
    :param json
    :return: list
    """
    with open(os.path.join(DATA_DIR, param['file_name'])) as f:
        res = do_cmd(param['cmd1'], param['value1'], f)
        res = do_cmd(param['cmd2'], param['value2'], res)

        return list(res)


@app.route("/perform_query", methods=['POST'])
def perform_query():
    req = request.json

    if not req:
        raise BadRequest

    if not os.path.exists(os.path.join(DATA_DIR, req['file_name'])):
        raise BadRequest

    return jsonify(do_query(req))


if __name__ == '__main__':
    app.run()
