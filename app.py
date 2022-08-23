import os
import re
from typing import Any

from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import BadRequest

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

def do_cmd(cmd: str, value: str, data: Any) -> list:
    """
    Обрабатывает запрос при помощи Query.
    :param cmd: команда query
    :param value: аргумент команды query
    :param data: данные для обработки.
    :return: Iterable
    """

    if cmd == 'filter':
        cmd_res = list(filter(lambda record: value in record, data))
    elif cmd == 'map':
        col_num: int = int(value)
        if col_num == 0:
            cmd_res = list(map(lambda record: record.split()[col_num], data))
        elif col_num == 1:
            cmd_res = list(map(lambda record: record.split()[3] + record.split()[4], data))
        elif col_num == 2:
            cmd_res = list(map(lambda record: ' '.join(record.split()[5:]), data))
        else:
            raise BadRequest
    elif cmd == 'unique':
        cmd_res = list(set(data))
    elif cmd == 'sort':
        reverse = (value == 'desc')
        cmd_res = sorted(data, reverse=reverse)
    elif cmd == 'limit':
        cmd_res = data[:int(value)]
    elif cmd == 'regex':
        cmd_res = list(filter(lambda r: re.compile(value).search(r), data))
    else:
        raise BadRequest
    return cmd_res


def do_query(param: dict) -> list[str]:
    """
    Обрабатывает запрос, получает json, возвращает результат в виде списка.
    :param param: json
    :return: list
    """
    with open(os.path.join(DATA_DIR, param['file_name'])) as f:
        res = do_cmd(param['cmd1'], param['value1'], f)
        res = do_cmd(param['cmd2'], param['value2'], res)

        return res

@app.post("/perform_query")
def perform_query() -> Response:

    req = request.json

    if not req:
        raise BadRequest

    if not os.path.exists(os.path.join(DATA_DIR, req['file_name'])):
        raise BadRequest

    return jsonify(do_query(req))
