#!/usr/bin/env python
# MIT License

# Copyright (c) 2022, Parallel Systems Architecture Lab (PARSA)

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:

# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

# -*- coding: utf-8 -*-
import json
import csv
import unidecode
from contextlib import contextmanager
import sys, os


def copy_dic(in_dic, out_dic, schema):
    for k in schema:
        out_dic[k] = in_dic[k]


def iterate_csv(filename, encoding=""):
    if not encoding:
        encoding = "latin-1"
    with open(filename, newline="", encoding=encoding) as csvfile:
        csv_it = csv.reader(csvfile)
        next(csv_it, None)
        for r in csv_it:
            yield [unidecode.unidecode(i) for i in r]


def read_csv(filename, schema):
    result = []
    indep_var = schema[0]
    dep_schema = schema[1:]
    for row in iterate_csv(filename):
        idx = float(row[0])
        r = {indep_var: idx}
        for k, v in zip(dep_schema, row[1:]):
            r[k] = v
        result.append(r)
    return result


def write_csv(filename, schema, data):
    data_ = [[str(d) for d in r] for r in data]
    csv_str = ",".join(schema) + "\n"
    csv_str += "\n".join([",".join(r) for r in data_])

    with open(filename, "w") as f:
        f.write(csv_str)


def init_or_add_nested_dict(d, k, k2, v):
    if k in d:
        d[k][k2] = v
    else:
        d[k] = {}
        d[k][k2] = v


def init_or_add_to_dic(d, k, v):
    if k in d:
        d[k].append(v)
    else:
        d[k] = [v]


def get_from_dic_or_false(d, k):
    if k in d:
        return d[k]
    else:
        return False


def get_dict_json(json_file):
    with open(json_file, "r", encoding="utf-8") as f:
        s = unidecode.unidecode(f.read())
        d = json.loads(s)

    return d


def save_dict_json(json_file, save_me):
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(save_me, f)


@contextmanager
def suppress_stdout():
    with open(os.devnull, "w") as devnull:
        old_stdout = sys.stdout
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = old_stdout
