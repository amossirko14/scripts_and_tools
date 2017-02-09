#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os

import fnvhash

elts = ["hello", "world", "foo", "bar"]

for elt in elts:
    res1 = fnvhash.fnv1a_32(elt)
    fp = os.popen("./go_fnv1a32 " + str(elt))
    tmp = fp.read()
    vals = [val for val in tmp.split("\n") if len(val) != 0]
    res2 = int(vals[0])
    print(elt, res1, res2, res1 == res2)
