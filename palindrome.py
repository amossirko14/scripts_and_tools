#!/usr/bin/env python
# -*- coding: utf-8 -*-

def is_palindrome(src):
    # skipped type checking:isinstance(src, str)
    trimmed = "".join([e.upper() for e in src if e.isalnum()])
    return trimmed[0:len(trimmed)//2:1] == trimmed[-1:(len(trimmed)-1)//2:-1]

elts = ["",
        "e",
        "madam",
        "oops",
        "ee",
        "Madam in Eden i'm Adam",
        "1-2*)1?",
        "se7en",
        ]

for e in elts:
    print(e.rjust(30) + " => " + str(is_palindrome(e)))
