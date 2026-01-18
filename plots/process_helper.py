from stylelib import *
import pandas as pd
import json
import matplotlib.patches as mpatches


C_BASE = "BLUE"
C_PROPOSE = "ORANGE"


def get_pattern_patches(labels, patterns):
    patches = []
    for i, l in enumerate(labels):
        p = mpatches.Patch(facecolor=(1, 1, 1, 0), edgecolor='black',alpha=.99,hatch=patterns[i],label=l)
        patches.append(p)
    return patches


def get_color_patches(labels, colors):
    patches = []
    for i, l in enumerate(labels):
        p = mpatches.Patch(facecolor=colors[i], edgecolor='black',alpha=.99, label=l)
        patches.append(p)
    return patches

def reformat_line(line):
    if "-nan" in line:
        return line.replace("-nan", "\"-nan\"")
#     line = line.split("{")[1].split("}")[0]
#     for token in line.split(","):
#         var = token.split(":")[0]
#         if "\"" not in var:
#             print("missing double quote in {}".format(var))
#             line = line.replace(var, "\"{}\"".format(var.strip()))
    return line

def apply_mask(df, mask):
    for m in mask:
        if len(m) == 3:
            df = df[df[m[0]] != m[1]]
        else:
            if isinstance(m[1], list):
                # format [operation, value], e.g. ["le", 0]
                if m[1][0] == "le":
                    df = df[df[m[0]] <= m[1][1]]
                elif m[1][0] == "ge":
                    df = df[df[m[0]] >= m[1][1]]
                elif m[1][0] == "lt":
                    df = df[df[m[0]] < m[1][1]]
                elif m[1][0] == "gt":
                    df = df[df[m[0]] > m[1][1]]
                elif m[1][0] == "eq":
                    df = df[df[m[0]] == m[1][1]]
                elif m[1][0] == "ne":
                    df = df[df[m[0]] != m[1][1]]
            else:
                df = df[df[m[0]] == m[1]]
    return df


