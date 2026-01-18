import json
import os
import subprocess
import pandas as pd


def set_config(conf, db_conf, ycsb_conf, fname="sample.cfg"):
    template = json.load(open(fname))
    template.update(conf)
    template["db_config"].update(db_conf)
    template["ycsb_config"].update(ycsb_conf)
    out_fname = fname.split(".cfg")[0]+"_runtime.cfg"
    json.dump(template, open(out_fname, "w+"), indent=4)
    return out_fname


def try_compile(name):
    try:
        subprocess.run("cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=/home/hippo/mydata/vcpkg/scripts/buildsystems/vcpkg.cmake ../", shell=True, check=True)
        subprocess.run("make -j %s" % name, shell=True, check=True)
    except Exception as e:
        print("ERROR compiling, will exit. ")
        print(e)
        exit(1)


def try_exec(name, conf_file, info):
    cmd = "./%s %s" % (name, conf_file)
    try:
        subprocess.run(cmd, shell=True, check=True)
        return True
    except Exception as e:
        print("ERROR executing: {}".format(info))
        print(e)
        err_file = open("error.log", "a+")
        err_file.write(info + ": ")
        err_file.write(str(e))
        err_file.write("\n")
        err_file.close()
        return False
