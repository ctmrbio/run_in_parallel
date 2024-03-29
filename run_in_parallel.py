#!/usr/bin/env python
# Fredrik Boulund (c) 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021
# Run a program on multiple data files on a SLURM managed cluster

__author__ = "Fredrik Boulund"
__version__ = "v2.2"
__date__ = "2014-2021"

from sys import argv, exit
from subprocess import Popen, PIPE
import os
import argparse


def parse_commandline():
    """Parse commandline.
    """

    desc = """Run in parallel (RIP), version {ver}.
              {author} (c) {date}""".format(author=__author__, date=__date__, ver=__version__)

    parser = argparse.ArgumentParser(description=desc)

    slurm = parser.add_argument_group("SLURM", "Set slurm parameters.")
    slurm.add_argument("-n", type=int, metavar="n",
            default=1,
            help="Number of cores. [%(default)s].")
    slurm.add_argument("-N", type=int, metavar="N",
            default=0,
            help="""Number of nodes [%(default)s]. 
                    Setting N>0 will require 'n mod 16 = 0'.""")
    slurm.add_argument("-p", metavar="p",
            default="ctmr",
            help="Slurm partition [%(default)s].")
    slurm.add_argument("-A", metavar="account",
            default="bio",
            help="Slurm account [%(default)s].")
    slurm.add_argument("-t", metavar="t",
            default="01:00:00",
            help="Max runtime per job [%(default)s].")
    slurm.add_argument("-C", 
            default="",
            help="""Specify node memory size [default let Slurm decide].
                  Several options available, e.g.: 
                  mem64GB, mem128GB, mem256GB, mem512GB, usage_mail. 
                  Combine options with '&', e.g. 'mem128GB&usage_mail'.""")
    slurm.add_argument("-J", metavar="jobname",
            default="",
            help="Slurm job name [query file name].")
    slurm.add_argument("--dryrun", action="store_true",
            default=False,
            help="""Perform a dry run, i.e. print job scripts to STDOUT
                    and do not call Slurm [%(default)s].""")

    program_parser = parser.add_argument_group("COMMAND", "Command to run in parallel.")
    program_parser.add_argument("--call", required=True,
            default="",
            help="""Program and arguments in a single-quoted string,
                    e.g. 'blat dbfile.fasta {query} -t=dnax q=prot {query}.blast8'.
                    {query} is substituted for the filenames specified on
                    as arguments to run_in_parallel.py (one file per Slurm job).""")
    program_parser.add_argument("--stack", type=int, metavar="N",
            default=1,
            help="""Stack N calls on each node. Remember to end your
                    command with '&' so the commands are run simultaneously 
                    [%(default)s].""")
    program_parser.add_argument("--copy-decompress", dest="copy_decompress",
            default=False,
            action="store_true",
            help="""Copy query file to $TMPDIR on node and decompress (if
                    necessary) before running command [%(default)s].""")
    program_parser.add_argument("query", nargs="*", metavar="FILE", 
            default="",
            help="Query file(s).")
    program_parser.add_argument("-f", "--file", metavar="FILE", dest="read_from_file",
            default="",
            help="Read query file(s) from FILE, one file per line. "
                 "Overrides files given on command line if present.")

    if len(argv)<2:
        parser.print_help()
        exit()

    options = parser.parse_args()
    return options



def copy_decompress(source_fn):
    """Generate bash code to copy/decompress file to $TMPDIR.
    """
    new_fn = os.path.basename(source_fn)
    if source_fn.lower().endswith(".gz"):
        new_fn = new_fn.strip(".gz")
        cmd = "gunzip -C {source_fn} > $TMPDIR/{new_fn}"
    elif source_fn.lower().endswith(".bz2"):
        new_fn = new_fn.strip(".bz2")
        cmd = "bzip2 -dc {source_fn} > $TMPDIR/{new_fn}"
    elif source_fn.lower().endswith(".dsrc"):
        new_fn = new_fn.strip(".dsrc")
        cmd = "dsrc d {source_fn} $TMPDIR/{new_fn}"
    else:
        cmd = "cp {source_fn} $TMPDIR/{new_fn}"
    cmd = "\n".join([cmd, "cd $TMPDIR"])
    return cmd.format(source_fn=source_fn, new_fn=new_fn), new_fn



def generate_sbatch_scripts(options):
    """Generate sbatch scripts.

    The default Slurm job name is the name of the first query file name in the
    produced job script. 
    """

    if options.read_from_file:
        with open(options.read_from_file) as f:
            query_files = [line.strip() for line in f.readlines()]
    else:
        query_files = options.query
    while query_files:
        query_files_in_script = []
        calls = []
        cwd = os.getcwd()+"/"

        for query_file in query_files[0:options.stack]:
            query_files.pop(0)
            query_files_in_script.append(query_file)
            if options.copy_decompress:
                cp_cmd, query_file = copy_decompress(query_file)
                calls.append(cp_cmd)
            call = options.call.format(query=query_file, cwd=cwd)
            calls.append(call)

        sbatch_script = ["#!/usr/bin/env bash",
                "# Automatically generated by run_in_parallel.py",
                "#SBATCH -n {n}".format(n=options.n),
                "#SBATCH -p {p}".format(p=options.p),
                "#SBATCH -A {A}".format(A=options.A),
                "#SBATCH -t {t}".format(t=options.t),
                ]
        if options.N:
            sbatch_script.append("#SBATCH -N {N}".format(N=options.N))
        if options.C:
            sbatch_script.append("#SBATCH -C {C}".format(C=options.C))
        if options.J:
            sbatch_script.append("#SBATCH -J {J}".format(J=options.J))
        else:
            # No job name specified; use first query file in script
            sbatch_script.append("#SBATCH -J {J}".format(J=query_files_in_script[0]))

        [sbatch_script.append(c) for c in calls]

        yield "\n".join(sbatch_script), query_files_in_script



def call_sbatch(sbatch_script):
    """Run sbatch in a subprocess.
    """

    sbatch = Popen("sbatch", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = sbatch.communicate(sbatch_script.encode("utf-8"))
    if err:
        raise Exception("sbatch error: {}".format(err))



if __name__ == "__main__":
    options = parse_commandline()
    for sbatch_script, query_files in generate_sbatch_scripts(options):
        if options.dryrun:
            print(sbatch_script)
        else:
            call_sbatch(sbatch_script)
            if len(query_files) > 1:
                print("Submitted stacked Slurm job for {num} files: '{names}'".format(
                        num=len(query_files), 
                        names="', '".join(query_files)))
            else:
                print("Submitted Slurm job for: '{name}'".format(name=query_files[0]))

