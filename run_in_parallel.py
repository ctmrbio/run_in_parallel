#!/usr/bin/env python
# Fredrik Boulund (c) 2014, 2015, 2016
# Run a program on multiple data files on a SLURM managed cluster

from sys import argv, exit
from subprocess import Popen, PIPE
import argparse


def parse_commandline():
    """Parse commandline.
    """

    desc= ("Run in parallel on UPPMAX using Slurm sbatch. "
           "Fredrik Boulund (c) 2014, 2015, 2016.")

    parser = argparse.ArgumentParser(description=desc)

    slurm = parser.add_argument_group("SLURM", "Set slurm parameters.")
    slurm.add_argument("-n", type=int, metavar="n",
        default=1,
        help="Number of cores. [%(default)s].")
    slurm.add_argument("-N", type=int, metavar="N",
        default=0,
        help="Number of nodes [%(default)s]. Setting N>0 will require 'n mod 16 = 0'.")
    slurm.add_argument("-p", metavar="p",
        choices=["core", "node", "devel", "devcore"],
        default="core",
        help="Slurm partition [%(default)s].")
    slurm.add_argument("-A", metavar="account",
        default="b2016371",
        help="Slurm account [%(default)s].")
    slurm.add_argument("-t", metavar="t",
        default="01:00:00",
        help="Max runtime per job [%(default)s].")
    slurm.add_argument("-C", 
        default="",
        help=("Specify node memory size [default let Slurm decide]. "
              "Several options available, e.g.: "
              "mem64GB, mem128GB, mem256GB, mem512GB, usage_mail. "
              "Combine options with '&', e.g. 'mem128GB&usage_mail'."))
    slurm.add_argument("-J", metavar="jobname",
        default="",
        help="Slurm job name [query file name].")

    program_parser = parser.add_argument_group("PROGRAM", "Command to run in parallel.")
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
    program_parser.add_argument("query", nargs="+", metavar="FILE",
        default="",
        help="Query file(s).")

    if len(argv)<2:
        parser.print_help()
        exit()

    options = parser.parse_args()
    return options



def generate_sbatch_scripts(options):
    """Generate sbatch scripts.

    The default Slurm job name is the name of the first query file name in the
    produced job script. 
    """

    while options.query:
        query_files_in_script = []
        calls = []
        for query_file in options.query[0:options.stack]:
            options.query.pop(0)
            query_files_in_script.append(query_file)
            call = options.call.format(query=query_file)
            calls.append(call)

        sbatch_script = ["#!/usr/bin/env bash",
            "# Job script automatically generated using run_in_parallel.py",
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
    print(sbatch_script)

    sbatch = Popen("sbatch", stdin=PIPE, stdout=PIPE, stderr=PIPE)
    out, err = sbatch.communicate(sbatch_script.encode("utf-8"))
    if err:
        raise Exception("sbatch error: {}".format(err))



if __name__ == "__main__":
    options = parse_commandline()
    for sbatch_script, query_files in generate_sbatch_scripts(options):
        call_sbatch(sbatch_script)
        if len(query_files) > 1:
            print("Submitted stacked Slurm job for {num} files: '{names}'".format(num=len(query_files), 
                    names="', '".join(query_files)))
        else:
            print("Submitted Slurm job for: '{name}'".format(name=query_files[0]))
