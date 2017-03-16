#!/usr/bin/env impala-python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Usage: single_node_perf_run.py [options] base_revision target_revision
#
#   -h, --help            show this help message and exit
#   -e EXEC_OPTIONS, --exec-options=EXEC_OPTIONS
#                         Additional Impala exec options.
#   -r ITERATIONS, --iterations=ITERATIONS
#                         Number of iterations per query. Default is 5.
#   -l, --load            Indicates if data needs to be loaded
#   --no-build            If set, will not build Impala and simply run the
#                         benchmark.
#   --no-perf             If set, will not execute the workload
#   -c NUM_CLIENTS, --num-clients=NUM_CLIENTS
#                         Number of client connections.
#   --query-filter=QUERY_FILTER
#                         Regex to match query files to run.
#   -s SCALE, --scale=SCALE
#                         Scale factor for the workload.
#   --table-formats=TABLE_FORMATS
#                         Table format
#   -w WORKLOAD, --workload=WORKLOAD
#                         Workload to run.
#   -i NUM_IMPALADS, --num-impalads=NUM_IMPALADS
#                         Number of impalad processes.
#   -b BASELINE, --baseline=BASELINE
#                         Results of the baseline in form of a JSON file.
#   -t, --use-thirdparty-services
#                         If this option is set, it will assume that thirdparty
#                         services are already started
#
# Typical usage patterns:
# -----------------------
#
# The script has to be called from IMPALA_HOME.
#
#   * Compare two revision:
#         ./single_node_perf_run.py --workload=tpch HASH1 HASH2
#   * Compare two revisions and load data:
#          ./single_node_perf_run.py --workload=tpch --load HASH1 HASH2
#   * Load only, no build, no test:
#          ./single_node_perf_run.py --workload=tpch --load --no-perf --no-build
#   * No load, no build, only test:
#          ./single_node_perf_run.py --workload=tpch --no-build
#   * No build, but load and test
#          ./single_node_perf_run.py --workload=tpch --no-build --load
#   * No build, use currently running thirdparty services and compare to existing baseline
#          ./single_node_perf_run -w tpch --no-build -t -b file.json


from optparse import OptionParser
from tempfile import mkdtemp

import json
import logging
import logging.handlers
import os
import sh
import sys

logger = logging.getLogger("perf_run")
logger.setLevel(logging.DEBUG)

# Creates a file appender.
LOG_FILENAME = "single_node_perf_run.log"
fh = logging.handlers.RotatingFileHandler(LOG_FILENAME, backupCount=1)
fh.setLevel(logging.DEBUG)
fh.doRollover()

# Creates a stdout appender.
ch = logging.StreamHandler()
if "PERF_DEBUG" in os.environ:
    ch.setLevel(logging.DEBUG)
else:
    ch.setLevel(logging.INFO)

# Create formatter and add it to the handlers.
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

# Helper and implicit check if Impala environment is correctly
impala_home = os.environ["IMPALA_HOME"]

def tee(line):
    """Output wrapper function used by sh to send the stdout or stderr to the
    module's logger."""
    logger.debug(line.strip())


def load_tpch_data(scale=10):
    """Loads TPC-H with a particular scale factor. The below code models the scripts
    in the TPC-DS repository. Since these scripts are tailored for a managed cluster
    they do not work locally."""
    logger.info("Preparing to generate TPC-H SF {0}".format(scale))
    os.chdir(impala_home)

    # Check if generator directory exists
    if os.path.isdir("TPC-DS-master"):
        sh.rm("TPC-DS-master", R=True, f=True)
        sh.rm("/tmp/tpch-generate", R=True, f=True)
        sh.hdfs.dfs("-rm", '-r', '-f', "/tmp/tpch-generate")

    sh.wget("http://github.mtv.cloudera.com/Performance/TPC-DS/archive/master.zip",
            O="master.zip")
    sh.unzip("master.zip")
    os.chdir(os.path.join(impala_home, "TPC-DS-master"))

    # Patch the TPC-H generator
    sh.sed("s:DOUBLE:DECIMAL(12,2):g", "ddl-tpch/bin_flat/alltables.sql",
           "ddl-tpch/text/alltables.sql", i=True)

    logger.info("Building TPC-H generator")
    tpch_build = sh.Command("./tpch-build.sh").bake(_out=tee, _err=tee)
    tpch_build()

    logger.info("Loading TPC-H data")
    os.chdir("tpch-gen")
    sh.hadoop("jar", "target/tpch-gen-1.0-SNAPSHOT.jar", d="/tmp/tpch-generate/", s=scale)

    os.chdir("..")
    sh.hive("-d", "DB=tpch_{0}_text".format(scale),
            "-d", "LOCATION=/tmp/tpch-generate",
            i="settings/load-flat.sql",
            f="ddl-tpch/bin_flat/alltables.sql",
            _out=tee,
            _err=tee)

    # In a development checkout, impala-shell is impala-shell.sh
    sh.sed("s:impala-shell:impala-shell.sh:g",
           "./ddl-tpch/parquet/create_tpch_tables.sh",
           i=True)

    parquet = sh.Command("./ddl-tpch/parquet/create_tpch_tables.sh").bake(
            scale, _out=tee, _err=tee)
    parquet()

    logger.info("Data load complete")


def get_git_hash_for_name(name):
    return sh.git("rev-parse", name).strip()


def build(git_hash):
    """Builds Impala in release mode without tests and loads the latest metastore
    snapshot. The snapshot is primarily used to have some data for tpc-ds or tpc-h
    available."""
    logger.info("Starting Impala build with Commit: {0}".format(git_hash))
    os.chdir(impala_home)
    sh.git.checkout(git_hash)

    # Build backend
    buildall = sh.Command("{0}/buildall.sh".format(impala_home))
    buildall("-notests", "-release", _out=tee, _err=tee)

    # make_impala = sh.Command("{0}/bin/make_impala.sh".format(impala_home))
    # make_impala("-notests", "-build_type=Release", "-build_static_libs",
    #             _out=tee, _err=tee)

    # # External data source
    # os.chdir(os.path.join(os.environ["IMPALA_HOME"], "ext-data-source"))
    # make_ext_dsrc = sh.mvn.bake("clean", "install", "-U", "-DskipTests",
    #                             _out=tee, _err=tee)
    # make_ext_dsrc()

    # # Build frontend
    # os.chdir(os.path.join(os.environ["IMPALA_HOME"], "fe"))
    # make_frontend = sh.mvn.bake("clean", "install", "-U", "-DskipTests",
    #                             _out=tee, _err=tee)
    # make_frontend()
    logger.info("Build finished.")


def start_dependent_services(num_impalads, start_other_services):
    """Will start all dependent Hadoop services and the Impala mini cluster."""
    run_all = sh.Command("{0}/testdata/bin/run-all.sh".format(
            impala_home)).bake(_out=sys.stdout, _err=sys.stderr)

    impala_cluster = sh.Command("{0}/bin/start-impala-cluster.py".format(
            impala_home)).bake(_out=tee, _err=tee)

    if start_other_services:
        logger.info("Starting services")
        services = run_all(_bg=True)
        logger.info("Waiting for services to become available.")
        services.wait()

    logger.info("Starting Impala cluster")
    cluster = impala_cluster("--build_type=release", s=num_impalads, _bg=True)
    logger.info("Waiting for cluster to be started...")
    cluster.wait()


def run_workload(base_dir, workload, options):
    """Runs the workload identified by `workload` with the given options. Returns the
    git hash of the current revision to identify the output file."""
    git_hash = sh.git("rev-parse", "HEAD").strip()
    logger.info("Start running workload {0} - {1}".format(workload, git_hash))

    run_workload = sh.Command("{0}/bin/run-workload.py".format(
            impala_home)).bake(_out=tee, _err=tee)

    run_workload = run_workload.bake("--client_type=beeswax",
                                     "--workloads={0}".format(workload),
                                     "--query_iterations={0}".format(options.iterations),
                                     "--impalad=localhost:21000",
                                     "--num_clients={0}".format(options.num_clients),
                                     "--table_formats={0}".format(options.table_formats),
                                     "--results_json_file={0}/{1}.json".format(base_dir,
                                                                               git_hash)
                                     )

    if options.exec_options:
        run_workload = run_workload.bake(
            "--exec_options={0}".format(options.exec_options))

    if options.query_filter:
        run_workload = run_workload.bake("--query_names={0}".format(options.query_filter))

    run_workload()
    logger.info(
        "Workload run complete: {0}.json".format(os.path.join(base_dir, git_hash)))


def report_benchmark_results(input, base, description):
    """Wrapper around the report_benchmark_result.py script invoked with an input file
    and a reference file."""
    report = sh.Command("{0}/tests/benchmark/report_benchmark_results.py".format(
            impala_home)).bake(_err=tee)
    report("--input_result_file={0}".format(input),
           "--reference_result_file={0}".format(base),
           '--report_description="{0}"'.format(description),
           _out="{0}/performance_result.txt".format(impala_home))
    logger.info("Performance results stored in {0}".format(
                os.path.join(impala_home, "performance_result.txt")))


def compare(base_dir, base_git_hash, ref_git_hash):
    """Take the results of two performance runs and compare them."""
    base_file = os.path.join(base_dir, base_git_hash + ".json")
    ref_file = os.path.join(base_dir, ref_git_hash + ".json")
    description = "Base: {0} vs Ref: {1}".format(base_git_hash, ref_git_hash)
    report_benchmark_results(ref_file, base_file, description)

    # From the two json files extract the profiles and diff them
    generate_profile_file(base_file, base_git_hash, base_dir)
    generate_profile_file(ref_file, ref_git_hash, base_dir)

    sh.diff("-u",
            os.path.join(base_dir, base_git_hash +"_profile.txt"),
            os.path.join(base_dir, ref_git_hash +"_profile.txt"),
            _out=os.path.join(impala_home, "performance_result_profile_diff.txt"),
            _ok_code=[0,1])
    logger.info("Runtime profile diff stored in {0}".format(
            os.path.join(impala_home, "performance_result_profile_diff.txt")))


def generate_profile_file(name, hash, base_dir):
    """Parses the JSON file identified by `name` and extracts the runtime profiles and
    writes them back in a simple text file in the same directory.
    """
    with open(name) as fid:
       data = json.load(fid)
       with open(os.path.join(base_dir, hash +"_profile.txt"), "w+") as out:
           # For each query
           for key in data:
               for iteration in data[key]:
                   out.write(iteration["runtime_profile"])
                   out.write("\n\n")

def backup_workloads():
    """Helper method that copies the workload folder to a temporary directory and returns it's
    name."""
    logger.info("Creating backup of old workloads.")
    temp_dir = mkdtemp()
    sh.cp(os.path.join(impala_home, "testdata", "workloads"),
          temp_dir, R=True, _out=tee, _err=tee)
    return temp_dir


def restore_workloads(source):
    """Restores the workload directory from source into the Impala tree"""
    logger.info("Restoring workload directory.")
    sh.cp(os.path.join(source, "workloads"), os.path.join(impala_home, "testdata"),
          R=True, _out=tee, _err=tee)

def main():
    parser = OptionParser()
    parser.add_option("-e", "--exec-options", help="Additional Impala exec options.")
    parser.add_option("-r", "--iterations", default=5, help=("Number of iterations per query. "
                                                       "Default is 5."))
    parser.add_option("-l", "--load", action="store_true",
                      help="Indicates if data needs to be loaded")
    parser.add_option("--no-build", dest="build", action="store_false", default=True,
                      help="If set, will not build Impala and simply run the benchmark.")
    parser.add_option("--no-perf", action="store_true",
                      help="If set, will not execute the workload")
    parser.add_option("-c", "--num-clients", default=1, help="Number of client connections.")
    parser.add_option("--query-filter", help="Regex to match query files to run.")
    parser.add_option("-s", "--scale", help="Scale factor for the workload.")
    parser.add_option("--table-formats", default="parquet/none", help="Table format")
    parser.add_option("-w", "--workload", default="targeted-perf",
                      choices=["tpch", "targeted-perf", "tpcds"], help="Workload to run.")
    parser.add_option("-i", "--num-impalads", default=1, help="Number of impalad processes.")
    parser.add_option("-b", "--baseline", help="Results of the baseline in form of a JSON file.")
    parser.add_option("-t", "--use-thirdparty-services", dest="start_other_services",
                      action="store_false", default=True,
                      help=("If this option is set, it will assume that thirdparty "
                            "services are already started"))
    parser.set_usage("single_node_perf_run.py [options] base_revision target_revision")

    options, args = parser.parse_args()

    if options.build and options.no_perf:
        print "Options --no-perf cannot be used without --no-build."
        sys.exit(1)

    if len(args) != 2 and options.build and not options.no_perf:
        parser.print_usage()
        sys.exit(1)

    if options.load and not options.workload in ("tpch", "targeted-perf"):
        print("Currently, only TPC-H data can be loaded automatically.")
        sys.exit(1)

    # Get the current directory
    cwd = os.getcwd()
    os.chdir(impala_home)

    # Save the current hash to be able to return to this place
    current_hash = sh.git("rev-parse", "--abbrev-ref", "HEAD").strip()
    if current_hash == "HEAD":
        current_hash = get_git_hash_for_name("HEAD")

    if options.build:
        hash_base = get_git_hash_for_name(args[0])
        hash_ref = get_git_hash_for_name(args[1])

    # Create the base directory to store the results in
    try:
        os.makedirs(os.path.join(impala_home, "perf_results"))
    except:
        logger.debug("Directory exists")

    temp_dir = mkdtemp(dir=os.path.join(impala_home, "perf_results"), prefix="perf_run_")
    workload_dir = backup_workloads()
    logger.info("Backup workloads to {0}".format(workload_dir))

    # Check if build is required
    if options.build:
        build(hash_base)
        restore_workloads(workload_dir)
    start_dependent_services(options.num_impalads, options.start_other_services)

    # Check if data needs to be loaded
    if options.workload in ("tpch", "targeted-perf") and options.load:
        load_tpch_data(options.scale)

    # If no test execution is required, the only logical option is to exit here.
    if options.no_perf: return

    workload_name = options.workload
    if options.scale: workload_name += ":_{0}".format(options.scale)

    run_workload(temp_dir, workload_name, options)

    # If no build is required, it does not make sense to execute the same
    # workload again, so skip this section.
    if options.build:
        build(hash_ref)
        restore_workloads(workload_dir)
        start_dependent_services(options.num_impalads, options.start_other_services)
        run_workload(temp_dir, workload_name, options)
        compare(temp_dir, hash_base, hash_ref)
        sh.cat(os.path.join(impala_home, "performance_result.txt"), _out=tee, _err=tee)
        sh.git.checkout(current_hash)
    else:
        # Check if a baseline file is provided and if this is the case produce a report
        if options.baseline:
            input = os.path.join(temp_dir, current_hash + ".json")
            report_benchmark_results(input, options.baseline,
                                     "Base: {0} vs Ref: {1}".format(
                                         options.baseline, current_hash))

    os.chdir(cwd)
    logger.info("Done.")


if __name__ == "__main__":
    main()
