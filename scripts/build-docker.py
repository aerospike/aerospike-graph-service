import os, sys, subprocess
import argparse
import requests
from python_on_whales import docker

GRAPH_JAR = "aerospike-graph-gremlin/target/aerospike-graph-gremlin-2.4.0-SNAPSHOT.jar"
BULK_LOADER_JAR = "aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-2.4.0-SNAPSHOT.jar"
SPARK_VERSION = "3.4.1"
SPARK_URL = "https://archive.apache.org/dist/spark/spark-{}/spark-{}-bin-hadoop3.tgz".format(SPARK_VERSION,
                                                                                             SPARK_VERSION)
SPARK_ZIP = "spark-{}.tgz".format(SPARK_VERSION)


class BuildArguments:
    def __init__(self):
        self.output_tag = None
        self.platforms = []
        self.push = False

    def set_output_tag(self, output_tag):
        self.output_tag = output_tag

    def add_platform(self, platform):
        self.platforms.append(platform)

    def set_push(self, push):
        self.push = push

    def __str__(self):
        return f"output_tag: {self.output_tag}, platforms: {self.platforms}, push: {self.push}"


def main():
    build_args = parse_args()
    fetch_dependencies()
    build_jars()
    build_docker(build_args)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_tag", help="The tag to apply to the output image")
    parser.add_argument("--platforms", nargs="*", help="The platforms to build the image for")
    parser.add_argument("--push", action="store_true", help="Push the image to the registry")
    build_args = BuildArguments()
    args = parser.parse_args()
    if args.output_tag is None or len(args.output_tag) == 0:
        print("Output tag is required")
        parser.print_help()
        sys.exit(1)
    build_args.set_output_tag(args.output_tag)
    if args.platforms is not None:
        for platform in args.platforms:
            build_args.add_platform(platform)
    else:
        print("No platform provided, defaulting to linux/arm64 build.")
        build_args.add_platform("linux/amd64")
    build_args.set_push(args.push)
    print(build_args)
    for platform in build_args.platforms:
        if platform not in ["linux/amd64", "linux/arm64"]:
            print(f"Unsupported platform: {platform}")
            sys.exit(1)
    return build_args


def run_command(command):
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=os.environ.copy(),
                               shell=True, text=True)

    for line in iter(process.stdout.readline, ''):
        print(line, end='')

    process.stdout.close()
    return_code = process.wait()

    if return_code != 0:
        stderr_output = process.stderr.read()
        process.stderr.close()
        print(f"Error: {stderr_output}", file=sys.stderr)
        sys.exit(return_code)


def fetch_dependencies():
    if not os.path.exists(SPARK_ZIP):
        print("Downloading Spark")
        run_command(f"curl -L -o {SPARK_ZIP} {SPARK_URL}")
    else:
        print(f"{SPARK_ZIP} already exists, skipping download.")


def build_jars():
    run_command("mvn -pl aerospike-graph-gremlin -pl aerospike-graph-bulk-loader -am -DskipTests=true clean install "
       "--no-transfer-progress")
    pass


def build_docker(build_args):
    print(f"Building for platforms: {build_args.platforms}")
    docker_build_args = {
        "FIREFLY_GRAPH": GRAPH_JAR,
        "BULKLOADER": BULK_LOADER_JAR,
        "SPARK_ZIP": SPARK_ZIP,
        "SPARK_VERSION": SPARK_VERSION
    }
    # See https://gabrieldemarmiesse.github.io/python-on-whales/sub-commands/buildx/ for help.
    docker.buildx.build(".", build_args=docker_build_args, build_contexts={}, builder=None,
                        cache=True, cache_from=None, cache_to=None, file="docker/Dockerfile", labels={}, load=False, network=None,
                        output={}, platforms=build_args.platforms, progress='auto', provenance=None, pull=False, push=build_args.push, sbom=None,
                        secrets=[], ssh=None, tags=[build_args.output_tag], target=None, stream_logs=False)


if __name__ == "__main__":
    main()
