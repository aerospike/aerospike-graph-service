import os, sys, subprocess
import argparse
from python_on_whales import docker

# TODO: These need to be dynamic.
GRAPH_JAR = "aerospike-graph-gremlin/target/aerospike-graph-gremlin-2.4.0-SNAPSHOT.jar"
BULK_LOADER_JAR = "aerospike-graph-bulk-loader/target/aerospike-graph-bulk-loader-2.4.0-SNAPSHOT.jar"
SPARK_VERSION = "3.4.1"
SPARK_URL = "https://archive.apache.org/dist/spark/spark-{}/spark-{}-bin-hadoop3.tgz".format(SPARK_VERSION,
                                                                                             SPARK_VERSION)
SPARK_ZIP = "spark-{}.tgz".format(SPARK_VERSION)


class BuildArguments:
    def __init__(self):
        self.tags = None
        self.platforms = []
        self.push = False
        self.slim = False
        self.use_local = False

    def set_use_local(self, use_local):
        self.use_local = use_local

    def set_tags(self, tags):
        self.tags = tags

    def add_platform(self, platform):
        self.platforms.append(platform)

    def set_push(self, push):
        self.push = push

    def set_slim(self, slim):
        self.slim = slim

    def __str__(self):
        return f"tags: {self.tags}, platforms: {self.platforms}, push: {self.push}, slim: {self.slim}, use_local: {self.use_local}"


def main():
    build_args = parse_args()
    if not build_args.use_local:
        build_jars(build_args)
    if not build_args.slim:
        fetch_dependencies()
    build_docker(build_args)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tags", nargs="*", help="The tags to apply to the output image")
    parser.add_argument("--platforms", nargs="*", help="The platforms to build the image for")
    parser.add_argument("--push", action="store_true", help="Push the image to the registry")
    parser.add_argument("--slim", action="store_true", help="Slim image")
    parser.add_argument("--use_local", action="store_true", help="Use local jars")
    build_args = BuildArguments()
    args = parser.parse_args()
    if args.tags is None or len(args.tags) == 0:
        print("tags is required")
        parser.print_help()
        sys.exit(1)
    build_args.set_tags(args.tags)
    if args.platforms is not None:
        for platform in args.platforms:
            build_args.add_platform(platform)
    else:
        print("No platform provided, defaulting to linux/arm64 build.")
        build_args.add_platform("linux/amd64")
    build_args.set_push(args.push)
    build_args.set_slim(args.slim)
    build_args.set_use_local(args.use_local)
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


def build_jars(build_args):
    if not build_args.slim:
        run_command(
            "mvn -pl aerospike-graph-gremlin -pl aerospike-graph-bulk-loader -am -DskipTests=true clean install "
            "--no-transfer-progress")
    else:
        run_command("mvn -pl aerospike-graph-gremlin -am -DskipTests=true clean install --no-transfer-progress")


def build_docker(build_args):
    print(f"Building for platforms: {build_args.platforms}")
    docker_build_args = {
        "FIREFLY_GRAPH": GRAPH_JAR,
        "BULKLOADER": BULK_LOADER_JAR,
        "SPARK_ZIP": SPARK_ZIP,
        "SPARK_VERSION": SPARK_VERSION
    } if not build_args.slim else {
        "FIREFLY_GRAPH": GRAPH_JAR
    }
    # See https://gabrieldemarmiesse.github.io/python-on-whales/sub-commands/buildx/ for help.
    docker_file = "docker/Dockerfile-slim" if build_args.slim else "docker/Dockerfile"
    docker.buildx.build(".",
                        build_args=docker_build_args,
                        build_contexts={},
                        file=docker_file,
                        output={"type" : "oci"},
                        platforms=build_args.platforms,
                        load=True,
                        push=build_args.push,
                        tags=build_args.tags)


if __name__ == "__main__":
    main()
