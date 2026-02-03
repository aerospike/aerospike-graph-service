import os, sys, subprocess
import argparse
from python_on_whales import docker

GRAPH_JAR_DIRECTORY = "aerospike-graph-gremlin/target/"
BULK_LOADER_JAR_DIRECTORY = "aerospike-graph-bulk-loader/target/"
OLAP_JAR_DIRECTORY = "aerospike-graph-olap/target/"


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
    graph_jar, bulk_loader_jar, olap_jar = find_jars(build_args)
    build_docker(build_args, graph_jar, bulk_loader_jar, olap_jar)


def find_jars(build_args):
    graph_jar = None
    for path, dirs, files in os.walk(os.path.abspath(GRAPH_JAR_DIRECTORY)):
        for filename in files:
            if filename.startswith("aerospike-graph-gremlin") and filename.endswith(".jar"):
                graph_jar = os.path.join(path, filename)
                graph_jar = os.path.relpath(graph_jar, os.getcwd())
    if graph_jar is None:
        print("Could not find graph jar in directory: {}".format(GRAPH_JAR_DIRECTORY))
        sys.exit(1)

    if build_args.slim:
        return graph_jar, None, None

    bulk_loader_jar = None
    for path, dirs, files in os.walk(os.path.abspath(BULK_LOADER_JAR_DIRECTORY)):
        for filename in files:
            if filename.startswith("aerospike-graph-bulk-loader") and filename.endswith(".jar"):
                bulk_loader_jar = os.path.join(path, filename)
                bulk_loader_jar = os.path.relpath(bulk_loader_jar, os.getcwd())
    if bulk_loader_jar is None:
        print("Could not find bulk loader jar in directory: {}".format(BULK_LOADER_JAR_DIRECTORY))
        sys.exit(1)

    olap_jar = None
    for path, dirs, files in os.walk(os.path.abspath(OLAP_JAR_DIRECTORY)):
        for filename in files:
            if filename.startswith("aerospike-graph-olap") and filename.endswith(".jar"):
                olap_jar = os.path.join(path, filename)
                olap_jar = os.path.relpath(olap_jar, os.getcwd())
    if olap_jar is None:
        print("Could not find olap jar in directory: {}".format(OLAP_JAR_DIRECTORY))
        sys.exit(1)

    return graph_jar, bulk_loader_jar, olap_jar


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


def build_jars(build_args):
    if not build_args.slim:
        run_command(
            "mvn -pl aerospike-graph-gremlin -pl aerospike-graph-bulk-loader -pl aerospike-graph-olap -am -DskipTests=true clean install "
            "--no-transfer-progress")
    else:
        run_command("mvn -pl aerospike-graph-gremlin -am -DskipTests=true clean install --no-transfer-progress")


def build_docker(build_args, graph_jar, bulk_loader_jar, olap_jar):
    print(f"Building for platforms: {build_args.platforms}")
    docker_build_args = {
        "FIREFLY_GRAPH": graph_jar,
        "BULKLOADER": bulk_loader_jar,
        "OLAP": olap_jar
    } if not build_args.slim else {
        "FIREFLY_GRAPH": graph_jar
    }
    # See https://gabrieldemarmiesse.github.io/python-on-whales/sub-commands/buildx/ for help.
    docker_file = "docker/Dockerfile-slim" if build_args.slim else "docker/Dockerfile"
    try:
        docker.buildx.build(".",
                            build_args=docker_build_args,
                            build_contexts={},
                            file=docker_file,
                            platforms=build_args.platforms,
                            load=not build_args.push, # Only load if not pushing. Cannot load multi-arch.
                            push=build_args.push,
                            tags=build_args.tags)
    except Exception as e:
        print("Failed to build Docker image: {}".format(e))
        raise e


if __name__ == "__main__":
    main()
