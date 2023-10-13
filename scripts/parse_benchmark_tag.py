import sys

benchmarks = {'synthetic'}

default_instance_type = {
    '1g': 'n2d-standard-4',
    '2g': 'n2d-standard-4',
    '4g': 'n2d-standard-4',
    '8g': 'n2d-standard-4',
    '16g': 'n2d-highmem-4',
    '32g': 'n2d-highmem-8',
    '64g': 'n2d-highmem-16',
    '128g': 'n2d-highmem-32'
}

# Expansion factor of 10 and 50% overhead. Each disk is fixed at 400 GB.
size_to_ssd_count = {
    '1g': '1',
    '2g': '1',
    '4g': '1',
    '8g': '1',
    '16g': '1',
    '32g': '2',
    '64g': '4',
    '128g': '7'
}


def main(argv):
    tag = argv[1]
    split_tag = tag.split('-')
    benchmark_index = split_tag.index('benchmark')
    benchmark_name = split_tag[benchmark_index + 1]
    if benchmark_name not in benchmarks:
        raise ValueError(benchmark_name + ' is not a valid benchmark name')
    benchmark_size = split_tag[benchmark_index + 2]
    if size_to_ssd_count.get(benchmark_size) is None:
        raise ValueError('Benchmark size ' + benchmark_size + ' is invalid')
    benchmark_instance = default_instance_type[benchmark_size]
    if len(split_tag) == (benchmark_index + 6):
        benchmark_instance = '-'.join(split_tag[benchmark_index + 3:])
    elif len(split_tag) != (benchmark_index + 3):
        raise ValueError('Specified instance type ' + '-'.join(split_tag[benchmark_index + 3:]) + ' is invalid')
    data_size = 'data-size=' + benchmark_size
    server_instances = 'server-instances=3'
    instance_type = 'instance-type=' + benchmark_instance
    benchmark = 'benchmark=' + benchmark_name
    ssd_count = 'ssd-count=' + size_to_ssd_count[benchmark_size]

    with open('benchmark.properties', 'w') as properties:
        properties.write(f'{data_size}\n')
        properties.write(f'{server_instances}\n')
        properties.write(f'{instance_type}\n')
        properties.write(f'{benchmark}\n')
        properties.write(f'{ssd_count}\n')


if __name__ == "__main__":
    main(sys.argv)
