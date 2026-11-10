# Copyright 2022-2026 Aerospike, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import time

benchmarks = {'synthetic'}

storage_types = {'mmd', 'dmd', 'ddd'}

# C3/C3D "-lssd" variants bundle a fixed number of pre-attached local SSDs into the
# machine type itself (see size_to_ssd_count below for the count each tier gets).
default_instance_type = {
    '1g': 'c3-standard-4-lssd',
    '2g': 'c3-standard-4-lssd',
    '4g': 'c3-standard-4-lssd',
    '8g': 'c3d-highmem-8-lssd',
    '16g': 'c3d-highmem-30-lssd',
    '32g': 'c3d-highmem-60-lssd',
    '64g': 'c3d-highmem-90-lssd',
    '128g': 'c3d-highmem-180-lssd'
}

# Tried when the default instance type above has no capacity in any zone. "-lssd" capacity
# often runs out, so these are N2/N2D types with separately attached local SSDs, matching
# the default's memory as closely as possible. Each must accept size_to_ssd_count local SSDs:
# https://cloud.google.com/compute/docs/disks/local-ssd#lssd_disk_options
# No N2/N2D type has 128g's 1440 GB of memory; n2-highmem-128 has 864 GB.
default_fallback_instance_type = {
    '1g': 'n2d-standard-4',
    '2g': 'n2d-standard-4',
    '4g': 'n2d-standard-4',
    '8g': 'n2d-highmem-8',
    '16g': 'n2d-highmem-32',
    '32g': 'n2d-highmem-64',
    '64g': 'n2d-highmem-96',
    '128g': 'n2-highmem-128'
}

# Expansion factor of 10 and 50% overhead. Each disk is fixed at 400 GB.
# Must match the fixed local SSD count baked into the corresponding "-lssd"
# instance type in default_instance_type above.
size_to_ssd_count = {
    '1g': '1',
    '2g': '1',
    '4g': '1',
    '8g': '1',
    '16g': '2',
    '32g': '4',
    '64g': '8',
    '128g': '16'
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

    instance_offset_min = 3
    instance_offset_max = 6
    storage_type = 'mmd'
    if (benchmark_index + 3) < len(split_tag) and split_tag[benchmark_index + 3] in storage_types:
        storage_type = split_tag[benchmark_index + 3]
        instance_offset_min = instance_offset_min + 1
        instance_offset_max = instance_offset_max + 1

    benchmark_instance = default_instance_type[benchmark_size]
    fallback_instance = default_fallback_instance_type[benchmark_size]
    if len(split_tag) == (benchmark_index + instance_offset_max):
        benchmark_instance = '-'.join(split_tag[benchmark_index + instance_offset_min:])
        # An instance type named in the tag is used as is.
        fallback_instance = ''
    elif len(split_tag) != (benchmark_index + instance_offset_min):
        raise ValueError('Specified instance type ' + '-'.join(split_tag[benchmark_index + instance_offset_min:])
                         + ' is invalid')

    data_size = 'data-size=' + benchmark_size
    server_instances = 'server-instances=3'
    instance_type = 'instance-type=' + benchmark_instance
    fallback_instance_type = 'fallback-instance-type=' + fallback_instance
    benchmark = 'benchmark=' + benchmark_name
    ssd_count = 'ssd-count=' + size_to_ssd_count[benchmark_size]
    storage_type = 'storage-type=' + storage_type
    tag_hash = 'tag-hash=' + str((abs(hash(time.time())) + abs(hash(tag))) % (10 ** 8))

    with open('benchmark.properties', 'w') as properties:
        properties.write(f'{data_size}\n')
        properties.write(f'{server_instances}\n')
        properties.write(f'{instance_type}\n')
        properties.write(f'{fallback_instance_type}\n')
        properties.write(f'{benchmark}\n')
        properties.write(f'{ssd_count}\n')
        properties.write(f'{storage_type}\n')
        properties.write(f'{tag_hash}\n')


if __name__ == "__main__":
    main(sys.argv)
