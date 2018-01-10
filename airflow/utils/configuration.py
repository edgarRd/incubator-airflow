# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import unicode_literals
from __future__ import absolute_import

import os
import json
from contextlib import contextmanager

from airflow import configuration as conf
from tempfile import mkstemp


MAIN_SECTIONS = [
    'core', 'smtp', 'scheduler', 'webserver', 'hive'
]


def __write_tmp_config(cfg_dict):
    temp_fd, cfg_path = mkstemp()

    with os.fdopen(temp_fd, 'w') as temp_file:
        json.dump(cfg_dict, temp_file)

    return cfg_path


@contextmanager
def tmp_copy_configuration():
    """
    Creates a temporary file with the full copy of the configuration file. This context
    manager will remove the temporary file created on closing.
    :return: a path to the temp file with the copy of the configuration
    :type: string
    """
    tmp_conf_path = None
    try:
        tmp_conf_path = __write_tmp_config(conf.as_dict(display_sensitive=True))
        yield tmp_conf_path
    finally:
        if tmp_conf_path and os.path.isfile(tmp_conf_path):
            os.remove(tmp_conf_path)


def configuration_subset(sections):
    """
    Creates a temporary file with a subset of the configuration specified by the sections
    in the parameters, copy only those sections and returning a path for the temp file.
    :return: a path to the temp file with the configuration subset.
    :type: string
    """
    cfg_subset = dict()
    cfg_dict = conf.as_dict(display_sensitive=True)
    for section in sections:
        cfg_subset[section] = cfg_dict.get(section, {})

    return __write_tmp_config(cfg_subset)
