#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright 2020 Alibaba Group Holding Limited. All Rights Reserved.
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
#

import importlib
import logging
import os
import random
import string
import sys

import numpy as np
import pytest

import graphscope
from graphscope.config import GSConfig as gs_config
from graphscope.dataset.ldbc import load_ldbc
from graphscope.dataset.modern_graph import load_modern_graph
from graphscope.dataset.ogbn_mag import load_ogbn_mag
from graphscope.framework.graph import Graph
from graphscope.framework.loader import Loader

graphscope.set_option(show_log=True)
graphscope.set_option(initializing_interactive_engine=False)

test_repo_dir = os.path.expandvars("${GS_TEST_DIR}")


@pytest.fixture
def sess():
    s = graphscope.session(cluster_type="hosts", num_workers=2)
    yield s
    s.close()


@pytest.fixture
def ogbn_mag_small():
    return "{}/ogbn_mag_small".format(test_repo_dir)


def demo(sess, ogbn_mag_small):
    graph = load_ogbn_mag(sess, ogbn_mag_small)

    # Interactive engine
    interactive = sess.gremlin(graph)
    papers = interactive.execute(
        "g.V().has('author', 'id', 2).out('writes').where(__.in('writes').has('id', 4307)).count()"
    ).one()


def test_demo(ogbn_mag_small):
    sess = graphscope.session(cluster_type="hosts", num_workers=2)
    demo(sess, ogbn_mag_small)
    sess.close()
