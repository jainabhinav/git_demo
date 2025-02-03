from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from gem_testing.graph.filter_customer_id_range import *
from gem_testing.config.ConfigStore import *


class filter_customer_id_rangeTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/gem_testing/graph/filter_customer_id_range/in0/schema.json',
            'test/resources/data/gem_testing/graph/filter_customer_id_range/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/gem_testing/graph/filter_customer_id_range/out/schema.json',
            'test/resources/data/gem_testing/graph/filter_customer_id_range/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = filter_customer_id_range(self.spark, dfIn0)
        assertDFEquals(dfOut.select("customer_id"), dfOutComputed.select("customer_id"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/config/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
