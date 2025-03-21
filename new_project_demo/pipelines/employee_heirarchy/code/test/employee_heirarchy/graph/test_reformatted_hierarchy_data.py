from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from argparse import Namespace
from prophecy.test import BaseTestCase
from prophecy.test.utils import *
from employee_heirarchy.graph.reformatted_hierarchy_data import *
from employee_heirarchy.config.ConfigStore import *


class reformatted_hierarchy_dataTest(BaseTestCase):

    def test_unit_test_(self):
        dfIn0 = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/employee_heirarchy/graph/reformatted_hierarchy_data/in0/schema.json',
            'test/resources/data/employee_heirarchy/graph/reformatted_hierarchy_data/in0/data/test_unit_test_.json',
            'in0'
        )
        dfOut = createDfFromResourceFiles(
            self.spark,
            'test/resources/data/employee_heirarchy/graph/reformatted_hierarchy_data/out/schema.json',
            'test/resources/data/employee_heirarchy/graph/reformatted_hierarchy_data/out/data/test_unit_test_.json',
            'out'
        )
        dfOutComputed = reformatted_hierarchy_data(self.spark, dfIn0)
        assertDFEquals(dfOut.select("AUX_SRC_SYS"), dfOutComputed.select("AUX_SRC_SYS"), self.maxUnequalRowsToShow)

    def setUp(self):
        BaseTestCase.setUp(self)
        import os
        fabricName = os.environ['FABRIC_NAME']
        Utils.initializeFromArgs(
            self.spark,
            Namespace(
              file = f"configs/resources/employee_heirarchy/{fabricName}.json",
              config = None,
              overrideJson = None,
              defaultConfFile = None
            )
        )
