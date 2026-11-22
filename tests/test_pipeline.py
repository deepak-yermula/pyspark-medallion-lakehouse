import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

try:
    from pyspark.sql import SparkSession
    from pipeline import build
except ImportError:
    SparkSession = None

ROOT = Path(__file__).resolve().parents[1]

@unittest.skipIf(SparkSession is None, 'PySpark is not installed')
class MedallionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master('local[2]')
                     .appName('PortfolioMedallionTests')
                     .config('spark.sql.shuffle.partitions', '2')
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_gold_aggregations(self):
        with tempfile.TemporaryDirectory() as tmp:
            gold = build(self.spark, ROOT / 'data', Path(tmp) / 'out')
            actual = {(r['sales_month'], r['country']): (Decimal(str(r['revenue'])), r['orders'])
                      for r in gold.collect()}
            expected = {('2026-11', 'Canada'): (Decimal('200.00'), 2),
                        ('2026-12', 'USA'): (Decimal('80.00'), 1)}
            self.assertEqual(actual, expected)

if __name__ == '__main__':
    unittest.main()
