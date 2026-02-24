import os
import shutil
import tempfile
import unittest
from pyspark.sql import SparkSession
import importlib.util
import sys

# Dynamically load local module to avoid collision with installed 'kafka' package
module_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'kafka', 'aadharpoc.py'))
spec = importlib.util.spec_from_file_location('aadharpoc_local', module_path)
aadharpoc = importlib.util.module_from_spec(spec)
sys.modules['aadharpoc_local'] = aadharpoc
spec.loader.exec_module(aadharpoc)


class AadharPocTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('aadharpoc-tests').getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_masking_aadhaar_pan_phone_email(self):
        rows = [
            ('1234 5678 9012', 'ABCDE1234F', 'john.doe@example.com', '+91-9876543210'),
        ]
        schema = ['Aadhaar', 'PAN', 'Email', 'Phone']
        df = self.spark.createDataFrame(rows, schema)

        # normalize and mask
        df = aadharpoc.normalize_columns(df)
        df = aadharpoc.trim_and_nullify(df)
        df = aadharpoc.mask_pii(df)

        res = df.collect()[0]
        # Aadhaar masked: last 4 digits 9012
        self.assertEqual(res['aadhaar'], '**** **** 9012')
        # PAN masked: keep last 3 chars '34F', mask rest with X (7 X)
        self.assertTrue(res['pan'].endswith('34F'))
        self.assertEqual(len(res['pan']) - 3, res['pan'][:-3].count('X'))
        # Phone masked: last 4 digits '3210'
        self.assertTrue(res['phone'].endswith('3210'))
        # Email masked: local-part masked except first letter -> jXXXXXXX@example.com
        self.assertTrue(res['email'].endswith('@example.com'))
        self.assertTrue(res['email'].startswith('j'))

    def test_process_writes_parquet_end_to_end(self):
        tmp_dir = tempfile.mkdtemp()
        try:
            csv_path = os.path.join(tmp_dir, 'sample_pii.csv')
            with open(csv_path, 'w', encoding='utf-8') as f:
                f.write('Aadhaar,PAN,Email,Phone\n')
                f.write('123456789012,ABCDE1234F,john.doe@example.com,9876543210\n')
                f.write('000011112222,XYZAB1234C,jane@example.org,9123456780\n')

            out_dir = os.path.join(tmp_dir, 'out')
            os.makedirs(out_dir, exist_ok=True)

            # run processing
            aadharpoc.process(csv_path, out_dir, self.spark)

            out_path = os.path.join(out_dir, os.path.splitext(os.path.basename(csv_path))[0])
            # Parquet files should exist under out_path
            self.assertTrue(os.path.exists(out_path))
            # Read back parquet and assert masked columns exist
            df = self.spark.read.parquet(out_path)
            cols = set(df.columns)
            self.assertIn('aadhaar', cols)
            self.assertIn('pan', cols)
            self.assertIn('email', cols)
            self.assertIn('phone', cols)

            rows = df.collect()
            self.assertGreaterEqual(len(rows), 2)
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)


if __name__ == '__main__':
    unittest.main()
