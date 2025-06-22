import unittest
import pandas as pd
from scripts import etl_pipeline

class TestETLPipeline(unittest.TestCase):
    def test_extract(self):
        df = etl_pipeline.extract()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

    def test_transform(self):
        raw_df = pd.DataFrame({
            'order_date': ['2025-06-20', None],
            'product_id': [1, 2],
            'quantity': [5, 10],
            'sales_amount': ['100.0', '200.0']
        })
        transformed_df = etl_pipeline.transform(raw_df)
        self.assertFalse(transformed_df.isnull().values.any())
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(transformed_df['order_date']))

if __name__ == '__main__':
    unittest.main()
