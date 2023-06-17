import great_expectations as gx
import pandas as pd
from datetime import datetime, timedelta
import sys

today = datetime.today().strftime('%m-%d-%Y')
context = gx.get_context()

dataframe = pd.read_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {today}.csv')
name = "job_board_frame"

datasource = context.sources.add_pandas(name="my_pandas_datasource")
data_asset = datasource.add_dataframe_asset(name=name)
my_batch_request = data_asset.build_batch_request(dataframe=dataframe)

context.add_or_update_expectation_suite("job_board_suite")

validator = context.get_validator(
    batch_request=my_batch_request,
    expectation_suite_name="job_board_suite",
)

validator.expect_column_values_to_not_be_null('ID')
validator.expect_column_to_exist('ID')
validator.expect_column_values_to_be_unique('ID')

validator.save_expectation_suite(discard_failed_expectations=False)

checkpoint = gx.checkpoint.SimpleCheckpoint(
    name="my_checkpoint",
    data_context=context,
    validations=[
        {
            "batch_request": my_batch_request,
            "expectation_suite_name": "job_board_suite",
        },
    ],
)

checkpoint_result = checkpoint.run()
context.build_data_docs()
context.add_checkpoint(checkpoint=checkpoint)