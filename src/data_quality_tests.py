import great_expectations as gx
import pandas as pd
from datetime import datetime
from great_expectations.core.expectation_configuration import ExpectationConfiguration

def run_data_tests():
    today = datetime.today().strftime('%m-%d-%Y')
    context = gx.get_context()

    dataframe = pd.read_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {today}.csv')
    name = "job_board_frame"

    datasource = context.sources.add_pandas(name="my_pandas_datasource")
    data_asset = datasource.add_dataframe_asset(name=name)
    my_batch_request = data_asset.build_batch_request(dataframe=dataframe)

    suite = context.add_or_update_expectation_suite("job_board_suite")

    validator = context.get_validator(
        batch_request=my_batch_request,
        expectation_suite_name="job_board_suite",
    )

    expectation_configuration_1 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column": "ID",
    },
    meta={
        "notes": {
            "format": "markdown",
            "content": "Making sure we have a unique identifier column", }
        },
    )
    
    expectation_configuration_2 = ExpectationConfiguration(
    expectation_type="expect_column_to_exist",
    kwargs={
        "column": "ID",
    },
    meta={
        "notes": {
            "format": "markdown",
            "content": "Making sure we have a unique identifier column", }
        },
    )
    
    expectation_configuration_3 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={
        "column": "ID",
    },
    meta={
        "notes": {
            "format": "markdown",
            "content": "Making sure we have a unique identifier column", }
        },
    )
    
    expectation_configuration_4 = ExpectationConfiguration(
    expectation_type="expect_table_columns_to_match_ordered_list",
    kwargs={
        'column_list': [
            'Report Date', 'Published At', 'ID', 'Title', 'Street', 'City' , 'Country Code', 
            'Address Text','Marker Icon', 'Workplace Type', 'Company Name', 
            'Company Size', 'Experience Level', 'Latitude', 'Longitude' ,
            'Remote interview', 'Remote', 'Open to Hire Ukrainians', 'Skills',
            'Employment Types', 'Salary From', 'Salary To'
        ]
    })
    

    expectation_configuration_5 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "Experience Level",
        "value_set": ['mid' 'senior' 'junior'],
    },
   
    )
    
    suite.add_expectation(expectation_configuration=expectation_configuration_1)
    suite.add_expectation(expectation_configuration=expectation_configuration_2)
    suite.add_expectation(expectation_configuration=expectation_configuration_3)
    suite.add_expectation(expectation_configuration=expectation_configuration_4)
    suite.add_expectation(expectation_configuration=expectation_configuration_5)
    context.save_expectation_suite(expectation_suite=suite)

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
    
    return checkpoint_result['success']


today = datetime.today().strftime('%m-%d-%Y')
context = gx.get_context()

dataframe = pd.read_csv(f'/home/jagac/projects/job-board-e2e/csv_data/data {today}.csv')
print(dataframe['Experience Level'].unique())