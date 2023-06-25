import great_expectations as gx
from great_expectations.core.expectation_configuration import ExpectationConfiguration
import pandas as pd
from datetime import datetime

def run_data_tests(csv_path):
    today = datetime.today().strftime('%Y-%m-%d')
    context = gx.get_context()

    dataframe = pd.read_csv(f'{csv_path}\\data {today}.csv')
    name = "job_board_frame"

    datasource = context.sources.add_pandas(name="my_pandas_datasource")
    data_asset = datasource.add_dataframe_asset(name=name)
    my_batch_request = data_asset.build_batch_request(dataframe=dataframe)

    suite = context.add_or_update_expectation_suite("job_board_suite")

    expectation_configuration_1 = ExpectationConfiguration(
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
    
    expectation_configuration_2 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_not_be_null",
    kwargs={
        "column": "ID",
    },)
    
    expectation_configuration_3 = ExpectationConfiguration(
    expectation_type="expect_column_to_exist",
    kwargs={
        "column": "ID",
    },)
    
    expectation_configuration_4 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_unique",
    kwargs={
        "column": "ID",
    },)
    
    expectation_configuration_5 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "Experience Level",
        "value_set": ['mid' 'senior' 'junior'],
    },)
    
    expectation_configuration_6 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "Employment Types",
        "value_set": ['b2b' 'permanent' 'mandate_contract'],
    },)
    
    expectation_configuration_7 = ExpectationConfiguration(
    expectation_type="expect_column_values_to_be_in_set",
    kwargs={
        "column": "Remote",
        "value_set": [True, False],
    },)
    
    
    suite.add_expectation(expectation_configuration=expectation_configuration_1)
    suite.add_expectation(expectation_configuration=expectation_configuration_2)
    suite.add_expectation(expectation_configuration=expectation_configuration_3)
    suite.add_expectation(expectation_configuration=expectation_configuration_4)
    suite.add_expectation(expectation_configuration=expectation_configuration_5)
    suite.add_expectation(expectation_configuration=expectation_configuration_6)
    suite.add_expectation(expectation_configuration=expectation_configuration_7)
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

