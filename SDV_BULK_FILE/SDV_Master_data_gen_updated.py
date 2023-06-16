import pandas as pd
import numpy as np
import datetime
import json
import random
import string
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.sampling import Condition

class Master_data_gen():



    def Masterdata_transformations(self,ENG_MD_df,parameter_value, json_ui, concat_dataframe,Historic_data_locations,Pickle_file_locations,Data_dependency,Pikcle_file_description):
        print("Masterdata_transformations")
        #print(ENG_MD_df)
        #print(parameter_value)
        iterations = parameter_value.split('-')[1]
        iterations = int(iterations)
        result_dataframe = pd.DataFrame([])
        extracted_json = json.loads(json_ui)
        #print('result_dataframe',result_dataframe)

        for index,row in Pickle_file_locations.iterrows():
            if row['Pikcle_file_description'].lower()==Pikcle_file_description.lower():
                saved_location = row['Saved_location']


        synthesizer = GaussianCopulaSynthesizer.load(
            filepath=saved_location
        )

        #print(extracted_json['Number_of_Records'])
        #print(extracted_json['VBUKR'])
        #print(extracted_json['L2_levels'])


        if str(extracted_json['VBUKR']) == str(0):
            synthetic_data_temp = synthesizer.sample(num_rows=int(extracted_json['Number_of_Records']))
            synthetic_data = synthetic_data_temp.loc[sorted([*synthetic_data_temp.index] * iterations)].reset_index(
                drop=True)
            #synthetic_data.to_excel(
            #    r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\sample_synthetic_master.xlsx')
            #print(synthetic_data)

        else :

            VBUKR_condition = Condition(
                num_rows=int(extracted_json['Number_of_Records']),
                column_values={'Entity': extracted_json['VBUKR']}
            )
            synthetic_data_temp = synthesizer.sample_from_conditions(
                conditions=[VBUKR_condition],
                output_file_path=None
            )
            synthetic_data = synthetic_data_temp.loc[sorted([*synthetic_data_temp.index] * iterations)].reset_index(
                drop=True)
            #synthetic_data.to_excel(
            #   r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\sample_synthetic_master.xlsx')
            #print(synthetic_data)

        if int(extracted_json['L2_levels']) == int(1):
            synthetic_data = synthetic_data

        else:
            synthetic_concat_dataframe = pd.DataFrame([])
            number_of_level2_required = int(extracted_json['L2_levels'])
            #print("masterdata",number_of_level2_required)
            for index,row in synthetic_data.iterrows():
                Client_entity_condition = Condition(
                    num_rows=number_of_level2_required,
                    column_values={'Entity': row['Entity'],'Client' : row['Client']}
                    )
                synthetic_data_temp = synthesizer.sample_from_conditions(
                        conditions=[Client_entity_condition],
                        output_file_path=None
                    )
                synthetic_concat_dataframe = pd.concat([synthetic_concat_dataframe,synthetic_data_temp],axis = 0).reset_index(drop=True)

                synthetic_data = synthetic_concat_dataframe










            #return (synthetic_data)


        # df = pd.melt(synthetic_data, id_vars='PRJ-PRPS-WBSELEMENT-S', value_vars=['Y3', 'Z3', 'Z4', 'Z6', 'Z1', 'Z2', 'Z8', 'Z9'],
        #              var_name='PRJ-IHPA-PARVW-S', value_name='PRJ-IHPA-PARNR-S')
        # g = df.merge(synthetic_data, how='inner', on='PRJ-PRPS-WBSELEMENT-S')
        # g.to_excel(
        #     r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\sample_synthetic_master.xlsx')

        #return(g)

        return (synthetic_data)












