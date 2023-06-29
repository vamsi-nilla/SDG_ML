import json
import requests
import pyodbc
import pandas as pd
from flask import Flask, request,render_template,send_file,Request,jsonify
from SDV_BULK_API_FILE.SDV_BULK_UPDATED_file_API import Bulk_Driver
from SDV_BULK_API_FILE.SDV_BULK_GET_Data_Display import Generated_data_display
from SDV_BULK_API_FILE.SDV_BULK_DB_DataSet_file import DB_Updates

app = Flask(__name__,template_folder='templates')

try:
    @app.route("/API/1.0/DataGenRequestSet", methods=['POST'])
    def data_gen_request_set():
        data = request.get_json()
        header_info = data["HeaderInfo"]
        input_set = data["InputSet"]
        Action = request.args.get('Action')
        header_info['Action'] = Action
        print("action header info",header_info)
        #print(input_set)
        if Action.lower() =='Generate'.lower():
            bulk_object = Bulk_Driver()
            responselist = bulk_object.bulk_driver_method(header_info, input_set)
            dat_frame = responselist[0]
            input_dict = responselist[1]

            display_object = Generated_data_display()
            response_display = display_object.Screen_Data_display(dat_frame, input_dict)

            response_display.to_excel(
                r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\output_file.xlsx', index=False)
            # print("second response")
            print("retun printed_display_data", response_display)
            rows = response_display.to_json(orient='records')
            return rows
        elif Action.lower() =='Draft'.lower():
            bulk_object = Bulk_Driver()
            responselist = bulk_object.bulk_driver_method(header_info, input_set)

            response = {'outputSet': responselist}
            #print('app',responselist)
            return response



    @app.route('/API/1.0/getGeoList',methods = ['GET'])
    def get_GeoList():

        Database_object = DB_Updates()
        df= Database_object.Retrieve_dropdown()
        print(df)
        temp1_df = df['Field_values'].str.split(',',expand=True)
        temp2_df = temp1_df[0].str.split('@',expand=True)
        temp2_df.rename(columns={0: 'countryKey',1:'countryName'}, inplace=True)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df_list = temp2_df.to_dict('records')
        response = {'geographySet':temp2_df_list}
        return response


    @app.route('/API/1.0/getEntityList', methods=['GET'])
    def get_EntityList():
        Database_object = DB_Updates()
        df= Database_object.Retrieve_dropdown()
        print(df)
        countryKey = request.args.get('CountryKey')
        print(countryKey)

        df = df[df['Field_values'].str.contains(countryKey)]
        print(df)
        temp1_df = df['Field_values'].str.split(',', expand=True)
        temp2_df = temp1_df[1].str.split('@', expand=True)
        temp2_df.rename(columns={0: 'entity', 1: 'entityName'}, inplace=True)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df['countryKey']=countryKey
        temp2_df_list = temp2_df.to_dict('records')
        response = {'entitySet': temp2_df_list}
        return response

    @app.route('/API/1.0/getClientList', methods=['GET'])
    def Get_ClientList():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()
        print(df)

        countryKey = request.args.get('CountryKey')
        print(countryKey)
        entity = request.args.get('entity')
        df = df[df['Field_values'].str.contains(countryKey)]
        df = df[df['Field_values'].str.contains(entity)]

        temp1_df = df['Field_values'].str.split(',', expand=True)
        temp2_df = temp1_df[2].str.split('@', expand=True)
        temp2_df.rename(columns={0: 'countryKey', 1: 'clientNum'}, inplace=True)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df['countryKey'] = countryKey
        temp2_df_list = temp2_df.to_dict('records')
        response = {'clientSet': temp2_df_list}
        return response


    @app.route('/API/1.0/getEngmtTypeList', methods=['GET'])
    def Get_Engagement_typeList():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()
        countryKey = request.args.get('CountryKey')
        print(countryKey)
        entity = request.args.get('entity')
        print(type(entity),entity)
        if '-' in entity:
            entity_list = entity.split('-')
            entity = "|".join(entity_list)

        df = df[df['Field_values'].str.contains(countryKey)]
        print("country",df)
        df = df[df['Field_values'].str.contains(str(entity))]
        print("entity", df)
        temp1_df = df['Field_values'].str.split(',', expand=True)
        print('temp1',temp1_df)
        temp2_df = temp1_df[3].str.split('@', expand=True)
        print('temp2before', temp2_df)
        temp2_df.rename(columns={0: 'engTypeIdNum', 1: 'projectType' , 2:'engTypeId', 3:'engTypeName',4:'BEZEI'}, inplace=True)
        print('temp2after', temp2_df)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df_list = temp2_df.to_dict('records')
        print(temp2_df_list)
        response = {'engTypeSet': temp2_df_list}
        print("response",response)
        return response

    @app.route('/API/1.0/getPartnerNumbers', methods=['GET'])
    def Get_PartnerNum_List():
        Database_object = DB_Updates()
        df = Database_object.Retrieve_dropdown()
        partner_dict_map = {'Z3':5,'Z4':4,'Z7':6,'Z6':7}
        countryKey = request.args.get('CountryKey')
        print(countryKey)
        entity = request.args.get('entity')
        print(type(entity),entity)
        if '-' in entity:
            entity_list = entity.split('-')
            entity = "|".join(entity_list)
        partnerType = request.args.get('partnerType')
        print(type(partnerType), partnerType)
        df = df[df['Field_values'].str.contains(countryKey)]
        print("country",df)
        df = df[df['Field_values'].str.contains(str(entity))]
        print("entity", df)
        df = df[df['Field_values'].str.contains(str(partnerType))]
        print("partnerType", df)
        temp1_df = df['Field_values'].str.split(',', expand=True)
        print('temp1',temp1_df)
        temp2_df = temp1_df[partner_dict_map[partnerType]].str.split('@', expand=True)
        print('temp2before', temp2_df)
        temp2_df.rename(columns={0: 'partnerTypeId', 1: 'partnerTypeIdDes' , 2:'partnerNum', 3:'partnerName'}, inplace=True)
        print('temp2after', temp2_df)
        temp2_df = temp2_df.drop_duplicates().reset_index(drop=True)
        temp2_df_list = temp2_df.to_dict('records')
        print(temp2_df_list)
        response = {'partnerNumSet': temp2_df_list}
        print("response",response)
        return response

    @app.route('/API/1.0/download_excel')
    def download_excel():
        excel_file_path = r'sample_synthetic/output_file.xlsx'
        return send_file(excel_file_path, as_attachment=True)


    @app.route('/API/1.0/display_data')
    def display_data():
        df = pd.read_excel(r'C:\Users\vamsikkrishna\PycharmProjects\pythonProject1\sample_synthetic\output_file.xlsx')
        rows = df.to_dict('records')

        # Get the column names
        columns = df.columns.tolist()

        return render_template(r'table.html', columns=columns, rows=rows)


    @app.route('/API/1.0/')
    def index():
        return render_template(r'index.html')

except Exception as e:
    print('Flask API call', e)


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=5000, debug=True)