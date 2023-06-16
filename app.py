import json
import requests

import pandas as pd
from flask import Flask, request,render_template,send_file,Request,jsonify
import json
#from SDV_BULK_FILE.SDV_BULK_Driver_file import Bulk_Driver
from SDV_BULK_API_FILE.SDV_BULK_UPDATED_file_API import Bulk_Driver
from SDV_BULK_API_FILE.SDV_BULK_GET_Data_Display import Generated_data_display

app = Flask(__name__,template_folder='templates')

try:
    @app.route("/API/1.0/DataGenRequestSet", methods=['POST','GET'])
    def data_gen_request_set():
        # df = pd.read_excel(r'file_paths/Conca_df.xlsx')

        data = request.get_json()
        header_info = data["HeaderInfo"]
        input_set = data["InputSet"]
        #print(input_set)
        bulk_object = Bulk_Driver()
        responselist = bulk_object.bulk_driver_method(header_info, input_set)
        dat_frame = responselist[0]
        input_dict = responselist[1]

        display_object = Generated_data_display()
        response_display = display_object.Screen_Data_display(dat_frame,input_dict)
        #print("second response")
        print("retun printed_display_data",response_display)
        #rows = response_display.to_dict('records')
        #columns = response_display.columns.tolist()
        # render_template(r'table.html', columns=columns, rows=rows)
        rows = response_display.to_json(orient='records')
        return rows

        # return "execution completed"


    @app.route('/API/1.0/download_excel')
    def download_excel():
        excel_file_path = r'sample_synthetic/output_file.xlsx'
        return send_file(excel_file_path, as_attachment=True)


    @app.route('/API/1.0/display_data')
    def display_data():
        df = pd.read_excel(r'sample_synthetic/output_file.xlsx')
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