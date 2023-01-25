import json
from urllib.request import Request, urlopen
import pandas as pd
import numpy as np

def run():
    """
    The run function is the main function that will be called when this module is executed. It will call all other functions in the correct order to generate a report.
    :return: The report of the sales and the details of each waiter
    """

    data_json = get_backup_data_dev()
    df = update_data_frame(data_json)
    obtain_detail_paid_data(df)

    #Attention by Zone
    create_csv(df.groupby(['zone'])['zone'].count(),"report_zone")

    #Sales by Zone
    create_csv(df.groupby(['zone','waiter'])['zone'].count(),"report_sales_by_zone")

    # Attention Sales by day
    create_csv(df[['total', 'Date_close_format']],"report_sales_by_day")

    # Attention Sales by month
    create_csv(df['total'].groupby(df['Date_close_format_mont']).sum().sort_values(ascending=False).reset_index(name='count'),"report_sales_by_month")

    # Check is payments is correct to the total payment
    headers_name=['Date','total','total_payments']
    create_csv_set_columns(df[['Date_close_format','total', 'check']],"report_exist_different_payment",headers_name)

    # Details the waiters work in diferenct zone and date
    headers_name=["day",'zone','waiter',"total"]
    create_csv_set_columns(df.groupby(['Date_close_format_mont','zone','waiter']).size().sort_values(ascending=False).reset_index(name='count'),"report_waiters_work",headers_name)

def get_backup_data_dev():
    """
    The get_backup_data_dev function downloads the backup data from a Google Cloud Storage bucket.
    It returns a list of dictionaries, where each dictionary contains the information for one sale.

    :return: A dictionary with the backup data from the dev storage
    """
    request=Request('https://storage.googleapis.com/backupdatadev/ejercicio/ventas.json')
    response = urlopen(request)
    data_read = response.read()
    return json.loads(data_read)

def update_data_frame(data_json):
    """
    The obtain_detail_paid_data function takes in a dataframe and returns a new dataframe with the total amount paid for each order.
        
    :param df: Obtain the dataframe that is being used
    :return: A dataframe with the sum of all payments per row
    """
    df = pd.json_normalize(data_json)
    df['Date_close_format'] = pd.to_datetime(df['date_closed']).dt.date
    df['Date_close_format_mont'] = pd.to_datetime(df['date_closed']).dt.to_period('M')
    df['check'] = 0
    return df

def obtain_detail_paid_data(df):
    """
    The obtain_detail_paid_data function takes in a dataframe and returns a new dataframe, with payments, calculate the total by event day.
    :param df: Store the dataframe that is passed to the function
    :return: A dataframe with the amount paid per order
    """
    df_new_2 = df[['payments']]
    list_data = [
        sum(d.get('amount', 0) for d in element)
        for element in df_new_2['payments']
    ]
    df['check'] = list_data
    return df

def create_csv(df_tmp,name_file):
    df_tmp.to_csv(f'{name_file}.csv', header=None, index=True, encoding="iso8859-1", sep ='\t',  mode='w')

def create_csv_set_columns(df_tmp,name_file,headers_name):
    df_tmp.to_csv(f'{name_file}.csv', header=headers_name, index=True, encoding="iso8859-1", sep ='\t',  mode='w')

if __name__ == "__main__":
    run()