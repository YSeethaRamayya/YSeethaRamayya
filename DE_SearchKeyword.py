import json
import boto3
import pandas as pd
import re
from datetime import datetime
import logging
from io import *

s3=boto3.client('s3')
now=datetime.now()
today_date=now.strftime("%Y-%m-%d")
response = s3.get_object(Bucket = "test-data-project", Key="test-app-folder/DE_SKP.json")
content=response['Body']
proeprties_data=json.loads(content.read())
filename=today_date+"_"+proeprties_data['output_filename']
regex_sc="[/\\|,.#$%&*\\=\-\'\"<>?:;]"

class SearchKeywordPerformance:
	def __init__(self, filename=None):
		print("Finding search keyword and engine that generated most revenue")
		self.tsvKey=filename
	#User Defined Functions
	def sep_list(self,element):
		elements = str(element).split(",")
		items=[]
		for item in elements:
			items.append(item.split(";"))
		return items

	def search_key(self,element):
		search_item={}
		item=re.split(regex_sc, element)
		if 'q' in item:
			qi=item.index('q')
			search_item[item[4]]=item[qi+1].replace("+"," ")
		elif 'p' in item:
			qi=item.index('p')
			search_item[item[4]]=item[qi+1].replace("+"," ")
		elif 'k' in item:
			qi=item.index('k')
			search_item[item[4]]=item[qi+1].replace("+"," ")
		else:
			return ''
		return search_item
	
	def get_column(self,element, cotype):
		for i in element:
			if cotype=='Revenue':
				return i[3]
			elif cotype=='PR_name':
				return i[1]
				
def lambda_handler(event, context):
	input_key=proeprties_data['inbound_key']
	input_filename=proeprties_data['input_file_name']
	input_file=input_key+input_filename
	skwp=SearchKeywordPerformance(input_file)
	#Reading input file and applying filters
	bucket_name=proeprties_data['bucket_name']
	obj=s3.get_object(Bucket=bucket_name, Key=skwp.tsvKey)
	input_file_df=pd.read_csv(obj['Body'], encoding='utf-8', dtype=str, header=0, sep="\t")
	input_file_df.reset_index(level=0, inplace=True)
	input_file_event_list_df=input_file_df[input_file_df['event_list'].isin(['1','2'])]
	#Selecting Required columns from input dataframe.
	input_file_selected_cols_df = input_file_event_list_df[['hit_time_gmt','geo_city','event_list','product_list','referrer']]
	input_file_selected_cols_df1 = pd.DataFrame(columns = ['hit_time_gmt','geo_city','event_list','product_list','referrer'])
	for index, row in input_file_selected_cols_df.iterrows():
		split = row['product_list'].split(',')
		for pr_list in split:
			input_file_selected_cols_df1=input_file_selected_cols_df1.append(pd.DataFrame({'hit_time_gmt':[row['hit_time_gmt']], 'geo_city':[row['geo_city']], 'event_list':[row['event_list']],'product_list':[pr_list], 'referrer':[row['referrer']]}))

	input_file_selected_cols_df1=input_file_selected_cols_df1.reset_index(drop=True)
	#Splitting product_list column
	input_file_selected_cols_df['product_list']=input_file_selected_cols_df['product_list'].apply(lambda x:skwp.sep_list(x))
	#Getting search key from referrer
	input_file_selected_cols_df['referrer']=input_file_selected_cols_df['referrer'].apply(lambda x:skwp.search_key(x))
	#Filtering Records with event list 2 and referrer not null records, getting product_name and assigning Revenue value as 0 for event list 2 
	input_file_event_2_df=input_file_selected_cols_df.loc[input_file_selected_cols_df['event_list'].isin(['2']) & (input_file_selected_cols_df['referrer'] !='' )]
	input_file_event_2_df['Total_Revenue']=0
	input_file_event_2_df['Product_name']=input_file_event_2_df['product_list'].apply(lambda x: skwp.get_column(x,'PR_name'))
	#Getting Records with event_list 1, Total_revenue and product_name from input dataframe
	input_file_event_1_df=input_file_selected_cols_df.loc[input_file_selected_cols_df['event_list'].isin(['1'])]
	input_file_event_1_df['Total_Revenue']=input_file_event_1_df['product_list'].apply(lambda x: skwp.get_column(x, 'Revenue'))
	input_file_event_1_df['Product_name']=input_file_event_1_df['product_list'].apply(lambda x: skwp.get_column(x,'PR_name'))
	#Joining Records event_list 2 df and event_list 1 df on product name
	pre_output_df=pd.merge(input_file_event_2_df,input_file_event_1_df, on='Product_name')
	pre_output_df=pre_output_df[['referrer_x','Total_Revenue_y']]
	pre_output_df=pre_output_df.rename(columns={'referrer_x':'referrer','Total_Revenue_y':'Total_Revenue'})
	#Splitting records which are stored as dict in referrer column into two separate columns and dropping referrer col
	pre_output_df[['search_engine','search_key']]=pre_output_df['referrer'].astype(str).str.strip("{}").str.split(":", expand=True)
	output_df=pre_output_df.drop(['referrer'], axis=1)
	output_df['Total_Revenue']=output_df['Total_Revenue'].astype('int')
	#Grouping on search key and engine to get sum of total revenue for each product and sorting in descending
	output_df=output_df.groupby(['search_key','search_engine'], group_keys=False, as_index=False).sum()
	output_df=output_df.sort_values(by='Total_Revenue', ascending=False)
	output_df[['search_key','search_engine']]=output_df[['search_key','search_engine']].apply(lambda x:x.str.replace("'",""))
	#Writing to final output tab separated file.
	csv_buffer=StringIO()
	output_df.to_csv(csv_buffer, index=False, sep="\t")
	okey=proeprties_data['output_key']+filename #"test-data-folder/output-folder/"
	s3.put_object(Bucket=bucket_name, Key=okey, Body=csv_buffer.getvalue())
