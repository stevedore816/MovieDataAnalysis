from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import pandas as pd
from pandas import DataFrame
import json
from datetime import datetime


class ElasticDBInterface():
	"""
	This class handles communicating with Elastic to insert and pull data for Movie Data
	Attributes:
	client: Elasticsearch Client that talks to Container Instance
	"""

	def __init__(self):
		"""
		Initalize the Elastic Controller Class

		"""

		self.client = Elasticsearch(['https://localhost:9200'],
					basic_auth = ('elastic','changeme'), #need to put this in seperate file before doing anything special
					ca_certs = '/home/steven-arroyo/Desktop/personalGit/MovieDataAnalysis/ElasticDockerInstance/http_ca.crt')
					
		#If Client Does not have a weatherIndex, we will create one. 	
		#self.client.indices.delete(index = 'weatherData')
		self.listIndexes()
				
    
 
        	
	def createIndex(self, indexName:str)->None:
		"""    
		Creates an index in Elastic
		
		Args:
		    indexName: name of the index we want to create

		"""
		
		try:
			if indexName not in self.listIndexes():
				print(f'creating {indexName} index...')
				#Bare minimum settings to create the elastic index. 
					
				parent_settings = {
					    "settings": {
						"number_of_shards": 1,
						"number_of_replicas": 0
					    },
					    "mappings": {
						"dynamic": "true"
						
					    }
					}
					
				self.client.indices.create(index = indexName,body = parent_settings)

		except Exception as e:
			print(f"An error occurred: {e}") 
	
	def deleteIndex(self, indexName:str)->None:
		"""    
		Deletes an index in Elastic
		
		Args:
		    indexName: name of the index we want to delete

		"""
		try:
			if indexName in self.listIndexes():
				print(f'deleting {indexName} index...')


				self.client.indices.delete(index = indexName)
		except Exception as e:
			print(f"An error occurred: {e}") 
	

	def listIndexes(self):
		"""    
	       	Lists all of the Indexes that exist in our Elastic instance


		Returns:
		    List of Indexes
		"""
		try:
			response = self.client.cat.indices(format='json')
	  
			indexes = [index['index'] for index in response]
			print("List of indexes: " + str(indexes))
			return indexes; 

		except Exception as e:
			print(f"An error occurred: {e}") 
			return indexes; 
	def insertDataFrame(self, df: list, indexName : str)-> None:
		"""    
	       	Bulk inserts a list of dictionaries originally from a dataframe

		Args:
		    df: list of dictionaries converted by pandas. 
		    index: index we are bulk inserting to
		"""

		success, failed = bulk(self.client, ({"_index" : indexName, "_source" : dic }  for dic in df) )

		if failed:
		    print(f"Failed to upload {len(failed)} documents")
		    
		self.client.indices.refresh(index=indexName)
		
	def getIndexData(self,indexName:str, query: dict = {},maxInstances:int = 1000) -> DataFrame:

		try:
			if query == {}:
				response = self.client.search(index=indexName,body = {"size":maxInstances})
			else:
				print("specialized query detected!")
				response = self.client.search(index = indexName, body = query) 
			if len(response['hits']['hits']) > 0: 
				dataframes = [pd.json_normalize(json_string['_source'])for json_string in response['hits']['hits']]
				return pd.concat(dataframes,ignore_index=True)
		except Exception as e:
			print(f"An error occurred: {e}")
			
	def getActorByName(self, actorName : str):
		query = {
			"query": {
				"match" : {"name": actorName},
				 }
				 

		  
			}
		return self.getIndexData("actors,movies", query = query)
		
	def getMovieDataByID(self, movieId : int, indexName:str):
		query = {
			"query": {
				"match" : {"id": movieId},
				 }
				 

		  
			}
		return self.getIndexData(indexName, query = query)
		
	def getMovieByName(self, movieName : str):
		query = {
		    "query": {
		    	"query_string":{"fields":["name"], "query": f'*{movieName}*' }
		    }
		}
		return self.getIndexData("movies", query = query)


		

if __name__ == '__main__':
	elasticDB = ElasticDBInterface()



	

