import pandas as pd
from ElasticDBInterface import ElasticDBInterface

class InjectMovieData():
	"""
	This class handles communicating with Elastic to insert and pull data for Movie Data
	Attributes:
	elasticDB: Elasticsearch Database to pull index data out of. 
	"""
	def __init__(self):
		"""    
	       	Initalizes the Query Creator. 

		"""
		self.elasticDB = ElasticDBInterface()
	def injectData(self,factor:float)->None:
		"""    
	       	Injects data into elasticsearch
	       	
		Args:
		    factor: what fraction we want to divvy the data by
		"""
		names = ["movies",
			"actors",
			"countries",
			"crew",
			"genres",
			"languages",
			"releases",
			"studios",
			"themes"]
			
		for name in names:
			elasticDB.deleteIndex(name)
			
			elasticDB.createIndex(name)
			

			data = pd.read_csv(f'movieData/{name}.csv', engine='pyarrow')
			data = data[:int(len(data)*factor)].dropna()
			
			print("LENGTH: ", len(data))
			
			dictionaries = data.to_dict(orient='records')
			from elasticsearch.helpers import BulkIndexError
			
			try:
				elasticDB.insertDataFrame(dictionaries,name)
			except BulkIndexError as e:
			    # Handle bulk indexing errors
				print(f"{len(e.errors)} document(s) failed to index.")
				
				for error in e.errors:
					print(f"Error: {error['index']['_id']}, Reason: {error['index']['error']['reason']}")

			
			data = elasticDB.getIndexData(name) #This just tests that the data received is correct.

			print("Index: ",name, "\ncolumns: ", data.keys(), "\nnumber of instances: ", len(data))

if __name__ == "__main__":
	injectMovieData = InjectMovieData()
	injectMovieData.injectData(.25)

