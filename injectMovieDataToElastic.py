import pandas as pd
from ElasticDBInterface import ElasticDBInterface

if __name__ == "__main__":
	elasticDB = ElasticDBInterface()
	names = ["actors",
		"countries",
		"crew",
		"genres",
		"languages",
		"movies",
		"releases",
		"studios",
		"themes"]
		
	for name in names:
		elasticDB.deleteIndex(name)
		
		elasticDB.createIndex(name)

		data = pd.read_csv(f'movieData/{name}.csv', engine='pyarrow')
		
		dictionaries = data.to_dict(orient='records')

		elasticDB.insertDataFrame(dictionaries,name)
		
		data = elasticDB.getIndexData(name) #This just tests that the data received is correct.

		print("Index: ",name, "\ncolumns: ", data.keys(), "\nnumber of instances: ", len(data))
