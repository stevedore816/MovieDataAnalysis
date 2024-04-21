import pandas as pd
from pandas import DataFrame
from ElasticDBInterface import ElasticDBInterface

names = ["actors",
	"countries",
	"crew",
	"genres",
	"languages",
	"movies",
	"releases",
	"studios",
	"themes"]

class QueryCreator:
	def __init__(self, elasticCreator: ElasticDBInterface):
		self.elasticDB = elasticCreator
		
	def getAverageHelper(self,genreUnderObservation:str,genreData:DataFrame):
		sums = 0 
		count = 0
		for i in range(0,len(genreData)):
			movieGenre = genreData.iloc[i]
			if movieGenre["genre"] == genreUnderObservation:
				rating = self.elasticDB.getMovieDataByID(movieGenre["id"],"movies")["rating"]
				sums = sums + rating
				count = count + 1
				
		return sums/count
		
	def getAveragePerGenre(self):
		data = {"Genere":[],
			"Average-Rating": []
		}
		genreData = self.elasticDB.getIndexData("genres")
		
		for genreUnderObservation in genreData["genre"].unique():
			data["Genere"].append(genreUnderObservation)
			data["Average-Rating"].append(self.getAverageHelper(genreUnderObservation,genreData))

		return pd.DataFrame(data)
			
	def getMovieByRatingRange(self,lower:int,upper:int)->DataFrame:
		lowBracketRating = lower

		hightBracketRating = upper

		query = {
			"query": {
				    		
				"bool":{"must":[
				    					
					{"range": {"rating": {"gte": lowBracketRating,"lte": hightBracketRating}}},
										
				    			
					]}

				},
			}
			
		df = self.elasticDB.getIndexData("movies",query)
		return df
	def getAverageTimeBetweenRating(self,ratingRange:int):
		data = { "Rating":[],
			 "Average":[]
		}
		lower = 1
		while(lower < 5):
			upper = lower + ratingRange
			df = self.getMovieByRatingRange(lower,upper)
			print( "Range: ", lower , "-",upper)
			if len(df) > 0:
				#print("Mean: ", df["minute"].mean(),)
				data["Rating"].append(f"{lower}-{upper}")
				data["Average"].append(df["minute"].mean())
			lower = upper
		return pd.DataFrame(data)
		
	def generateDirectorDictionary(self,lower:int,upper:int):
		df = self.getMovieByRatingRange(lower,upper)
		movieId=0
		keys = []
		crewDictionary = {}
		for i in range(0,len(df)):
			movieData = df.iloc[i]
			movieId = movieData["id"]
			crewQuery = {
			"query": {
				    		
				"bool":{"must":[
				    					
					{"match" : {"id": movieId}},
					{"match" : {"role": "Director"}},
					
										
				    			
					]}

				},
			}
			directorTable = self.elasticDB.getIndexData("crew",crewQuery)
			if len(directorTable) > 0 :
				name = directorTable.iloc[0]["name"]
				if crewDictionary.get(name):
					crewDictionary[name] = crewDictionary[name] + 1
				else:
					crewDictionary[name] = 1
					keys.append(name)
		return crewDictionary,keys
	def generateTopDirector(self,ratingRange:int):
		data = {"Director": [],
			"Rating Range": [],
			"Total": []
		}
		lower = 1
		while(lower < 5):
			upper = lower + ratingRange
			crewDictionary,keys = self.generateDirectorDictionary(lower,upper)
			if len(keys) > 0:
				topDirector = keys[0]
				for director in keys:
					if crewDictionary[director] > crewDictionary[topDirector]:
						#print(crewDictionary[director])
						topDirector = director
						#print(topDirector)
				#print( "Range: ", lower , "-",upper)
				#print(topDirector, crewDictionary[topDirector])
				data["Director"].append(topDirector)
				data["Rating Range"].append(f"{lower}-{upper}")
				data["Total"].append(crewDictionary[topDirector])
				
			lower = upper

		return pd.DataFrame(data)
		
if __name__ == '__main__':
	
	queryCreator = QueryCreator(ElasticDBInterface())
	#Query 1 Test
	print(queryCreator.getAveragePerGenre())
	#Query 2 Test
	print(queryCreator.getAverageTimeBetweenRating(1))
	#Query 3 Test
	print(queryCreator.generateTopDirector(1))



