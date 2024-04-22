import pandas as pd
from pandas import DataFrame
from ElasticDBInterface import ElasticDBInterface

class QueryCreator:
	"""
	This class handles communicating with Elastic to insert and pull data for Movie Data
	Attributes:
	elasticDB: Elasticsearch Database to pull index data out of. 
	"""
	def __init__(self, elasticCreator: ElasticDBInterface):
		"""    
	       	Initalizes the Query Creator. 

		Args:
		    elasticCreator: ElasticDBInterface that communicates and talks to elasticsearch. 
		"""
		self.elasticDB = elasticCreator
		
	def getAverageHelper(self,genreUnderObservation:str,genreData:DataFrame)->float:
		"""    
	       	Gets Average Rating for a particular genre

		Args:
		    genreUnderObservation: Genre we want to observe
		    genreData: All the genreData from the genre index we query from
		    
		Returns:
		    Average rating of the genre
		"""
		sums = 0 
		count = 0
		for i in range(0,len(genreData)):
			movieGenre = genreData.iloc[i]
			if movieGenre["genre"] == genreUnderObservation:
				movie = self.elasticDB.getMovieDataByID(movieGenre["id"],"movies")
				if (len(movie) > 0):
					rating = movie["rating"]
					sums = sums + rating
					count = count + 1
				
		return float(sums/count)
		
	def getAveragePerGenre(self)->DataFrame:
		"""    
	       	Gets Average for all genres
		    
		Returns:
		    List of all the Genre and their respective avg rating. 
		"""
		data = {"Genere":[],
			"Average-Rating": []
		}
		genreData = self.elasticDB.getIndexData("genres")
		
		for genreUnderObservation in genreData["genre"].unique():
			data["Genere"].append(genreUnderObservation)
			data["Average-Rating"].append(self.getAverageHelper(genreUnderObservation,genreData))

		return pd.DataFrame(data)
			
	def getMovieByRatingRange(self,lower:int,upper:int)->DataFrame:
		"""    
	       	Gets Movie Data between a movie range. 

		Args:
		    lower: Lower bound rating
		    upper: upper bound rating
		    
		Returns:
		    Movies with a rating between that particular rng 
		"""
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
	def getAverageTimeBetweenRating(self,ratingRange:int)->DataFrame:
		"""    
	       	Gets average time based on rating buckets

		Args:
		    ratingRange: range between each bucket created ([1-2,2-3,3-4,4-5] if ratingRange is 1)
		    
		Returns:
		    Data of all the ranges with a respective average for each range. 
		"""
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
		
	def generateDirectorDictionary(self,lower:int,upper:int)->dict:
		"""    
	       	Gets Directors between a particular rating range 

		Args:
		    lower: lower rating bound
		    upper: upper rating bound
		    
		Returns:
		    Dictionary of directors and amount of movies they've directed
		"""
		df = self.getMovieByRatingRange(lower,upper)
		movieId=0
		#keys = []
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
					#keys.append(name)
		return crewDictionary
	def generateTopDirector(self,ratingRange:int)->DataFrame:
		"""    
	       	Gets top director between various ratings

		Args:
		    ratingRange: range between each bucket created ([1-2,2-3,3-4,4-5] if ratingRange is 1)
		    
		Returns:
		    Top director for each rating range. 
		"""
		data = {"Director": [],
			"Rating Range": [],
			"Total": []
		}
		lower = 1
		while(lower < 5):
			upper = lower + ratingRange
			crewDictionary = self.generateDirectorDictionary(lower,upper)
			keys = [key for key in crewDictionary]
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
	def generateActorsByRatingHelper(self,lower:int,upper:int)->dict:
		"""    
	       	Gets actors between a particular rating range

		Args:
		    lower: lower rating bound
		    upper: upper rating bound
		    
		Returns:
		    dictionary of actors between said range. 
		"""
		movieId=0
		#keys = []
		actorDictionary = {}
		df = self.getMovieByRatingRange(lower,upper)
		for i in range(0,len(df)):
			movieData = df.iloc[i]
			movieId = movieData["id"]
			actorQuery = {
			"query": {
				    		
				"bool":{"must":[
				    					
					{"match" : {"id": movieId}},
			
				    			
					]}

				},
			}
			actorTable = self.elasticDB.getIndexData("actors",actorQuery)
			if len(actorTable) > 0 :
				name = actorTable.iloc[0]["name"]
				if actorDictionary.get(name):
					actorDictionary[name] = actorDictionary[name] + 1
				else:
					actorDictionary[name] = 1
					#keys.append(name)
		return actorDictionary
	def getTop5Actors(self,rng:int)->DataFrame:
		"""    
	       	Gets Top 5 Actors between High Rating Range

		Args:
		    rng: size of the bucket we want top 5 actors in (4-5 if rng is 1)
		    
		Returns:
		    List of top 5 actors and number of ratings acquired. 
		"""
		dataframe = {"Actor":[],
			     "Number of Ratings":[]}
		actorDictionary = self.generateActorsByRatingHelper(5.0-rng,5.0)
		
		for i in range(0,5):
			keys = [key for key in actorDictionary]
			topActor = keys[0]
			for actor in keys:
				if actorDictionary[actor] > actorDictionary[topActor]:
					topActor = actor
					
			dataframe["Actor"].append(topActor)
			dataframe["Number of Ratings"].append(actorDictionary[topActor])
			del actorDictionary[topActor]
		return pd.DataFrame(dataframe)
		
		
		
if __name__ == '__main__':
	
	queryCreator = QueryCreator(ElasticDBInterface())
	#Query 1 Test
	print(queryCreator.getAveragePerGenre())
	queryCreator.getAveragePerGenre().to_csv("Query1.csv",index=False)
	#Query 2 Test
	print(queryCreator.getAverageTimeBetweenRating(1))
	queryCreator.getAverageTimeBetweenRating(1).to_csv("Query2.csv",index=False)
	
	#Query 3 Test
	print(queryCreator.generateTopDirector(1))
	queryCreator.generateTopDirector(1).to_csv("Query3.csv",index=False)
	#Query 4 Test
	print(queryCreator.getTop5Actors(1))
	queryCreator.getTop5Actors(1).to_csv("Query4.csv",index=False)



