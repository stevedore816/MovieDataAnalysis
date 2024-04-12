import pandas as pd
from ElasticDBInterface import ElasticDBInterface

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
#print(elasticDB.getActorByName("Kofi Siriboe"))


print(elasticDB.getMovieDataByID(1000013,"actors")["name"])



print(elasticDB.getIndexData("movies")["id"].min(),elasticDB.getIndexData("movies")["id"].max())

print(elasticDB.getMovieByName("Barb"))
