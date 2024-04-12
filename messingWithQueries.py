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
print(elasticDB.getActorByName("Kofi Siriboe"))

print(elasticDB.getIndexData("actors"))
