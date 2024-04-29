ElasticSearch LetterBoxd Visualizations

Download Dataset:
	Download Kagle Data into MovieData/
	https://www.kaggle.com/datasets/gsimonx37/letterboxd

Standup Elasticsearch:
	You will need this to inject the movie data into elastic 


	Need to haves: Docker

	1) Go into ElasticDocker Instance and run ./startInstance

	2) This will take you to the instances server to create the http crt, just type exit to leave it.
	
	
	
	
Inject data into elasticsearch:

	1) pip install -r requirements.txt
	
	2) python3 injectMovieDataToElastic.py
	

Pull Visualizations into CSVs:
	1) python3 messingWithQueries.py
	
Pull Visualizations into plots w/ matplotlib:
	1) python3 Plotter.py 



