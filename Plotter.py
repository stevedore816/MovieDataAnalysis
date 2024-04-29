from messingWithQueries import QueryCreator
from ElasticDBInterface import ElasticDBInterface
import pandas as pd
import matplotlib.pyplot as plt
import threading
class Plotter():
	"""
	This class handles creating subplots for matplotlib
	Attributes:
	queryCreator: Query Creator used that interfaces with ElasticDB to make and pull from indexes. 
	"""
	def __init__(self,queryCreator:QueryCreator):
		"""    
	       	Initalizes the Plotter. 

		Args:
		    queryCreator: query creator that will be used to pull dataframes from elastic indexes. 
		"""
		self.queryCreator = queryCreator
	def plotAveragePerGenre(self,plt)->None:
		"""    
	       	Plots Average Rating for a particular genre

		Args:
		    plt: matplot subplot that we use to form our figure (Cannot import type as it is private.)
		"""
		avgPerGenre = self.queryCreator.getAveragePerGenre()
		#avgPerGenre = avgPerGenre.sort_values(by=["Average-Rating","Genre"])
		for cnt, genre in enumerate(avgPerGenre["Genre"]):
			print(genre,cnt)
			rating = float(avgPerGenre["Average-Rating"].get(cnt))
			bar = plt.barh(genre,rating, align='center', alpha=0.5,color='skyblue')
			plt.bar_label(bar,label_type='center')
			plt.axvline(x=avgPerGenre["Average-Rating"].mean(), color='cyan', linestyle='--', linewidth=.2,alpha=.5)  # Add a line along the x-axis
		#plt.plot(avgPerGenre["Genre"],avgPerGenre["Average-Rating"])
		plt.set_title("Average Rating By Genre")
		plt.set_ylabel("Genre")
		plt.set_xlabel("Rating")
		#plt.show()
		
	def plotAvgTimePerRating(self,plt)->None:
		"""    
	       	Plots Average Time for Ratings Range

		Args:
		    plt: matplot subplot that we use to form our figure
		"""
		avgPerRating = self.queryCreator.getAverageTimeBetweenRating(1)
		
		for cnt,rating in enumerate(avgPerRating["Rating"]):
			avg = avgPerRating["Average"][cnt]
			bar = plt.bar(rating,avg, align='center', alpha=0.5,color='salmon')
			plt.bar_label(bar,label_type='center')
			
		plt.set_title("Average Movie Length Per Rating")
		plt.set_ylabel("Minutes")
		plt.set_xlabel("Rating")
			
		#plt.show()

	def plotTopDirectorPerRating(self,plt):	
		"""    
	       	Plots Top Director By Rating Ranges

		Args:
		    plt: matplot subplot that we use to form our figure
		"""
		directorPerRating = self.queryCreator.generateTopDirector(1) #Director, Rating Range, Total 
		#colors = ['salmon','skyblue','cyan','periwinkle']
		for cnt,director in enumerate(directorPerRating["Director"]):
			ratingRng = directorPerRating["Rating Range"][cnt]
			total = directorPerRating["Total"][cnt]
			bar = plt.bar(director,total,label=ratingRng, align='center', alpha=0.5)
			#plt.text("THis",director,total)
			plt.bar_label(bar,label_type='center')
		plt.set_title("Top Director By Rating Range")
		plt.set_ylabel("Total Movies")
		plt.set_xlabel("Director")
		plt.legend(title='Rating Ranges')
		#plt.show()

	
	def plotTop5Actors(self,plt)->None:
		"""    
	       	Plots Top 5 Actors between 4-5 stars. 

		Args:
		    plt: matplot subplot that we use to form our figure
		"""
		rng = 1
		actorPerRating = self.queryCreator.getTop5Actors(rng) #Actor,Number of Ratings
		
		plt.set_title(f"Actors in Movies Between {5-rng} and 5 stars")
		
		for cnt,actor in enumerate(actorPerRating["Actor"]):
			rating = float(actorPerRating["Number of Ratings"].get(cnt))
			bar = plt.barh(actor,rating, align='center', alpha=0.5,color='limegreen')
			plt.bar_label(bar)
		
		plt.set_ylabel("Actors")
		plt.set_xlabel("Number of Movies")
			
		#plt.show()
		
if __name__ == '__main__':
	plotter = Plotter(QueryCreator(ElasticDBInterface()))
	
	fig, axs = plt.subplots(2, 2)
	
	plotter.plotAveragePerGenre(axs[0][0])
	plotter.plotAvgTimePerRating(axs[0][1])
	plotter.plotTop5Actors(axs[1][0])
	plotter.plotTopDirectorPerRating(axs[1][1])
	plt.show()

