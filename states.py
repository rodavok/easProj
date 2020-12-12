import matplotlib.pyplot as plt
import geopandas
import pandas as pd
import random
import numpy as np
from us_state_abbrev import us_state_abbrev, abbrev_us_state

#.shp files contains the data used to create a map of the USA
#states is a GeoDataFrame, which has the features of a regular dataframe, but has some additional methods for working with coordinate data
#states['NAME'] contains the name of the each state
states_map_path = 'data/usa-states-census-2014.shp'
states = geopandas.read_file(states_map_path)

#Use proper projection for North America
states = states.to_crs("EPSG:3395")

#List of mainland US States to match with states['NAME'] 
statenames = us_state_abbrev.keys()
#Just some random values for each state
statevals = [i for i in range(0,len(statenames))]

#Create a dictionary mapping state names to their values
examplevals = {key: val for (key, val) in zip(statenames, statevals)}

#Add values to plot the GeoDataFrame
states['statevals'] = [examplevals[x] for x in states['NAME']]


#Plot the map, and color the states based on the values in the newly added column
#I picked a cmap (colormap) that maps bolder colors to higher values
#More about colormaps here: https://matplotlib.org/3.1.0/tutorials/colors/colormaps.html
states.plot(column='statevals', cmap='PuRd', legend=True)
#*G*et the *c*urrently plotted *a*xes... 
ax = plt.gca()
#And hide them
ax.axes.xaxis.set_visible(False)
ax.axes.yaxis.set_visible(False)

plt.show()
