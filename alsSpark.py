import numpy as np
import random
from numpy import array
from pyspark import SparkContext, SparkConf
from pyspark.mllib.recommendation import ALS
import sys

## maybe implement some similarity based recommendations later down the road
	# explicit
def cosine(a, b):
	dot_product = np.dot(a,b)
	numerator = dot_product
	denominator = (a**2)*(b**2)
	return (numerator / (float(denominator))) 

	# implicit
def jaccard(users_in_common, total_users1, total_users2):
    union = total_users1 + total_users2 - users_in_common
    return (users_in_common / (float(union))) if union else 0.0

# for cross validation.. do it for a more complete data-set
def make_training_and_test(rdd) :
	seed = random.seed(42)
	training = rdd.sample(False, 0.75, seed)
	test = rdd.sample(False, 0.25, seed)
	return training, test

''' perform lookups for value and id using the corresponding tables. returns tuple '''
def format_line(line, uid, val_to_hash, hashmap, id_hash) :
	split = line.split(",")
	return (id_hash[split[uid]], hashmap[split[val_to_hash]] )

''' returns a row of the user - product matrix. 0 indicates user has not used product, 1 indicates product use'''
def dichotomize(line) :
	x = np.repeat(0, 412)
	x[list(iter(line[1]))] = 1
	return (line[0], x)

''' transforms iterator to list '''
def tolist (line) :
	return (line[0], list(iter(line[1])))
	

if __name__ == '__main__' :
	'''
	Configure the Spark Context and other important variables
	'''
	conf = SparkConf()
	conf.setMaster("local")
	conf.setAppName("eventRecommend")
	sc = SparkContext(conf = conf)
	UID_INDEX = 1


	''' 
	Open the text file, and get field/column to get recommendations for
	'''
	print "Opening %s" % sys.argv[1]
	lines = sc.textFile(sys.argv[1])

	prompt = "\n Please select a field to recommend based off of these fields: \n %s" % (lines.first().split(","))
	print prompt
	selection = raw_input().lower()
	indexOfField = lines.first().split(",").index(selection)

		# remove the head row from the dataset
	lines = lines.filter(lambda line: line.split(",")[1] != "_uid")
	# get all events where the field value is not the empty string
	values = lines.filter(lambda line: line.split(",")[int(indexOfField)] != '""')
	

	'''
	Create Hashmap lookups for Product ID (normally a string) and User ID (normally a double)
	Apache Spark MLLib wants product.id and user.id to both be in integer format 

	'''
	## hashmap for products/values
	distinct_values = values.map(lambda l: l.split(",")[int(indexOfField)]).distinct().collect()
	# enumerate all distinct values, then reverse the number/value to create a dictionary/hashtable <-- doesn't utilize spark ATM. Fix later
	value_hash = dict(map(lambda pair: (pair[1], pair[0]), list(enumerate(distinct_values))))
	
	# Do the same thing for user ID's
	distinct_ids = values.map(lambda l: l.split(",")[1]).distinct().collect()
	id_hash = dict(map(lambda pair: (pair[1], pair[0]), list(enumerate(distinct_ids))))



	'''
	ALS ~ Alternating Least Squares method to recommendation

	ALS can be done in two scenarios - implicitly or explicitly. Since we are doing song recommendations in this case, 
	we are using ALS in the implicit case (because they don't vote on how much they like the song, for instance). 

	Thus, we need to create a binary matrix of all the songs vs. users, where 1 indicates that a user listened to a given song, 
	and 0 indicates that the user didn't listen to the song. 
	'''	
	# calls format_line to convert strings to integers based off of the hash table. Gets all songs(ints) for a user as an array
	data = values.map(lambda x: format_line(x, UID_INDEX, int(indexOfField), value_hash, id_hash)).distinct().groupByKey() # or cache()
	
	# consider all songs that a user hasn't listened to and store in a numpy array
	user_arrays = data.map(lambda x: dichotomize(x))

	# create (user.id, np.array) format ====> (user.id, product.id, viewed) double lambda FTW
	ratings = user_arrays.flatMap(lambda user: map(lambda(i, x): array([float(user[0]), float(i), float(x)]), enumerate(user[1])))

	# sample without replacement
	training, test = make_training_and_test(data)

	# train the ALS model  w/ 1 latent variables, and with 10 iterations (should be enough to converge ALS)
	model = ALS.trainImplicit(ratings, 1, 10)	
	

	'''
	ALS Prediction for Model:
		* For a given user ID, get the top n-recommendations 
		* Due to the sparsity of the matrix, many of the recommendations have very low confidence scores
	'''
	prompt = "\n Enter the user ID for the user you want to get recommendations for \n"
	print prompt
	id_to_recommend = float(raw_input)
	try :
		id_to_num = id_hash[id_to_recommend]
	except :
		print "ID %s is currently not stored in the predictive model :(" % id_to_recommend
		break

	# generate prediction space of the form : [(id, 0), (id, 1), ...., (id, num_songs)]
	prediction_space = sc.parallelize(map(lambda x: (id_to_num, x), range(0, len(value_hash))))


	# User song lookup.. map of user(key) with array of songs(value)
	user_song_map = data.map(lambda x: tolist(x)).collectAsMap()

	# get predictions  -> return as RDD w/ form : (confidence_score, product_id)
	preds = model.predictAll(prediction_space).map(lambda val: ((val[2] , val[1]))) \
				 .sortByKey(False) \
				 .filter(lambda x: x[1] not in user_song_map[id_to_num])

	

	# integer to song name map
	num_to_song = dict(map(lambda pair: (pair[0], pair[1]), list(enumerate(distinct_values))))

	# map the song_id number back to the song name
	output = map(lambda x: (x[0], num_to_song[x[1]]), preds)

	# prints the top three recommendations
	print output[:3]



