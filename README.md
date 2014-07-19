### Potent Tomahawk

> Event level user-product recommendations using Apache Spark, and ALS (alternating least squares). 

#### Prerequisites

- Get dependencies: 
	- Hadoop (I use apache hadoop 2.4.1)
	- Apache Spark (1.0.1) 
	- numpy

#### Example

MLLib wants the user-product values to be in a user/product matrix, so this outlines how to get event-stream data in this format :

```
user_id1, field1, field2, ..., fieldN
user_id2, field1, field2, ..., fieldN
...
user_idN, field1, field2, ..., fieldN
```

into a matrix so that we can perform user-field recommenadations and also item-item recommendations. 

To run spark programs, I included a brief/sloppy shell script. 


