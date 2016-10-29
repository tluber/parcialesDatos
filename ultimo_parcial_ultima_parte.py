
# coding: utf-8

# In[29]:

from operator import add
data = [
    ('1', '1', 3),
    ('1', '1', 6),
    ('1', '2', 6),
    ('2', '2', 1),
    ('1', '3', 4),
    ('2', '2', 5),
    ('3', '1', 2),
    ('3', '2', 6),
    ('3', '2', 6),
    ('2', '3', 6),
    ('3', '1', 4),
]

rdd = sc.parallelize(data)
rdd = rdd.map(lambda x: ((x[0], x[1]), x[2]))
rdd = rdd.reduceByKey(lambda x, y: x + y)
rdd = rdd.map(lambda x: (x[0][0], x[0][1], x[1]))
rdd.collect()


# In[ ]:



