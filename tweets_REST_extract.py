### Extracts One Time Batch Pull of Tweets and Queues to Kafka Messaging 

import json, urllib3
from kafka import KafkaProducer
from pandas.io.json import json_normalize
from twitter import Twitter,OAuth, TwitterStream, TwitterHTTPError

tweet_num = 1000  # Number of tweets that need to be extracted * by 100

add_for_test = "im adding this as a test for git"
######## API URL UPDATE ############
data = json.load(urllib3.urlopen('http://127.0.0.1:6000/'))  #Insert Topic after slash at the end of url

topicName = data['topicName'][0]
print(topicName)

topicName = '  OR  '.join(topicName)

######### API CREDENTIALS #############

config = {}
exec('config.py',config)


########  TWITTER API OBJECT CREATION #########

twitter = Twitter(auth=OAuth(config["access_key"],config['access_secr'],config['consumer_key'],config['consumer_secr']) )

prod = KafkaProducer(serialize_vals=lamda v:json.dumps(v).encode('utf-8')) #Producer for writing json messages to kafka


file = open('C:\Users\Joe Malinao\Desktop\Projects\TweetSent','w')
i = 0
iterator = twitter.search.tweets(q=topicName, result_type='recent',lang='en',count = 100)
Nmin = float('-inf')
Nmax =float('+inf')

for i in range(tweet_num):
    print('Completed Search {}' % (iterator['search_metadata']['completed_in']))
    count = 0 
    for tweet in iterator['statuses']:
        count +=1 
        jsontweet = json.loads(tweet)

        Id_tweet = tweet['id']

        print(tweet['id'])
        if Id_tweet < Nmax:
            Id_min = Id_tweet
            Nmax = Id_tweet
        if Id_tweet > Nmin:
            Id_max = Id_tweet
            Nmin = Id_tweet
        producer.send('Twitter',tweet)
    json.dump(tweet,file)
    file.write('\n')

### Minimum ID ##

    i +=1 
    print(count)
    iterator = twitter.search.tweets(q = topicName,result_type='recent',lang='en',count=100,Id_max =Id_min)