import json, nltk, gensim,pyspark,re,textauger
from pyspark import *
from textblob import textblob
import pandas as pd
import matplotlib.pyplot as plt 
from textauger import textfeatures
from textauger import preprocessing
from textauger import textfeatures
# from nltk.sentiment.vader import SentimentIntensityAnalyzer as Vader


data_tweets = []

with open('#put tweet_file file path here') as json_data:
    data = json.loads(json_data)
    if len(d) > 1:
        for each in range(len(d)):
            data_tweets.append(data[each]) 
    else:
        data_tweets.append(data)

tweets = pd.DataFrame()
tweets['text'] = map(lambda tweet: tweet['text'].strip(), data_tweets)
tweets['user_name'] = map(lambda tweet: tweet['user']['name'].encode('utf-8'),data_tweets )
tweets['country'] = map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, data_tweets )
tweets['lang'] = map(lambda tweet: tweet['lang'],data_tweets )
tweets['coordinates'] = map(lambda tweet: tweet['coordinatres'],data_tweets )
tweets['location'] = map(lambda tweet: tweet['user']['location'],data_tweets )
tweets['retweet_count'] = map(lambda tweet: tweet['retweet_count'],data_tweets )


get_ipython().magic(u'matplotlib inline')

verba = [str(v.encode('utf-8')) for v in tweets.text.values.tolist()]

### Cleansed Tweets Using Regular Expressions ### 

tweets['text_clean'] = [re.sub(r"http\S+",'',v) for v in tweets.text.values.tolist()] 
tweets['text_clean'] = [re.sub(r"#/S+",'',v) for v in tweets.text_clean.values.tolist()] 
tweets['text_clean'] = [re.sub(r"@/S+",'',v) for v in tweets.text_clean.values.tolist()] 
tweets['text_clean'] = [re.sub(r"u'RT/S+",'',v) for v in tweets.text_clean.values.tolist()] 
tweets['text'] = [v.replace(r'','',v) for v in tweets.text.values.tolist()] 

tweets['text_clean'] = preprocessing.clean_text(text=tweets.text_clean.values,remove_short_tokens_flag=False,lemmatize_flag=True)

#tweets['sentiment_score'] = [textfeatures.score_sentiment(v)['compound'] for v in tweets.text_clean.values.tolist()]


textfeatures.score_sentiment(tweets['text_clean'][1])


tweets.loc[tweets['sentiment_score'] > 0.0, 'sentiment'] = 'positive'
tweets.loc[tweets['sentiment_score'] == 0.0, 'sentiment'] = 'neutral'
tweets.loc[tweets['sentiment_score'] < 0.0, 'sentiment'] = 'negative'

### Export to CSV and load DF ### 
tweets.to_csv("/Users/uun466/Desktop/Data-Science-Project/tweet_file.csv", encoding = 'utf-8')

df = sqlContext.read.load('#put tweet_file file path here',format='com.databricks.spark.csv',header='true',inferSchema='true')

df.show()