import codecs
from datetime import datetime
import json
# import requests
import os
import string
import sys
import time


def parse_json_tweet(line):
    tweet = json.loads(line)

    hashtags = [hashtag['text'] for hashtag in tweet['entities']['hashtags']]
    urls = [url['expanded_url'] for url in tweet['entities']['urls']]



    return [hashtags, urls]


'''start main'''
if __name__ == "__main__":
    file_timeordered_json_tweets = codecs.open("tweets_output.json", 'r', 'utf-8')
    fout = codecs.open("output.txt", 'w', 'utf-8')

    for line in file_timeordered_json_tweets:
        try:
            [hashtags, urls] = parse_json_tweet(line)
            fout.write(str([ hashtags,urls]) + "\n")
          #  fout.write("Hashtag"+ str([ hashtags]) + "URLs"+ str([urls]) + "\n")

        except:
            pass
    file_timeordered_json_tweets.close()
    fout.close()

