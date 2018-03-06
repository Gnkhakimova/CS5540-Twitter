from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream




consumer_key="LBiJTcvUFWxjG2cC5xYaRLaLQ"
consumer_secret="ExuqqOHAWE9Css3GiNQpZLBclwdxWJKgb7CBioMaqFa45dpujP"
access_token="926091370363793413-BXuuOZBlqHS8kUDWs54XVWgH8AEWm8s"
access_token_secret="FskYTykFTJnqplMod1HPZpxAkNr2Dn2ExKl0vdxJ4DTqP"

class StdOutListener(StreamListener):

    def on_data(self, data):
        with open('tweets_output.json','a') as tf:
            tf.write(data)           
            print(data)
            return True
    
if __name__ == '__main__':
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, StdOutListener())
stream.filter(track=['BBC','CNN','NEWS'])
