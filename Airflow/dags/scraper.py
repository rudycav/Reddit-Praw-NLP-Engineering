from configparser import ConfigParser
import praw
from praw.models import MoreComments
import pandas as pd
import matplotlib.pyplot as plt
import json
import plotly
import plotly.express as px
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')
nltk.download('vader_lexicon')
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import re
import requests
from flask import Flask, render_template
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago


file = 'login.ini'
config = ConfigParser()
config.read(file)


app = Flask(__name__,template_folder='templates')

client_id = config.get('account','client_id')
client_secret = config.get('account','client_secret')
password = config.get('account','password')
user_agent = config.get('account','user_agent')
username = config.get('account','username')

reddit = praw.Reddit(
      client_id= client_id,
      client_secret = client_secret,
      password = password,
      user_agent = user_agent,
      username = username,
      check_for_async=False
  )

@app.route("/")

def reddit_scraper():
  with app.app_context():
  #coinmarketcap API call - limit to top 150 coins
    url='https://web-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    for start in range(1, 100, 1000):
        params = {
            'start': start,
            'limit': 150,
                }
        r = requests.get(url, params=params)
        data = r.json()

    ticker = [(crypto['symbol'].lower()) for crypto in (data['data'])]
    slug = [(crypto['name'].lower()) for crypto in (data['data'])]

    #subreddit to set parameters for title, author, body, score
    def reddit_scraper(reddit, subreddit_page, limit):
      df = pd.DataFrame()
      subreddit = reddit.subreddit(subreddit_page)
      hot_topic = subreddit.hot(limit=limit)

      for topics in hot_topic: 
        topic_list = topics.comments.list()
        for comments in topic_list:
          if isinstance(comments, MoreComments):
            continue
          df = df.append({'title': topics.title,
                          #'author': comments.author,
                          'comments': comments.body,
                          #'upvote_score': comments.score
                          },                                          
                          ignore_index=True) 

      return df

    wall = reddit_scraper(reddit, 'CryptoCurrency', 40)

    tokenizer = RegexpTokenizer(r'\w+')
    stop_words = stopwords.words('english')
    lemmatizer = WordNetLemmatizer()

    #text cleaning; tokenization, stop words, lemmatization
    def text_cleaner(df):
        cleaned_text = []
        for text in df:
            tokenize_words = tokenizer.tokenize(text)      
            stop_word_filter = [token.lower() for token in tokenize_words if token.lower() not in stop_words and not token.isdigit()]
            lemmitize_words = ([lemmatizer.lemmatize(token) for token in stop_word_filter])
            cleaned_text.extend(lemmitize_words)
        
        return cleaned_text

    def sentiment_analysis(df):
      return text_cleaner(df)
      '''
    #number visualization on top of bars
    def addlabels(x,y):
        for i in range(len(x)):
            plt.text(i, y[i], y[i], ha = 'center')
    '''
    sia = SIA()

    ps = []
    for comment in wall.comments:
        #polarity scores for SentimentIntensityAnalyzer
        score = sia.polarity_scores(comment)
        score['replies'] = comment
        ps.append(score)

    pol_df = pd.DataFrame.from_records(ps)

    #set bullish if 1, bearish if 0, neutral if 0
    pol_df['sentiment'] = 0
    pol_df.loc[pol_df['compound'] > 0.2, 'sentiment'] = 1
    pol_df.loc[pol_df['compound'] < -0.2, 'sentiment'] = -1


    bullish = sentiment_analysis(pol_df[pol_df.sentiment==1].replies)
    bearish = sentiment_analysis(pol_df[pol_df.sentiment==-1].replies)
    neutral = sentiment_analysis(pol_df[pol_df.sentiment==0].replies)

    #bullish ticker & names 
    df_bull_ticker = pd.DataFrame(nltk.FreqDist([crypto for bull in text_cleaner(bullish) for crypto in ticker if bull==crypto]).most_common(5), columns=['Name', 'Frequencies'])
    df_bull_slug = pd.DataFrame(nltk.FreqDist([crypto for bull in text_cleaner(bullish) for crypto in slug if bull==crypto]).most_common(5), columns=['Name', 'Frequencies'])
    #bearish ticker & names
    df_bear_ticker = pd.DataFrame(nltk.FreqDist([crypto for bear in text_cleaner(bearish) for crypto in ticker if bear==crypto]).most_common(5), columns=['Name', 'Frequencies'])
    df_bear_slug = pd.DataFrame(nltk.FreqDist([crypto for bear in text_cleaner(bearish) for crypto in slug if bear==crypto]).most_common(5), columns=['Name', 'Frequencies'])
    #neutral ticker & names
    df_neutral_ticker = pd.DataFrame(nltk.FreqDist([crypto for neu in text_cleaner(neutral) for crypto in ticker if neu==crypto]).most_common(5), columns=['Name', 'Frequencies'])
    df_neutral_slug = pd.DataFrame(nltk.FreqDist([crypto for neu in text_cleaner(neutral) for crypto in slug if neu==crypto]).most_common(5), columns=['Name', 'Frequencies'])

    
    bulltickerfig = px.bar(df_bull_ticker, x=df_bull_ticker.Name, y=df_bull_ticker.Frequencies)
    bullslugfig = px.bar(df_bull_slug, x=df_bull_slug.Name, y=df_bull_slug.Frequencies)
    bulltickergraphJSON = json.dumps(bulltickerfig, cls=plotly.utils.PlotlyJSONEncoder)
    bullsluggraphJSON = json.dumps(bullslugfig, cls=plotly.utils.PlotlyJSONEncoder)

    beartickerfig = px.bar(df_bear_ticker, x=df_bear_ticker.Name, y=df_bear_ticker.Frequencies)
    bearslugfig = px.bar(df_bear_slug, x=df_bear_slug.Name, y=df_bear_slug.Frequencies)
    beartickergraphJSON = json.dumps(beartickerfig, cls=plotly.utils.PlotlyJSONEncoder)
    bearsluggraphJSON = json.dumps(bearslugfig, cls=plotly.utils.PlotlyJSONEncoder)

    neutickerfig = px.bar(df_neutral_ticker, x=df_neutral_ticker.Name, y=df_neutral_ticker.Frequencies)
    neuslugfig = px.bar(df_neutral_slug, x=df_neutral_slug.Name, y=df_neutral_slug.Frequencies)
    neutickergraphJSON = json.dumps(neutickerfig, cls=plotly.utils.PlotlyJSONEncoder)
    neusluggraphJSON = json.dumps(neuslugfig, cls=plotly.utils.PlotlyJSONEncoder)

    return render_template('index.html', bulltickergraphJSON=bulltickergraphJSON, bullsluggraphJSON=bullsluggraphJSON, beartickergraphJSON=beartickergraphJSON, 
                            bearsluggraphJSON=bearsluggraphJSON, neutickergraphJSON=neutickergraphJSON, neusluggraphJSON=neusluggraphJSON)

if __name__ == '__main__':
    app.run(debug=True)

  
args = {
    'owner': 'Rudy',
    'start_date': days_ago(1) 
}

dag = DAG(
    dag_id='reddit_scraper',
    default_args=args,
    schedule_interval='@daily',
    catchup=False
)

with dag:
    reddit_scraper = PythonOperator(
      task_id = 'reddit_scraper',
      python_callable = reddit_scraper
    )


#reddit_scraper