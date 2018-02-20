from flask import Flask, request, render_template, url_for
from predict_party import dem_or_rep
from src.features.fetch_tweet_features import generate_tweet_features, fetch_tweet_info
from tweepy.error import TweepError


# create a flask object
app = Flask(__name__)


# creates an association between the / page and the entry_page function (defaults to GET)
@app.route('/')
def entry_page():
    return render_template('index.html')


# creates an association between the /predict_recipe page and the render_message function
# (includes POST requests which allow users to enter in data via form)
@app.route('/predict_party/', methods=['GET', 'POST'])
def render_message():

    # User-entered URL
    url = request.form['tweet_url']

    # Error message if not valid Tweet URL
    messages = ["Twitter API is not available for this user"]

    # Generate features from tweet
    try:
        tweet_info = fetch_tweet_info(url)
        base_features, text_features, display_info = generate_tweet_features(tweet_info)
    except TweepError:
        return render_template('index.html', message=messages[0])

    # show user final message
    final_message, party = dem_or_rep(base_features, text_features)
    return render_template('index.html', profile_photo=display_info['profile_image'],
                           message=final_message, party_color=party)


if __name__ == '__main__':
    app.run(debug=True)