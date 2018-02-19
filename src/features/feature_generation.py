from src.features import feature_functions as feat_funcs
from sklearn.model_selection import train_test_split
from flask import Flask


app = Flask(__name__)


@app.cli.command()
def pickle_all_features():
    """
    Generate new features from all available data
    """
    all_tweets = feat_funcs.fetch_all_tweets(config_file='config.ini',
                                             conn_name='PostgresConfig')

    # Pickle base features for model on meta data
    base_features = feat_funcs.generate_features(all_tweets)
    base_features.to_pickle('data/processed/base_features.pkl')

    # Pickle text features for future predictions
    tweet_text = all_tweets['text']

    # Create features for top 1750 most common words
    text_features = feat_funcs.generate_common_word_features(tweet_text,
                                                             pickle_new_features=True,
                                                             word_feature_filename='all_word_features')

    text_features.to_pickle('data/processed/all_text_features.pkl')

    target = base_features['target']
    target.to_pickle('data/processed/all_target.pkl')


@app.cli.command()
def pickle_train_test_features():
    """
    Generate new text features for model evaluation
    """
    all_tweets = feat_funcs.fetch_all_tweets(config_file='config.ini',
                                             conn_name='PostgresConfig')

    all_tweets['target'] = all_tweets['party'].replace({'Republican': 1, 'Democrat': 0})

    # Pickle text features
    target = all_tweets['target']
    features = all_tweets['text']

    x_train, x_test, y_train, y_test = train_test_split(features, target,
                                                        test_size=.2,
                                                        random_state=42)

    # Clean and tokenize words in train and test set before identifying feature set
    train_features = feat_funcs.generate_common_word_features(x_train,
                                                              pickle_new_features=True,
                                                              word_feature_filename='train_word_features')

    test_features = feat_funcs.generate_common_word_features(x_test,
                                                             pickle_new_features=False,
                                                             word_feature_filename='train_word_features')

    train_features['target'] = list(y_train)
    test_features['target'] = list(y_test)

    train_features.to_pickle('data/processed/train_text_features.pkl')
    test_features.to_pickle('data/processed/test_text_features.pkl')


if __name__ == '__main__':
    pickle_all_features()
    pickle_train_test_features()
