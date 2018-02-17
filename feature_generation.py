import feature_functions as feat_funcs
from sklearn.model_selection import train_test_split
import pickle

# Pickle base features (non-text)
all_tweets = feat_funcs.fetch_all_tweets(config_file='config.ini',
                                         conn_name='PostgresConfig')

relevant_features, all_possible_features = feat_funcs.generate_features(all_tweets)

relevant_features.to_pickle('data/feature_engineering.pkl')
all_possible_features.to_csv('data/all_feature_engineering.csv')

# Pickle text features
target = all_tweets['target']
features = all_tweets['text']

x_train, x_test, y_train, y_test = train_test_split(features, target,
                                                    test_size=.2,
                                                    random_state=42)

# Clean and tokenize words in train and test set before identifying feature set
clean_train = feat_funcs.tokenize_tweets(x_train)
clean_test = feat_funcs.tokenize_tweets(x_test)

word_feature_set = feat_funcs.find_top_used_words(tokenized_text=clean_train, top_x=1750)

# Write word_features file to project for future use
with open('data/word_features.pkl', 'wb') as wf:
    pickle.dump(word_feature_set, wf)

# Find features for training and test set
train = zip(clean_train, y_train)
test = zip(clean_test, y_test)

train_set = [(feat_funcs.find_text_features(tweet, feature_set=word_feature_set), ind) for (tweet, ind) in train]
test_set = [(feat_funcs.find_text_features(tweet, feature_set=word_feature_set), ind) for (tweet, ind) in test]

# Pickle training and test text feature sets for model selection
with open('data/train_text_features.pkl', 'wb') as wf:
    pickle.dump(train_set, wf)

with open('data/test_text_features.pkl', 'wb') as wf:
    pickle.dump(test_set, wf)