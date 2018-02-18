import pickle
import numpy as np

# read in the model
with open("models/gcb_clf_base_features.pkl", "rb") as mdl:
    clf = pickle.load(mdl)


# create a function to take in user-entered amounts and apply the model
def dem_or_rep(input_df, model=clf):

    reshaped_input = np.array(input_df).reshape(1, -1)

    # make a prediction
    prediction = model.predict(reshaped_input)[0]
    predict_prob = model.predict_proba(reshaped_input)[0]

    # return a message
    message_array = ["I'm {}% sure you're a Democrat!".format(round(predict_prob[0]*100, 2)),
                     "I'm {}% sure you're a Republican!".format(round(predict_prob[1]*100, 2))]

    party_color = ['blue', 'red']

    return message_array[prediction], party_color[prediction]

