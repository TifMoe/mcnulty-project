import pickle
from src.models.ensemble_models import ensemble_base_text_models

# read in the models
with open("models/gcb_clf_base_features.pkl", "rb") as mdl:
    base_model = pickle.load(mdl)

with open("models/nb_clf_text_features.pkl", "rb") as mdl:
    text_model = pickle.load(mdl)


# create a function to take in user-entered amounts and apply the model
def dem_or_rep(base_features, text_features,
               base_model=base_model, text_model=text_model):

    # make a prediction
    predict_prob, prediction = ensemble_base_text_models(base_features=base_features,
                                                         text_features=text_features,
                                                         base_model=base_model,
                                                         text_model=text_model)

    # return a message
    message_array = ["{}% Democrat!".format(round((1-predict_prob)*100, 2)),
                     "{}% Republican!".format(round(predict_prob*100, 2))]

    party_color = ['#4c4cff', '#ff3232']
    print(party_color[prediction])

    return message_array[prediction], party_color[prediction]

