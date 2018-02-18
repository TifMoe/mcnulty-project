import numpy as np


def ensemble_base_text_models(base_features, base_model, text_features, text_model):
    """
    Function to ensemble together predictions from the base and text models
    """
    base_pred = base_model.predict_proba(base_features)[:, 1]
    text_pred = text_model.predict_proba(text_features)[:, 1]

    predict_prob = np.mean([base_pred[0], text_pred[0]])
    predict_class = predict_prob >= .5

    return predict_prob, predict_class
