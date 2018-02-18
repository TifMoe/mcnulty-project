import numpy as np
import operator
import pandas as pd
from pandas_ml import ConfusionMatrix
from sklearn import model_selection
from sklearn.metrics import log_loss, roc_auc_score, classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier


def find_best_classifier(features, response, set_seed, k_folds, crossval_scoring):

    """Rapidly test multiple classifiers on a data set using cross validation.
        Evaluate performance to find model with highest score as defined by
        the 'crossval_scoring' argument ('accuracy','roc_auc', 'precision', 'recall', etc)"""

    if isinstance(features, np.ndarray) and isinstance(response, np.ndarray):

        # Split features and response into training and validation sets
        validation_size = 0.20
        seed=set_seed
        X_train, X_validation, Y_train, Y_validation = model_selection.train_test_split(features, response,
                                                                                        test_size=validation_size,
                                                                                        random_state=seed)
        # Test options and evaluation metric
        scoring=crossval_scoring

        # Spot Check Algorithms
        models = []
        models.append(('LogisticRegression', LogisticRegression()))
        models.append(('LinearDiscriminantAnalysis', LinearDiscriminantAnalysis()))
        models.append(('KNeighborsClassifier', KNeighborsClassifier()))
        models.append(('DecisionTree', DecisionTreeClassifier()))
        models.append(('NaiveBayesGaussian', GaussianNB()))
        models.append(('RandomForest', RandomForestClassifier()))

        # Evaluate each model in turn
        results = []
        names = []

        for name, model in models:
            kfold = model_selection.StratifiedKFold(n_splits=k_folds, random_state=seed)
            cv_results = model_selection.cross_val_score(model, X_train, Y_train, cv=kfold, scoring=scoring)
            results.append(cv_results)
            names.append(name)
            msg = "%s: %s (%f), std (%f)" % (name, scoring, cv_results.mean(), cv_results.std())
            print(msg)

        zipped_eval = zip(models, [i.mean() for i in results])
        model_eval = sorted(zipped_eval, key=operator.itemgetter(1))

        best_clf = model_eval[-1][0][1]
        clf_name = model_eval[-1][0][0]
        print('\n')
        print("Model with best %s is %s" %(scoring, clf_name))
        print('\n')
        print('Printing evaluation on validation set:')

        # Validate model with highest score on test set (separate from training set used in k-folds)
        clf = best_clf
        clf.fit(X_train, Y_train)
        predictions = clf.predict(X_validation)
        print('Log Loss: %s' %(log_loss(Y_validation, predictions)))
        print('ROC AUC Score: %s' %(roc_auc_score(Y_validation, predictions)))
        print(ConfusionMatrix(Y_validation, predictions))
        print(classification_report(Y_validation, predictions))

        return best_clf

    else:
        raise Warning("Features and response inputs must be of type numpy.ndarray")


data = pd.read_pickle('data/base_features.pkl')

y = np.array(data.pop('target'))
X = np.array(data.drop(['id'], axis=1))

best_clf = find_best_classifier(X, y, set_seed=42, k_folds=3, crossval_scoring='roc_auc')
