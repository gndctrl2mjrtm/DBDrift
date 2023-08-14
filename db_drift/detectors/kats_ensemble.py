from kats.models.ensemble.ensemble import EnsembleParams, BaseModelParams
from kats.models.ensemble.kats_ensemble import KatsEnsemble
from kats.models import (
    holtwinters,
    linear_model,
    prophet,
    quadratic_model,
    sarima,
    theta,
)


class KatsEnsembleModel(object):

    def __init__(self):
        self.model_params = EnsembleParams(
            [
                BaseModelParams(
                    "sarima",
                    sarima.SARIMAParams(
                        p=2,
                        d=1,
                        q=2,
                        trend='ct'
                    )
                ),
                BaseModelParams("prophet", prophet.ProphetParams()),
                BaseModelParams("linear", linear_model.LinearModelParams()),
                BaseModelParams("quadratic", quadratic_model.QuadraticModelParams()),
                BaseModelParams("theta", theta.ThetaParams(m=12)),
            ]
        )

        # create `KatsEnsembleParam` with detailed configurations
        self.KatsEnsembleParam = {
            "models": self.model_params,
            "aggregation": "median",
            "seasonality_length": 12,
            "decomposition_method": "multiplicative",
        }

    def train(self):
        # create `KatsEnsemble` model
        self.model = KatsEnsemble(
            data=sh_pd,
            params=self.KatsEnsembleParam
        )

        # fit and predict
        self.model.fit()

    def predict(self, window=7):
        if not isinstance(window, int):
            raise TypeError
        if window < 1:
            raise UserWarning("Window must be > 1")
        # predict for the next 30 steps
        fcst = self.model.predict(steps=window)
        return fcst
