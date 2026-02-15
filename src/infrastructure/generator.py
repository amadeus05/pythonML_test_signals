import logging
import pandas as pd
import numpy as np
import pickle
from typing import Optional
from catboost import CatBoostClassifier
from src.domain.contracts import SignalGeneratorInterface, SignalDTO, SignalSide
from config import MODELS_DIR, CONFIDENCE_THRESHOLD, SL_PCT, TP_PCT
from etl_pipeline import add_features, add_htf_features

logger = logging.getLogger(__name__)

class MLSignalGenerator(SignalGeneratorInterface):
    def __init__(self):
        self.model = CatBoostClassifier()
        self.model.load_model(str(MODELS_DIR / "catboost_model.cbm"))
        
        with open(MODELS_DIR / "features.pkl", "rb") as f:
            self.feature_names = pickle.load(f)

    def generate_signal(self, symbol: str, df: pd.DataFrame, htf_df: pd.DataFrame) -> Optional[SignalDTO]:
        # ВАЖНО: Используем ту же логику что и в ETL / Backtest
        df = add_features(df)
        df = add_htf_features(df, htf_df)
        
        if df.empty:
            logger.warning(f"Empty DataFrame after feature generation for {symbol}")
            return None
            
        current_row = df.iloc[[-1]] # Последняя закрытая свеча
        current_price = current_row['close'].values[0]
        
        # Предсказание
        probs = self.model.predict_proba(current_row[self.feature_names])[0]
        
        p_short, p_neutral, p_long = 0, 0, 0
        if len(probs) == 2:
            p_short, p_long = probs
        else:
            p_short, p_neutral, p_long = probs
            
        logger.info(f"Analysis results for {symbol}: LONG: {p_long:.2%}, SHORT: {p_short:.2%}, NEUTRAL: {p_neutral:.2%} (Threshold: {CONFIDENCE_THRESHOLD})")
            
        side = SignalSide.NEUTRAL
        confidence = 0.0
        
        if p_long > CONFIDENCE_THRESHOLD:
            side = SignalSide.LONG
            confidence = p_long
        elif p_short > CONFIDENCE_THRESHOLD:
            side = SignalSide.SHORT
            confidence = p_short
            
        if side == SignalSide.NEUTRAL:
            return None
            
        # Расчет уровней (как в бектесте)
        if side == SignalSide.LONG:
            tp = current_price * (1 + TP_PCT)
            sl = current_price * (1 - SL_PCT)
            move = TP_PCT
        else:
            tp = current_price * (1 - TP_PCT)
            sl = current_price * (1 + SL_PCT)
            move = TP_PCT # Для шорта тоже считаем профит в модуле или как указано

        return SignalDTO(
            symbol=symbol,
            side=side,
            confidence=confidence,
            current_price=current_price,
            take_profit=tp,
            stop_loss=sl,
            expected_move_pct=move
        )
