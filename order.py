import re

from datetime import datetime, timedelta
from common import Util
import pandas as pd
import traceback


class MarketOrder:
    def __init__(self, token, symbol, logging):
        self.token = token
        self.symbol = symbol
        self.order_id = None
        self.order_placed_at = None
        self.candle = None
        self.target = None
        self.quantity = None
        self.buy_price = None
        self.sl_price = None
        self.sl_order_id = None
        self.logging = logging
        self.trail_at = None
        self.close_position = None
        self.current_candle = {}
        self.trailed = False
        self.scalping = False

    def exceed_time(self):
        if self.order_placed_at is None:
            return False

        from_dt = datetime(self.order_placed_at.year, self.order_placed_at.month, self.order_placed_at.day, 9, 15)

        minutes_since_from_dt = (self.order_placed_at - from_dt).total_seconds() // 60
        candle_count = int(minutes_since_from_dt // 5)

        to_dt = from_dt + timedelta(minutes=candle_count * 5)
        return datetime.now() > to_dt + timedelta(minutes = 30)  # 6 * 5 = 30 minutes

    def update(self, order_details):           
        self.quantity = order_details.get("quantity", self.quantity)  # Use .get() to avoid KeyErrors
 
    def buy_order(self, kite_login, stop_loss, target, candle, scalping):
        order_id = self.place_market_order(kite_login)  # Pass quantity
        if order_id is not None:
            self.target = target
            self.sl_price = stop_loss
            self.candle = candle
            self.order_id = order_id
            date = candle['date']
            self.scalping = scalping
            if isinstance(date, str):
                date = datetime.fromisoformat(date)

            self.trail_at = datetime(date.year, date.month, date.day, date.hour, date.minute)
    
        return order_id

    def place_stop_loss_order(self, kite_login):
        if self.quantity is None:
            self.logging.error("Stop-loss order failed: No active buy order or quantity not set.")
            return None
    
        order_id = None
        try:
            tick_size = 1  # Adjust based on instrument tick size
            sl_price = self.sl_price - tick_size  # Use dynamic tick size
    
            order_id = kite_login.conn.place_order(
                variety=kite_login.conn.VARIETY_REGULAR,
                exchange=kite_login.conn.EXCHANGE_NFO,
                tradingsymbol=self.symbol,
                transaction_type=kite_login.conn.TRANSACTION_TYPE_SELL,
                quantity=self.quantity,
                order_type=kite_login.conn.ORDER_TYPE_SL,
                product=kite_login.conn.PRODUCT_NRML,
                trigger_price=self.sl_price,
                price=sl_price  # Use dynamic price calculation
            )
    
            self.sl_price = sl_price
            self.sl_order_id = order_id
            self.logging.info(f"Stop-loss order placed. ID: {order_id}")
        except Exception as e:
            self.logging.error(f"Stop-loss order placement failed: {e}")
    
        return order_id
        
    def place_market_order(self, kite_login):
        order_id = None
        transaction_type = (
                kite_login.conn.TRANSACTION_TYPE_BUY if self.quantity > 0 else kite_login.conn.TRANSACTION_TYPE_SELL
            )
        try:
            order_id = kite_login.conn.place_order(
                tradingsymbol=self.symbol,
                exchange=kite_login.conn.EXCHANGE_NFO,
                transaction_type=transaction_type,
                quantity=abs(self.quantity),
                variety=kite_login.conn.VARIETY_REGULAR,
                order_type=kite_login.conn.ORDER_TYPE_MARKET,
                product=kite_login.conn.PRODUCT_NRML,
                validity=kite_login.conn.VALIDITY_DAY
            )
            self.logging.info(f"Market order placed. Order ID: {order_id}")
        except Exception as e:
            self.logging.error(f"Market order placement failed: {e}")
    
        return order_id

    def cancel_position(self, kite_login):
        if self.quantity is not None and self.quantity != 0:
            transaction_type = (
                kite_login.conn.TRANSACTION_TYPE_SELL if self.quantity > 0 else kite_login.conn.TRANSACTION_TYPE_BUY
            )
            try:
                order_id = kite_login.conn.place_order(
                    variety=kite_login.conn.VARIETY_REGULAR,
                    exchange=kite_login.conn.EXCHANGE_NFO,
                    tradingsymbol=self.symbol,
                    transaction_type=transaction_type,  # Opposite transaction to square off
                    quantity=abs(self.quantity),
                    order_type=kite_login.conn.ORDER_TYPE_MARKET,  # Market order to close instantly
                    product=kite_login.conn.PRODUCT_NRML
                )
                self.logging.info(f"Closed position for {self.symbol}. Order ID: {order_id}")
                
                # Reset position
                self.quantity = 0
    
                return True
            except Exception as e:
                self.logging.error(f"Error closing position for {self.symbol}: {e}")
    
        return False

    def update_stop_loss_order(self, kite_login, live_data):
        if self.sl_order_id is not None:
            curr_data = live_data.get_current_data(self.token)
            if curr_data is None:
                return False
                
            last_price = curr_data['price']
            new_stop_loss_price = last_price - 10

            if new_stop_loss_price <= self.sl_price:
                return False

            if self.target is not None and self.sl_price < self.target - 3 and last_price > self.target:
                new_stop_loss_price = self.target
    
            try:
                kite_login.conn.modify_order(
                    variety=kite_login.conn.VARIETY_REGULAR,  # Use the same variety
                    order_id=self.sl_order_id,
                    trigger_price=new_stop_loss_price,  # Update SL trigger price
                    price=max(new_stop_loss_price - 1, 0)  # Ensure valid price
                )
    
                self.sl_price = new_stop_loss_price - 1.0
                self.logging.info(f"Stop-Loss order updated successfully for {self.symbol}. New SL: {new_stop_loss_price}")
    
                return True
            except Exception as e:
                self.logging.error(f"Error updating stop-loss order for {self.symbol}: {e}")
    
        return False

    def cancel_sl_order(self, kite_login, should_close_position = True):
        if self.sl_order_id is not None:
            try:
                kite_login.conn.cancel_order(variety=kite_login.conn.VARIETY_REGULAR, order_id=self.sl_order_id)
                self.logging.info(f"SL order {self.sl_order_id} canceled successfully.")
                self.sl_order_id = None
                self.sl_price = None
            except Exception as e:
                self.logging.error(f"Error canceling SL order {self.sl_order_id}: {e}")
                
        if self.sl_order_id is None and should_close_position:
            self.close_position = True
  
    def reload_order(self, kite_login, orders, instrument):
        for order in orders:
            if order['instrument_token'] == self.token:
                if order['transaction_type'] == 'BUY' and order['status'] == 'COMPLETE':
                    self.order_id = order['order_id']
                    self.order_placed_at = order['exchange_timestamp']
                    self.buy_price = order['average_price']
                elif order['order_type'] == 'SL' and order['status'] == 'TRIGGER PENDING':
                    self.sl_price = order['price'] * 1.0
                    self.sl_order_id = order['order_id']
    
        if self.order_placed_at is not None and self.candle is None:
            placed_time = self.order_placed_at
            from_dt = datetime(placed_time.year, placed_time.month, placed_time.day, 9, 15)
    
            minutes_since_from_dt = (placed_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes=candle_count * 5)
            from_dt = to_dt - timedelta(minutes=15)
    
            try:
                historical_data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
                if historical_data:
                    self.candle = historical_data[-1]
                    
                if self.candle:
                    if self.sl_price is None:
                        self.sl_price = self.candle['low'] * 1.0 - 7.0
                    date = self.candle['date']
                    if isinstance(date, str):
                        date = datetime.fromisoformat(date)
        
                    self.trail_at = datetime(date.year, date.month, date.day, date.hour, date.minute)
                            
            except Exception as e:
                self.logging.error("Failed to get candle: {}".format(e))
                return None

        return True

    def trail_stop_loss_order(self, kite_login):
        try:   
            date = datetime.now()
            self.trail_at = datetime(date.year, date.month, date.day, date.hour, date.minute)
            new_sl = self.candle['high'] - 3
            kite_login.conn.modify_order(
                variety = kite_login.conn.VARIETY_REGULAR,
                order_id = self.sl_order_id,
                trigger_price = new_sl,
                price=max(new_sl - 1, 0)
            )
            
            # Update SL price
            self.sl_price = new_sl - 1
            self.logging.info(f"Stop-Loss order trailed successfully for {self.symbol}. New SL: {new_sl}")
            self.trailed = True
            
            return True
    
        except Exception as e:
            self.logging.error(f"Error in trail_stop_loss_order: {e}")
    
        return False
            
    def is_target_achieved(self, live_data):
        curr_data = live_data.get_current_data(self.token)
        if curr_data is None or self.target is None:
            return False
            
        max_price = max(curr_data['price'], curr_data['high'])
    
        return max_price > self.target

    def is_trend_discontinues(self, kite_login, instrument, live_data):
        if not (self.sl_order_id and instrument.momentum_result and instrument.refresh_till_5m):
            return False
            
        if not (self.order_placed_at and instrument.current_data_analysis and instrument.current_data_analysis.get('unique_key')):
            return False

        try:
            nearest_pcr = instrument.current_data_analysis['pcr_nearest']
            next_pcr = instrument.current_data_analysis['pcr_next']
            order_pcr = instrument.current_data_analysis['pcr_order']
            direction = instrument.current_data_analysis['direction']
            bullish_candle = instrument.momentum_result['is_bullish_candle']
            bearish_candle = instrument.momentum_result['is_bearish_candle']
            is_bullish =  instrument.momentum_result['is_bullish']
            is_bearish =  instrument.momentum_result['is_bearish']
            down_target = instrument.current_data_analysis['nearest_gap']
            up_target = instrument.current_data_analysis['next_gap']
            nearest_pe_oi = instrument.current_data_analysis['oi_nearest'][0]
            nearest_ce_oi = instrument.current_data_analysis['oi_nearest'][1]
            next_ce_oi = instrument.current_data_analysis['oi_next'][1]
            next_pe_oi = instrument.current_data_analysis['oi_next'][0]

    
            # if not self.exceed_time():
            #     return False
    
            current_time = datetime.now()
            if not (current_time > instrument.refresh_till_5m and current_time - instrument.refresh_till_5m < timedelta(minutes=5)):
                return False
    
            if len(instrument.historical_data_5m) < 2:
                return False
    
            candle_5m = instrument.historical_data_5m.iloc[-1]
            pre_candle_5m = instrument.historical_data_5m.iloc[-2]
    
            check_unique_key = (
                candle_5m['unique_key'] == instrument.momentum_result['unique_key']
                and candle_5m['unique_key'] == instrument.current_data_analysis['unique_key']
            )
            if not check_unique_key:
                return False
                
            close, pre_low, pre_high = candle_5m['close'], pre_candle_5m['low'], pre_candle_5m['high']
                    
            if self.is_ce():
                if order_pcr == 0 or next_ce_oi == 0 or next_pcr == 0 or nearest_pcr == 0:
                    return False
                if ((direction == 'Down' or bearish_candle or is_bearish) and next_ce_oi > 7500000 and next_pcr < 0.95) or order_pcr < 1.25:
                    return True
                elif nearest_pcr < 1.1:
                    return True
                elif close < pre_low:
                    return True
                
            else:
                if order_pcr == 0 or nearest_pe_oi == 0 or nearest_pcr == 0 or next_pcr == 0:
                    return False
                if ((direction == 'Up' or bullish_candle or is_bullish) and nearest_pe_oi > 7500000 and nearest_pcr > 1.1) or order_pcr > 0.85:
                    return True
                elif next_pcr > 0.95:
                    return True
                elif close > pre_high:
                    return True
    
        except Exception as e:
            self.logging.error(f"Error in is_trend_discontinues: {e}")
            return False
       
    def is_selling_trend_discontinues(self, kite_login, instrument, live_data):
        if not (self.sl_order_id and instrument.momentum_result and instrument.refresh_till_5m):
            return False
            
        if not (self.order_placed_at and instrument.current_data_analysis and instrument.current_data_analysis.get('unique_key')):
            return False

        try:
            nearest_pcr = instrument.current_data_analysis['pcr_nearest']
            next_pcr = instrument.current_data_analysis['pcr_next']
            order_pcr = instrument.current_data_analysis['pcr_order']
            direction = instrument.current_data_analysis['direction']
            bullish_candle = instrument.momentum_result['is_bullish_candle']
            bearish_candle = instrument.momentum_result['is_bearish_candle']
            is_bullish =  instrument.momentum_result['is_bullish']
            is_bearish =  instrument.momentum_result['is_bearish']
            down_target = instrument.current_data_analysis['nearest_gap']
            up_target = instrument.current_data_analysis['next_gap']
            pe_oi = instrument.current_data_analysis['oi_nearest'][0]
            ce_oi = instrument.current_data_analysis['oi_next'][1]
    
            current_time = datetime.now()
            if not (current_time > instrument.refresh_till_5m and current_time - instrument.refresh_till_5m < timedelta(minutes=5)):
                return False
                    
            if self.is_ce():
                if ((direction == 'Up' or bullish_candle or is_bullish) and nearest_pcr > 1.3 and pe_oi > 7500000) or order_pcr > 0.75:
                    return True
            else:
                if ((direction == 'Down' or bearish_candle or is_bearish) and next_pcr < 0.9 and ce_oi > 7500000) or order_pcr < 1.5:
                    return True
                
    
        except Exception as e:
            self.logging.error(f"Error in is_selling_trend_discontinues: {e}")
            return False
            
 
    def should_cancel_position(self, live_data):
        if self.quantity is not None and self.quantity <= 0:
            return True

        if self.close_position and self.sl_order_id is None:
            return True
    
        curr_data = live_data.get_current_data(self.token)
        if curr_data is None:
            return False
    
        last_price = curr_data['price']

        if self.sl_price is not None and last_price < self.sl_price and self.sl_order_id is None:
            return True

        if self.exceed_time() and self.sl_order_id is None:
            return True
    
        return False


    def should_place_sl_order(self, live_data):
        if self.sl_order_id is None and self.sl_price is not None:
            curr_data = live_data.get_current_data(self.token)
            if curr_data is None:
                return False
    
            last_price = curr_data['price']
    
            if last_price > self.sl_price and not self.exceed_time():
                return True
    
        return False


    def should_modify_sl_order(self, live_data):
        if self.sl_order_id is not None and self.sl_price is not None:
            if self.exceed_time():
                return True
            if self.is_target_achieved(live_data):
                return True
    
        return False


    def invalid_sl_order(self, live_data):
        if self.sl_price is not None and self.sl_order_id is not None:
            curr_data = live_data.get_current_data(self.token)
            if curr_data is None:
                return False
    
            last_price = curr_data['price']
    
            if last_price < self.sl_price:
                return True
    
        return False

    def should_trail_order(self, kite_login):
        if self.trailed:
            return False
            
        if self.sl_order_id is not None and self.sl_price is not None:
            if self.order_placed_at is not None and self.exceed_time():
                if self.trail_at is None or datetime.now() > self.trail_at + timedelta(minutes = 5):
                    order_data = self.get_token_data(kite_login)
                    if order_data is not None and len(order_data) >= 2:
                        if order_data.iloc[-1]['low'] > self.candle['high'] + 5 and self.candle['high'] - self.sl_price > 5:
                            return True
        
        return False

    def get_token_data(self, kite_login):
        try:
            current_time = datetime.now()
            # current_time = datetime(current_time.year, 2, 28, 14, 15)
            from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)

            minutes_since_from_dt = (current_time - from_dt).total_seconds() // 60
            candle_count = int(minutes_since_from_dt // 5)
            
            to_dt = from_dt + timedelta(minutes = candle_count * 5)
            from_dt = to_dt - timedelta(minutes = 15)

            unique_key = Util.generate_5m_id(current_time - timedelta(minutes = 5))
            
            if unique_key in self.current_candle:
                return self.current_candle[unique_key]
            
            data = kite_login.conn.historical_data(self.token, from_dt, to_dt, "5minute")
            data_df = pd.DataFrame(data)

            self.current_candle[unique_key] = data_df
            
            return data_df
               
        except Exception as e:
            self.logging.error(f"Error in refreshing 5m data for {self.token}: {e}")
            self.logging.error(traceback.format_exc())
            return None

    def is_ce(self):
        return re.search(r"CE$", self.symbol) is not None
        
        
        