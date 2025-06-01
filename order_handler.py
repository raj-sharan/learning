from datetime import datetime, timedelta
import traceback

from order import MarketOrder

class OrderHandler:

    def __init__(self, kite_login, setting, instrument_token, logging):
        self.kite_login = kite_login
        self.setting = setting
        self.instrument_token = instrument_token
        self.logging = logging
        self.active_tokens = []
        self.position_loaded_at = None
        
    def load_positions(self, instruments): 
        if self.position_loaded_at is not None and datetime.now() - self.position_loaded_at <= timedelta(seconds = 30):
            return True
        
        orders = self.kite_login.conn.positions()
        any_position = False

        self.position_loaded_at = datetime.now()
        if not (orders and 'net' in orders):
            return None
    
        for order in orders['net']:  # Iterate over 'day' key in orders
            token = order['instrument_token']
            symbol = order['tradingsymbol']
            name = self.instrument_token.get_name_by_token(token)
    
            if name is not None:
                parent_token = self.setting.get_security_token_by_symbol(name)

                if parent_token in instruments.keys():
                    if token not in instruments[parent_token].orders:
                        if order['quantity'] != 0:
                            market_order = MarketOrder(token, symbol, self.logging)
                            instruments[parent_token].orders[token] = market_order
                            market_order.update(order)
                            any_position = True
                    else:
                        market_order = instruments[parent_token].orders[token]
                        if order['quantity'] != 0:
                            market_order.update(order)
                            any_position = True
                        else:
                            del instruments[parent_token].orders[token]

        return any_position

    def reload_positions(self, instruments, skip = False):
        if skip:
            return False
            
        try: 
            return self.load_positions(instruments)
        except Exception as e:
            self.logging.error(f"Error in reload_order: {e}")
            return None
        
    def fill_orders(self, instruments):
        orders = None
        for parent_token, instrument in instruments.items():
            for token, order in instrument.orders.items():
                if order.candle is None or order.sl_order_id is None:
                    try: 
                        if orders is None:
                            orders = self.kite_login.conn.orders()
                            
                        token_orders = [item for item in orders if item['instrument_token'] == token]
                        order.reload_order(self.kite_login, token_orders, instrument)
                    except Exception as e:
                        self.logging.error(f"Error in loading order_handler: {e}")
                        return None
    
        return True  


    def manage_orders(self, instruments, live_data):
        for parent_token, instrument in instruments.items():
            for token, order in instrument.orders.items():
                current_data = live_data.get_current_data(token)
                price = current_data['price'] if current_data is not None else 0
                sl_price = round(order.sl_price, 2) if order.sl_price else 0.0
                pcr = instrument.current_data_analysis['pcr_order'] if instrument.current_data_analysis else 0.0
                transaction_type = 'Buy' if order.quantity > 0 else 'Sell'
                
                self.logging.info(f'Order ({order.symbol}), Type: {transaction_type}, SL: {sl_price}, Last Price: {price}, PCR: {pcr}')

                if order.scalping is None:
                    self.set_order_scalping_mode(order, instrument)

                if order.quantity > 0:
                    if order.should_cancel_position(live_data):  
                        order.cancel_position(self.kite_login)
                    elif order.should_place_sl_order(live_data):
                        order.place_stop_loss_order(self.kite_login)
                    elif order.invalid_sl_order(live_data):
                        order.cancel_sl_order(self.kite_login)
                    elif order.is_trend_discontinues(self.kite_login, instrument, live_data):
                        order.cancel_sl_order(self.kite_login)
                    elif order.should_trail_order(self.kite_login):
                        order.trail_stop_loss_order(self.kite_login)
                    # elif order.should_modify_sl_order(live_data):
                    #     order.update_stop_loss_order(self.kite_login, live_data)
                    
    
                    if self.out_of_trading_session():
                        order.cancel_sl_order(self.kite_login)
                        
                if order.quantity < 0:
                    if order.is_selling_trend_discontinues(self.kite_login, instrument, live_data):
                        order.cancel_position(self.kite_login)

    def cancel_invalid_sl_orders(self, live_data, instruments):
        if live_data.order_updated:
            try: 
                active_order_tokens = [token for key in instruments.keys() for token in instruments[key].order_ids()]
                orders = self.kite_login.conn.orders()
    
                pending_sl_orders = [
                    order for order in orders
                    if order['status'] == 'TRIGGER PENDING' and order['order_type'] == 'SL'
                ]
    
                for order in pending_sl_orders:
                    if order['instrument_token'] not in active_order_tokens:
                        market_order = MarketOrder(order['instrument_token'], order['tradingsymbol'], self.logging)
                        market_order.sl_order_id = order['order_id']
                        market_order.cancel_sl_order(self.kite_login)
    
                live_data.order_updated = False        
            except Exception as e:
                self.logging.error(f"Error in cancel_invalid_sl_orders: {e}")
                self.logging.error(traceback.format_exc())

    def out_of_trading_session(self):
        current_time = datetime.now()
        # current_time = datetime(current_time.year, 2, 28, 14, 15)
        from_dt = datetime(current_time.year, current_time.month, current_time.day, 9, 15)
        to_dt = datetime(current_time.year, current_time.month, current_time.day, 15, 20)
        return current_time > to_dt

    def set_order_scalping_mode(self, order, instrument):
        if instrument.current_data_analysis:
            oi_ratio = instrument.current_data_analysis['ce_pe_oi_ratio']
            if 0.6 < oi_ratio <= 1.5:
                order.scalping = True
                
                    
            
        
        
        