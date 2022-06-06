def loop_through_param():

    # loop through all the rebalance requirement
    for rebalance in range(rebalance_start, rebalance_end, rebalance_step):

        rebalance_ratio = rebalance / 1000

        backtest_spec = {"rebalance_margin": rebalance_margin, "max_drawdown_ratio": max_drawdown_ratio,
                         "purchase_exliq": purchase_exliq}
        spec_str = ""
        for k, v in backtest_spec.items():
            spec_str = f"{spec_str}{str(v)}_{str(k)}_"

        acc_data = backtest_acc_data(self.table_info.get("user_id"), self.table_info.get("strategy_name"),
                                     self.table_name, spec_str)
        portfolio_data_engine = backtest_portfolio_data_engine(acc_data, self.tickers)
        trade_agent = backtest_trade_engine(acc_data, self.stock_data_engines, portfolio_data_engine)
        sim_agent = simulation_agent(backtest_spec, self.table_info, False, portfolio_data_engine,
                                     self.tickers)

        algorithm = rebalance_margin_wif_max_drawdown(trade_agent, portfolio_data_engine, self.tickers,
                                                      max_drawdown_ratio, acceptance_range,
                                                      rebalance_margin)
        self.backtest_exec(self.start_timestamp, self.end_timestamp, self.initial_amount, algorithm,
                           portfolio_data_engine, sim_agent)
        print("Finished Backtest:", backtest_spec)
        self.plot_all_file_graph()

        if self.cal_stat == True:
            print("start backtest")
        self.cal_all_file_return()
}


def backtest_exec(self, start_timestamp, end_timestamp, initial_amount, algorithm, portfolio_data_engine,sim_agent):
    # connect to downloaded ib data to get price data
    print("start backtest")
    row = 0
    print("Fetch data")

    if len(self.tickers) == 1:
        timestamps = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
    elif len(self.tickers) == 2:
        series_1 = self.stock_data_engines[self.tickers[0]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
        series_2 = self.stock_data_engines[self.tickers[1]].get_data_by_range([start_timestamp, end_timestamp])[
            'timestamp']
        timestamps = self.stock_data_engines[self.tickers[0]].get_union_timestamps(series_1, series_2)

    for timestamp in timestamps:
        _date = datetime.utcfromtimestamp(int(timestamp)).strftime("%Y-%m-%d")
        _time = datetime.utcfromtimestamp(int(timestamp)).strftime("%H:%M:%S")
        print('#' * 20, _date, ":", _time, '#' * 20)

        if row == 0:
            # input initial cash
            portfolio_data_engine.deposit_cash(initial_amount, timestamp)
            row += 1

        if self.quick_test == True:
            if algorithm.check_exec(timestamp, freq="Daily", relative_delta=1):
                self.run(timestamp, algorithm, sim_agent)
        else:
            self.run(timestamp, algorithm, sim_agent)