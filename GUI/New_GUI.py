import subprocess
import sys
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText

# import engine.mongoDB_engine.trying
# import algo.accelerating_dual_momentum.realtime
# import algo.factor.realtime
# import algo.portfolio_rebalance.realtime
# import algo.random_forest
# import algo.rebalance_margin_never_sell
# import algo.rebalance_margin_wif_maintainance_margin
# import algo.rebalance_margin_wif_max_drawdown_control.real_time

# import application.backtest_application
# import application.grab_data_application
# import application.grap_stock_data_application
# import application.lazyportfolioetf_scraper
# import application.realtime_application
# import application.realtime_statistic_application
# import application.streaming_application
# import application.realtime_update_db_info
# import application.rebalance_margin_wif_max_drawdown_control_backtest_application
# import application.smartContract_application
# import application.streamlit_application


# tk is the name of tkinter

# mainframe
interface = tk.Tk()
# Mainframe basic structure
interface.title("IndexTradingApplication")
interface.geometry("1050x600")
interface.minsize(width=1050, height=600)
interface.config(bg="light blue")
interface.attributes("-alpha", 0.95)
var = tk.IntVar()


class label_frame:
    def __init__(self):
        self.strategy1 = "1.Accelerating_dual_momentum"
        self.strategy2 = "2.Factor"
        self.strategy3 = "3.Portfolio_rebalance"
        self.strategy4 = "4.Random_forest"
        self.strategy5 = "5.Rebalance_margin_never_sell"
        self.strategy6 = "6.Rebalance_margin_wif_maintainance_margin"
        self.strategy7 = "7.Rebalance_margin_wif_max_drawdown_control"
        self.strategy8 = "8.Simplified Permanent Portfolio"
        self.strategy9 = "9.Vanguard Life Strategy Growth Fund"
        self.strategy10 = "10.No Brainer Portfolio"


class PrintLogger(object):  # create file like object

    def __init__(self, textbox):  # pass reference to text widget
        self.textbox = textbox  # keep ref

    def write(self, text):
        self.textbox.configure(state="normal")  # make field editable
        self.textbox.insert("end", text)  # write text to textbox
        self.textbox.see("end")  # scroll to end
        self.textbox.configure(state="disabled")  # make field readonly

    def flush(self):  # needed for file like object
        pass


def reset_logging(self):
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__


def redirect_logging(self):
    logger = PrintLogger(self.log_widget)
    sys.stdout = logger
    sys.stderr = logger


# function of creating labels
# name = text you want to input
def create_text(name):
    return tk.Label(interface, text=name, width=40, height=2, fg="black", bg="light blue", font="Helvetica")


# function of creating buttons
def create_button(name):
    return tk.Button(interface,
                     text=name,
                     width=15,
                     height=2,
                     fg="black",
                     bg="grey",
                     font=("Helvetica", 15),
                     activebackground="white")


def create_message_box(text):
    return messagebox.showinfo("Error Message", text)


# can use drop down menus with button
def create_combobox(lists):
    return ttk.Combobox(interface, values=lists, state="readonly")


def activate_submit(apply_button):
    if var.get() == 1:
        apply_button['state'] = tk.NORMAL
    elif var.get() == 0:
        apply_button['state'] = tk.DISABLED
    else:
        create_message_box("There is an error")


def check_button(name, apply_button):
    return tk.Checkbutton(interface,
                          text=name,
                          onvalue=1,
                          offvalue=0,
                          variable=var,
                          font=("Helvetica", 15),
                          command=lambda: activate_submit(apply_button))


def create_entry():
    return tk.Entry(interface,
                    fg='black',
                    bg="white",
                    font="Helvetica")


def get_data():
    value = enter.get()
    if value =="":
        return create_message_box("Empty Entry")
    elif (isinstance(value, int) != True):
        return create_message_box("Wrong Input")
    else:
        choose_program(int(value))


# function of clicking buttons
def onclick(name):
    processing = create_text("Success Running")
    processing.config(width=20, height=2, fg="black", bg="white", font="Helvetica")
    processing.grid(row=10, column=1)


def execute(self, param):
    onclick(self)
    # subprocess.call(["python", filename])
    # subprocess.check_call(["python", filename])
    # engine.mongoDB_engine.trying.main()


def choose_program(program_no):
    if program_no == 1:
        execute(strategy1, 1)
    elif program_no == 2:
        execute(strategy2, 2)
    elif program_no == 3:
        execute(strategy3, 3)
    elif program_no == 4:
        execute(strategy4, 4)
    elif program_no == 5:
        execute(strategy5, 5)
    elif program_no == 6:
        execute(strategy6, 6)
    elif program_no == 7:
        execute(strategy7, 7)
    else:
        print("error")


def getbutton(name):
    choose_num = name.current() + 1
    choose_program(choose_num)


def end():
    exit()


entry_text = create_text("Enter the program number")
entry_text.grid(row=0, column=1)

end_button = create_button("Close")
end_button.config(command=lambda: end())
end_button.grid(row=0, column=2)


enter = create_entry()
enter.grid(row=1, column=1)

button = create_button("run program")
button.config(state=tk.DISABLED, command=lambda: get_data())
button.grid(row=3, column=1)

check = check_button("Confirmed Program", button)
check.grid(row=2, column=1)

select_list = create_combobox(["1.Accelerating_dual_momentum",
                               "2.Factor",
                               "3.Portfolio_rebalance",
                               "4.Random_forest",
                               "5.Rebalance_margin_never_sell",
                               "6.Rebalance_margin_wif_maintainance_margin",
                               "7.Rebalance_margin_wif_max_drawdown_control"])
select_list.grid(row=4, column=1)

list_button = create_button("sure")
list_button.config(command=lambda: getbutton(select_list))
list_button.grid(row=5, column=1)
# Label
countrow = 0
# countcol = 0
Strategy = ['Strategy1', 'Strategy2', 'Strategy3', 'Strategy4', 'Strategy5', 'Strategy6', 'Strategy7', 'Strategy8',
            'Strategy9', 'Strategy10', 'Strategy11']

# init labels name to labels
labels = label_frame()

strategy1 = create_text(labels.strategy1)
strategy1.grid(row=0, column=0)

strategy2 = create_text(labels.strategy2)
strategy2.grid(row=1, column=0)

strategy3 = create_text(labels.strategy3)
strategy3.grid(row=2, column=0)

strategy4 = create_text(labels.strategy4)
strategy4.grid(row=3, column=0)

strategy5 = create_text(labels.strategy5)
strategy5.grid(row=4, column=0)

strategy6 = create_text(labels.strategy6)
strategy6.grid(row=5, column=0)

strategy7 = create_text(labels.strategy7)
strategy7.grid(row=6, column=0)

strategy8 = create_text(labels.strategy8)
strategy8.grid(row=7, column=0)

strategy9 = create_text(labels.strategy9)
strategy9.grid(row=8, column=0)

strategy10 = create_text(labels.strategy10)
strategy10.grid(row=9, column=0)

# mainframe loop
interface.mainloop()
