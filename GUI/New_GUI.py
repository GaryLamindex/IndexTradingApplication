import subprocess
import sys
import tkinter as tk
from tkinter import messagebox
from tkinter import ttk
from tkinter.scrolledtext import ScrolledText


# **import other python file in order to call their main function

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

# mainframe of gui
interface = tk.Tk()


# Mainframe basic structure
interface.title("IndexTradingApplication")
interface.geometry("1400x700")
interface.minsize(width=1400, height=700)
interface.config(bg="light blue")
interface.attributes("-alpha", 0.95)
var = tk.IntVar()


class label_frame:
    def __init__(self):
        # init strategy name
        self.strategy1 = "1.backtest_application"
        self.strategy2 = "2.grab_data_application"
        self.strategy3 = "3.grap_stock_data_application"
        self.strategy4 = "4.lazyportfolioetf_scraper"
        self.strategy5 = "5.realtime_application"
        self.strategy6 = "6.realtime_statistic_application"
        self.strategy7 = "7.realtime_update_db_info"
        self.strategy8 = "8.Simplified Permanent Portfolio (not exist)"
        self.strategy9 = "9.Vanguard Life Strategy Growth Fund (not exist)"
        self.strategy10 = "10.No Brainer Portfolio (not exist)"


# show data print in console
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


# reset output to console
def reset_logging():
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__


# redirect console output to gui
def redirect_logging():
    logger = PrintLogger(log_widget)
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


# use for send submit command
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


# get input from entry box
def get_data():
    value = enter.get()
    if value =="":
        return create_message_box("Empty Entry")
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


# call main function from other python file
def choose_program(program_no):
    if program_no == 1:
        # application.backtest_application.main()
        execute(strategy1, 1)
    elif program_no == 2:
        # application.grab_data_application.main()
        execute(strategy2, 2)
    elif program_no == 3:
        # application.grap_stock_data_application.main()
        execute(strategy3, 3)
    elif program_no == 4:
        # application.lazyportfolioetf_scraper.main()
        execute(strategy4, 4)
    elif program_no == 5:
        # application.realtime_application.main()
        execute(strategy5, 5)
    elif program_no == 6:
        # application.realtime_statistic_application.main()
        execute(strategy6, 6)
    elif program_no == 7:
        # application.realtime_update_db_info.main()
        execute(strategy7, 7)
    else:
        print("error")


def getbutton(name):
    choose_num = name.current() + 1
    choose_program(choose_num)


def end():
    exit()


entry_text = create_text("Enter the program number")
entry_text.grid(row=0, column=0)

end_button = create_button("Close")
end_button.config(command=lambda: end())
end_button.grid(row=0, column=2)


enter = create_entry()
enter.grid(row=1, column=0)

button = create_button("run program")
button.config(state=tk.DISABLED, command=lambda: get_data())
button.grid(row=3, column=0)

check = check_button("Confirmed Program", button)
check.grid(row=2, column=0)

# select_list = create_combobox(["1.Accelerating_dual_momentum",
#                                "2.Factor",
#                                "3.Portfolio_rebalance",
#                                "4.Random_forest",
#                                "5.Rebalance_margin_never_sell",
#                                "6.Rebalance_margin_wif_maintainance_margin",
#                                "7.Rebalance_margin_wif_max_drawdown_control"])
# select_list.grid(row=4, column=0)

# list_button = create_button("sure")
# list_button.config(command=lambda: getbutton(select_list))
# list_button.grid(row=5, column=0)

redirect_button = create_button("Redirect console to widget")
redirect_button.config(width=25, command=lambda: redirect_logging())
redirect_button.grid(row=7, column=0)

redirect_button2 = create_button("Redirect console reset")
redirect_button2.config(width=25, command=lambda: reset_logging())
redirect_button2.grid(row=8, column=0)

log_widget = ScrolledText(interface, height=8, width=100, font=("consolas", "10", "normal"), state='disabled')
log_widget.grid(row=6, column=0)
# Label
# init labels name to labels
labels = label_frame()

strategy1 = create_text(labels.strategy1)
strategy1.grid(row=0, column=1)

strategy2 = create_text(labels.strategy2)
strategy2.grid(row=1, column=1)

strategy3 = create_text(labels.strategy3)
strategy3.grid(row=2, column=1)

strategy4 = create_text(labels.strategy4)
strategy4.grid(row=3, column=1)

strategy5 = create_text(labels.strategy5)
strategy5.grid(row=4, column=1)

strategy6 = create_text(labels.strategy6)
strategy6.grid(row=5, column=1)

strategy7 = create_text(labels.strategy7)
strategy7.grid(row=6, column=1)

strategy8 = create_text(labels.strategy8)
strategy8.grid(row=7, column=1)

strategy9 = create_text(labels.strategy9)
strategy9.grid(row=8, column=1)

strategy10 = create_text(labels.strategy10)
strategy10.grid(row=9, column=1)

# mainframe loop
interface.mainloop()
