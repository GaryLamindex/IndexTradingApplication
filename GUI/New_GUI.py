import tkinter as tk
# tk is the name of tkinter

# mainframe
interface = tk.Tk()

# Mainframe basic structure
interface.title("IndexTradingApplication")
interface.geometry("700x500")
interface.minsize(width=700, height=600)
interface.config(bg="light blue")
interface.attributes("-alpha", 0.95)


class label_frame:
    def __init__(self):
        self.strategy1 = "1.Late Thirties to Early"
        self.strategy2 = "2.Warren Buffett Portfolio"
        self.strategy3 = "3.Edge Select Aggressive"
        self.strategy4 = "4.Jane Bryant Quinn Portfolio"
        self.strategy5 = "5.Ultimate Buy and Hold strategy"
        self.strategy6 = "6.GAA Global Asset Allocation"
        self.strategy7 = "7.Aggressive Global Income"
        self.strategy8 = "8.Simplified Permanent Portfolio"
        self.strategy9 = "9.Vanguard Life Strategy Growth Fund"
        self.strategy10 = "10.No Brainer Portfolio"


# function of creating labels
def create_text(name):
    return tk.Label(interface, text=name, width=35, height=2, fg="black", bg="white", font="Helvetica")


# function of creating buttons
def create_button(name):
    return tk.Button(interface, text=name, width=20, height=2, fg="black", bg="grey", font="Helvetica")


# function of clicking buttons
def onclick(name):
    name.config(text="Running Code...", width=20, height=2, fg="black", bg="white", font="Helvetica").grid(row=0,
                                                                                                           column=2)


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
"""
for x in Strategy:
    tk.Label(interface, text=x, width=10, height=2, fg="black", bg="white", font="Helvetica").grid(row=countrow,
                                                                                                   column=0)
    tk.Button(interface, text="Start Run "+x, width=20, height=2, command=lambda: onclick(countrow)).grid(row=countrow, column=1)
    countrow = countrow+1
"""

"""
Strategy1 = tk.Label(text="Strategy 1", width=10, height=2, fg="black", bg="white", font="Helvetica")
Strategy1.grid(row=0, column=0)

# button1
my_button1 = tk.Button(interface, text="Start Run Strategy 1", command=lambda: onclick(Strategy1), height=2)
my_button1.grid(row=0, column=1)


lb2 = tk.Label(text="Strategy 2", width=10, height=2, fg="black", bg="white", font="Helvetica")
lb2.grid(row=1, column=0)

# button2
my_button2 = tk.Button(interface, text="Start Run Strategy 2", command=lambda: onclick(Strategy1), height=2)
my_button2.grid(row=1, column=1)

lb3 = tk.Label(text="Strategy 3", width=10, height=2, fg="black", bg="white", font="Helvetica")
lb3.grid(row=2, column=0)


# button3
my_button3 = tk.Button(interface, text="Start Run Strategy 3", command=lambda: onclick(Strategy1), height=2)
my_button3.grid(row=2, column=1)


lb4 = tk.Label(text="Strategy 4", width=10, height=2, fg="black", bg="white", font="Helvetica")
lb4.grid(row=3, column=0)


# button4
my_button4 = tk.Button(interface, text="Start Run Strategy 4", command=lambda: onclick(Strategy1), height=2)
my_button4.grid(row=3, column=1)
"""

# mainframe loop
interface.mainloop()
