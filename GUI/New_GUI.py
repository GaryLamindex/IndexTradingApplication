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


def onclick(self):
    self.config(text="Running Code...", width=20, height=2, fg="black", bg="white", font="Helvetica")


# Label
countrow = 0
countcol = 0
Strategy = ['Strategy1', 'Strategy2', 'Strategy3', 'Strategy4', 'Strategy5', 'Strategy6', 'Strategy7', 'Strategy8',
            'Strategy9', 'Strategy10', 'Strategy11']


for x in Strategy:
    tk.Label(interface, text=x, width=10, height=2, fg="black", bg="white", font="Helvetica").grid(row=countrow,
                                                                                                   column=0)
    tk.Button(interface, text="Start Run ", width=10, height=2).grid(row=countrow, column=1)
    countrow = countrow+1


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
