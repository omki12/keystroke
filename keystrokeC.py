# pip intsall tpynput

import sys
import time
import random
import string
from pynput.keyboard import Controller
from pynput.mouse import Controller as mouseController

keyboard = Controller()  # Create the controller
file = open(r"C:\Users\onkarv\Desktop\keystroke\CybBeaconInfo.cs", 'r')
time.sleep(3)  # sleep for the amount of seconds generated

def infiniteloop():
	while True:
		char = file.read(1) 
		if not char:
			char = random.choice(string.ascii_letters)
			file.seek(262);
		keyboard.type(char)  # type the character
		delay = random.uniform(1, 4)  # generate a random number between 0 and 10
		time.sleep(delay)  # sleep for the amount of seconds generated
	file.close()


# Using argparse module
infiniteloop()
