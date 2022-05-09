# pip intsall tpynput

import sys
import time
import random
import string
from pynput.keyboard import Controller
from pynput.mouse import Controller as mouseController

keyboard = Controller()  # Create the controller
mouse = mouseController()

def MouseMovement(mouseMovementDx,mouseMovementDy):
	mouseDirection = random.uniform(-2,2)	
	try:
		mouse.scroll(0,mouseDirection)
		mouse.move(mouseMovementDx, mouseMovementDy)
	except:
		pass
		
def moveCursor():
	mouse.move(10, -15)
	#delay = random.uniform(2, 6)  # generate a random number between 0 and 10
	#time.sleep(3)  # sleep for the amount of seconds generated
	mouse.move(-10, 15)
	## Read pointer position
	#print('The current pointer position is {0}'.format(mouse.position))
	## Move pointer relative to current position
	#mouse.move(15, -15)
	## Read pointer position
	#print('The current pointer position is {0}'.format(mouse.position))

def infiniteloop():
	while True:
		charv = random.choice(string.ascii_letters)
		keyboard.type(charv)  # type the character

		# mouseMovementDx = mouseMovementDy =random.uniform(1,22)			 
		# MouseMovement(mouseMovementDx,mouseMovementDy)
		delay = random.uniform(1, 4)  # generate a random number between 0 and 10
		time.sleep(delay)  # sleep for the amount of seconds generated
		# MouseMovement(-mouseMovementDx,-mouseMovementDy)

def infiniteMouseLoop():
	while True:
		mouseMovementDx = mouseMovementDy = random.uniform(1,22)
		MouseMovement(mouseMovementDx,mouseMovementDy)
		
		delayM = random.uniform(2, 6)  # generate a random number between 0 and 10
		time.sleep(delayM)  # sleep for the amount of seconds generated
		
		MouseMovement(-mouseMovementDx,-mouseMovementDy)	
 
# total arguments
n = len(sys.argv)
#print("Total arguments passed:", n)
 
## Arguments passed
#print("\nName of Python script:", sys.argv[0])
 
#print("\nArguments passed:", end = " ")
#for i in range(1, n):
#    print(sys.argv[i], end = " ")
     

# Using argparse module
if n <= 1:
	infiniteloop()
elif sys.argv[1] == 'm':
	infiniteMouseLoop()

