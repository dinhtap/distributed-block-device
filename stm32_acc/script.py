import serial
import re
import mouse

ser = serial.Serial('/dev/ttyACM0')
regex = r'X([-]?)(\d+)Y([-]?)(\d+)'

while True:
    input = ser.readline().decode('utf-8').strip()
    match = re.match(regex, input)
    if match:
        x = int(match.group(2))
        y = int(match.group(4))
        if match.group(1) == '-':
            x = -x
        if match.group(3) == '-':
            y = -y
        
        mouse.move(x, y, absolute=False)
