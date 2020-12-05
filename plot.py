try:
    x = 1 / 0
except IOError:
    print('1')
else:
    print('2')
finally:
    print('3')
