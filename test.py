class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y
        self.z = (x * x + y * y) ** .5


p1 = Point(3, 4)
# a = p1.z
# print(a)


class Point1(Point):
    def __init__(self, color, x, y):
        # super().__init__(x, y)
        super().__init__(x, y)
        self.color = color
        a = x
        if a > 1:
            print(a)


colored_point = Point1('blue', 2, 3)
print(colored_point.color)
