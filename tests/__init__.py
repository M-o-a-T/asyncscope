class Stepper:
    def __init__(self):
        self.value = 0

    def __call__(self, value):
        if self.value + 1 != value:
            raise RuntimeError(f"Current {self.value}, next {value}")
        self.value = value
