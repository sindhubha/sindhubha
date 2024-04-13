def my_decorator(func):
    def wrapper():
        print("hii")
        func()
        print('thankyou')
    return wrapper
@my_decorator
def say_hello():
    print("hello!")

say_hello()