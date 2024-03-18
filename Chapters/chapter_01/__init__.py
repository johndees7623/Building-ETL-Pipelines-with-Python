
import os  # used when working with files (delete, create, etc.)

# data structures ----------------------------------------
sample_list = [1, "another", "list", "a", [3, 4]]
print(sample_list[0])
print(sample_list[1])
print(sample_list[-1])
print(sample_list[1:4])
print(sample_list[:4])
print(sample_list[3:])

sample_dict = {"Key1": "Value1", 2: 3, "Age": 23}
print(sample_dict)
print(sample_dict["Age"])
print(sample_dict[2])

sample_tuple = (1, "2", "brij")  # tuples are like lists but are immutable
print(sample_tuple[0])

sample_set = {1, 2, 3, 4, 5, 5, 5, 5, 9}  # sets are like lists but are unique and unordered, no dupes
print(sample_set)
sample_set.add(6)
print(sample_set)
sample_set.remove(9)
print(sample_set)

# if-else ----------------------------------------
num = 9
if num == 0:
    print("Number is 0")
elif num % 3 == 0:
    print("Number is divisible by 3")
else:
    print("Number is not divisible by 3")

# loops----------------------------------------
# for loop
numbers = [7, 4, 3, 5, 8, 9, 8, 6, 14]
even_numbers = []
for num in numbers:
    if num % 2 == 0:
        even_numbers.append(num)
print(f"The even numbers are: {even_numbers}")

# while loop
count = 0
while (count < 11):
    print(f"The current number is : {count}")
    count += 1
print("Loop ended")


# functions----------------------------------------
def div_of_numbers(num1, num2):
    """This function returns the division of two numbers"""
    if num2 != 0:
        return num1 / num2
    else:
        return "Division by zero is not allowed"


print(f"10 divided by 2 is: {div_of_numbers(10, 2)}")


# object oriented programming----------------------------------------
class DataPipeline():
    first_tool = "Airflow"
    # a function inside a class is called a method
    # the __init__ method of a class is called the constructor, it is called as soon as the object is created
    # the first parameter of the __init__ method is always self


datapipe = DataPipeline()
print(datapipe.first_tool)


# parent class
# to use this class, you would need to create an object from it and then call its methods
class DataPipelineBook:
    def __init__(self):
        print("This book is very hot on the market")
        self.pages = 300

    def what_is_this(self):
        print("Book")

    def pages(self):
        return self.pages


# child class
class PythonDataPipelineBook(DataPipelineBook):
    def __init__(self):
        # call super() function ... this initializes the parent class
        super().__init__()
        print("Create Data Pipeline with Python")

    def what_technology_is_used(self):
        return "Python"


pipeline = PythonDataPipelineBook()
pipeline.what_is_this()  # parent class method
pipeline.what_technology_is_used()  # child class method

# working with files in python ----------------------------------------
# the "with" specifies a context manager is used that will automatically close the file

# create a new file called "test_file.txt" and write to it
with open("test_file.txt", "w") as f:  # open file in write mode
    f.write("This is the original content of the file.")
with open("test_file.txt", "r") as f:  # open file in read mode
    print(f.read())

with open("test_file.txt", "a") as f:  # open file in append mode
    f.write(" ... This is will be written to the file in append mode")
with open("test_file.txt", "r") as f:  # open file in read mode
    print(f.read())

with open("test_file.txt", "w") as f:  # open file in write mode, this will overwrite the file
    f.write("This is will be written to the file in write mode, overwriting the content of the file.")
with open("test_file.txt", "r") as f:  # open file in read mode
    print(f.read())

# delete the file "test_file.txt"
os.remove("test_file.txt")
print("File deleted")

